"""Test fixtures.

``recorded/`` holds byte-for-byte captures of live public endpoints, taken on
2026-09-03 and listed in ``recorded/MANIFEST.json`` with the URL, HTTP status
and SHA-256 of each capture. They exist **only** so the test suite can drive
the real transport, parsers and synthesis code without depending on outbound
network. The application never reads this directory: it has no code path that
does, and ``tests/test_no_fabrication.py`` fails the build if one appears.

``synthetic_*`` builders produce controlled inputs for the statistics tests,
where the expected p-values and z-scores must be known exactly.
"""

from __future__ import annotations

import os
import tempfile

# Point every app import at a throwaway SQLite file before app.config is
# first evaluated, so no test ever touches a real database.
os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(
    tempfile.mkdtemp(prefix="lattice-test-"), "test.db"
)
os.environ["SCHEDULER_ENABLED"] = "false"
os.environ["SEMANTIC_SEARCH"] = "false"
os.environ["HTTP_RETRIES"] = "1"
os.environ["HTTP_TIMEOUT_SECONDS"] = "5"
os.environ["HTTP_CONCURRENCY"] = "4"

import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
RECORDED_DIR = FIXTURE_DIR / "recorded"


def recorded(name: str) -> dict | list:
    """Load a recorded live capture."""
    return json.loads((RECORDED_DIR / name).read_text(encoding="utf-8"))


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class RecordingCkanHandler(BaseHTTPRequestHandler):
    """Serves the recorded Phoenix CKAN payloads over real HTTP."""

    routes: dict[str, tuple[str, dict | list]] = {}

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        path = self.path.split("?", 1)[0]
        entry = self.routes.get(path)
        if entry is None and path.startswith("/dataset/"):
            entry = ("csv", None)

        if entry is None:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"success": false}')
            return

        _kind, payload = entry
        text = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(text)))
        self.end_headers()
        self.wfile.write(text)

    def log_message(self, *_args) -> None:  # silence
        pass


@pytest.fixture()
def make_ckan_server():
    """Factory: start a local HTTP server speaking recorded CKAN responses.

    ``datastore_file`` selects which datastore payload the server returns, so
    a test can run against the verbatim capture or against the synthetic
    volume file.
    """
    servers: list[ThreadingHTTPServer] = []

    def _start(datastore_file: str = "ckan_datastore_uof_officer_summary.json") -> str:
        handler = type(
            "BoundHandler",
            (RecordingCkanHandler,),
            {
                "routes": {
                    "/api/3/action/package_list": ("json", recorded("ckan_package_list.json")),
                    "/api/3/action/package_show": ("json", recorded("ckan_package_show_uof.json")),
                    "/api/3/action/datastore_search": ("json", recorded(datastore_file)),
                },
            },
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        servers.append(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield _start

    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def ckan_server(make_ckan_server):
    return make_ckan_server()



@pytest.fixture()
def memory_session():
    """An isolated in-memory SQLite lattice."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    import app.models  # noqa: F401
    from app.db import Base, install_timezone_coercion

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    install_timezone_coercion()
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Synthetic builders for the statistics tests
# ---------------------------------------------------------------------------

def synthetic_officer_metrics(n_officers: int = 200, base_rate: float = 8.0, seed: int = 7):
    """Deterministic peer group: most officers near ``base_rate``, one outlier."""
    import random

    rng = random.Random(seed)
    peers = [max(0, int(rng.gauss(base_rate, 1.5))) for _ in range(n_officers)]
    return peers


def synthetic_force_events(session, *, n_officers: int, period: str, agency_id: str = "test-pd",
                           outlier_index: int = 0, outlier_events: int = 40,
                           outlier_out_of_policy: int = 24, peer_events: int = 6,
                           peer_out_of_policy_rate: float = 0.02):
    """Insert a controlled officer population with one extreme outlier."""
    import random

    from app.models import Agency, ForceEvent, Incident, OfficerRef

    rng = random.Random(11)
    session.add(Agency(id=agency_id, name="Test Police Department", jurisdiction="Test, AZ"))
    session.flush()
    incident_total = 0

    for index in range(n_officers):
        officer = OfficerRef(agency_id=agency_id, external_key=f"OFFICER-{index:05d}")
        session.add(officer)
        session.flush()

        if index == outlier_index:
            total, oop = outlier_events, outlier_out_of_policy
        else:
            total = peer_events + rng.randint(-1, 1)
            total = max(0, total)
            oop = sum(1 for _ in range(total) if rng.random() < peer_out_of_policy_rate)

        for event_index in range(total):
            incident = Incident(
                agency_id=agency_id,
                external_number=f"INC-{index:05d}-{event_index:03d}",
                kind="use_of_force",
                period=period,
                source_id="test-source",
                source_url="https://example.test/dataset/test",
                retrieved_at=__import__("datetime").datetime.now(
                    __import__("datetime").UTC
                ),
                content_sha256=sha256(f"{index}-{event_index}"),
                data={},
            )
            session.add(incident)
            session.flush()
            session.add(
                ForceEvent(
                    officer_ref_id=officer.id,
                    incident_id=incident.id,
                    period=period,
                    within_policy="No" if event_index < oop else "Yes",
                    bwc_activated="Yes",
                    source_id="test-source",
                    source_url="https://example.test/dataset/test",
                    data={},
                )
            )
        incident_total += total
    session.flush()
    return {
        "officers": n_officers,
        "incidents": incident_total,
        "outlier_events": outlier_events,
        "outlier_out_of_policy": outlier_out_of_policy,
    }
