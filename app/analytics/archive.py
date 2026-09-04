"""Immutable monthly chron-archive protocol.

Every month, the pipeline persists FULL monthly data logs as discrete,
gzipped files stored inside the Railway PostgreSQL database
(``monthly_archive_files.payload`` BYTEA):

- ``raw_records__YYYY-MM.jsonl.gz``        every raw snapshot ingested that month
- ``staging_records__YYYY-MM.jsonl.gz``    staging records + verification verdicts
- ``entities__YYYY-MM.jsonl.gz``           every lattice entity created that month
- ``analytics_snapshot__YYYY-MM.json.gz``  the exact canonical analytics payload
- ``anomaly_findings__YYYY-MM.jsonl.gz``   officer anomaly findings + narratives

Files are content-addressed (SHA-256) and append-only: identical content is
not re-written; changed content is stored as a NEW versioned file, preserving
the full history of what was known when. On PostgreSQL, triggers physically
reject UPDATE/DELETE (see ``app.db.install_immutability_guards``).

The automated monthly refresh runs on the 1st of each month (scheduler) and
finalizes the month that just ended; the current month is continuously
re-archived so the active view and archive never diverge for long.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import orjson
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analytics.engine import AnalyticsEngine, month_bounds, shift_month
from app.models import (
    Agency,
    Arrest,
    Charge,
    CourtCase,
    Document,
    EntityLink,
    Incident,
    MonthlyArchiveFile,
    MonthlyRefreshRun,
    NewsArticle,
    Officer,
    OfficerAnomalyFinding,
    Person,
    RawRecord,
    StagingRecord,
    SurveillanceEvent,
    VerificationResult,
)

logger = logging.getLogger(__name__)

ARCHIVE_KINDS = (
    "raw_records",
    "staging_records",
    "entities",
    "analytics_snapshot",
    "anomaly_findings",
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def serialize_row(obj: Any) -> dict[str, Any]:
    """Serialize a SQLAlchemy model row to a JSON-safe dict (full fidelity)."""
    out: dict[str, Any] = {}
    for attr in obj.__mapper__.column_attrs:
        value = getattr(obj, attr.key)
        if isinstance(value, datetime):
            out[attr.key] = value.isoformat()
        elif isinstance(value, bytes):
            # archive payloads are stored as separate discrete files
            out[attr.key] = f"<{len(value)} bytes sha256:{hashlib.sha256(value).hexdigest()[:16]}>"
        else:
            out[attr.key] = value
    return out


def _json_safe(value: Any) -> Any:
    """Recursively coerce non-str dict keys to str (orjson requirement; SQLite
    JSON columns can deserialize integer keys)."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def jsonl_gz(rows: Iterable[dict[str, Any]]) -> tuple[bytes, int]:
    buf = bytearray()
    count = 0
    for row in rows:
        buf += orjson.dumps(_json_safe(row))
        buf += b"\n"
        count += 1
    return gzip.compress(bytes(buf), compresslevel=6, mtime=0), count


def json_gz(payload: dict[str, Any]) -> tuple[bytes, int]:
    return gzip.compress(orjson.dumps(_json_safe(payload)), compresslevel=6, mtime=0), 1


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class MonthlyArchiver:
    """Builds and persists the discrete monthly archive files."""

    def __init__(self, session: Session):
        self.session = session

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def previous_month_key(month_key: str) -> str:
        return shift_month(month_key, -1)

    def is_finalized(self, month_key: str) -> bool:
        rows = self.session.scalars(
            select(MonthlyArchiveFile).where(MonthlyArchiveFile.month_key == month_key)
        ).all()
        kinds = {r.kind for r in rows}
        if not set(ARCHIVE_KINDS).issubset(kinds):
            return False
        runs = self.session.scalars(
            select(MonthlyRefreshRun).where(MonthlyRefreshRun.month_key == month_key)
        ).all()
        return any(r.status == "completed" for r in runs)

    def _existing_sha_for(self, month_key: str, kind: str) -> dict[str, str]:
        rows = self.session.scalars(
            select(MonthlyArchiveFile).where(
                MonthlyArchiveFile.month_key == month_key, MonthlyArchiveFile.kind == kind
            )
        ).all()
        return {r.filename: r.sha256 for r in rows}

    def _store(
        self,
        month_key: str,
        kind: str,
        base_filename: str,
        content_type: str,
        data: bytes,
        record_count: int,
        pipeline_run_id: int | None,
    ) -> dict[str, Any]:
        """Append-only store of one discrete file. Returns disposition."""
        digest = sha256_hex(data)
        existing = self._existing_sha_for(month_key, kind)  # filename -> sha256

        # Content-identical file already archived: never duplicate.
        if digest in existing.values():
            match = next(fn for fn, sha in existing.items() if sha == digest)
            return {"filename": match, "disposition": "content_already_archived", "sha256": digest}

        if base_filename not in existing:
            filename = base_filename
        else:
            # Same logical file with different content: append a NEW version.
            if base_filename.endswith(".jsonl.gz"):
                stem, ext = base_filename[: -len(".jsonl.gz")], ".jsonl.gz"
            else:
                stem, ext = base_filename.rsplit(".", 1)
                ext = f".{ext}"
            version = 2
            while f"{stem}-v{version}{ext}" in existing:
                version += 1
            filename = f"{stem}-v{version}{ext}"

        row = MonthlyArchiveFile(
            month_key=month_key,
            kind=kind,
            filename=filename,
            content_type=content_type,
            sha256=digest,
            size_bytes=len(data),
            record_count=record_count,
            payload=data,
            pipeline_run_id=pipeline_run_id,
            created_at=_utcnow(),
        )
        self.session.add(row)
        self.session.flush()
        return {
            "filename": filename,
            "disposition": "written",
            "sha256": digest,
            "size_bytes": len(data),
            "record_count": record_count,
        }

    # -- main entry ---------------------------------------------------------

    def archive_month(
        self,
        month_key: str,
        pipeline_run_id: int | None = None,
        finalize: bool = False,
    ) -> dict[str, Any]:
        start, end = month_bounds(month_key)
        run = MonthlyRefreshRun(
            month_key=month_key,
            started_at=_utcnow(),
            status="running",
        )
        self.session.add(run)
        self.session.flush()

        try:
            files: dict[str, Any] = {}

            # 1. raw records log
            raws = self.session.scalars(
                select(RawRecord)
                .where(RawRecord.ingested_at >= start, RawRecord.ingested_at < end)
                .order_by(RawRecord.id)
            ).all()

            def _raw_rows():
                for r in raws:
                    yield {
                        **serialize_row(r),
                        "raw_data": r.raw_data,
                    }

            data, count = jsonl_gz(_raw_rows())
            files["raw_records"] = self._store(
                month_key, "raw_records", f"raw_records__{month_key}.jsonl.gz",
                "application/x-ndjson+gzip", data, count, pipeline_run_id,
            )

            # 2. staging records + verification verdicts
            stagings = self.session.scalars(
                select(StagingRecord)
                .where(StagingRecord.created_at >= start, StagingRecord.created_at < end)
                .order_by(StagingRecord.id)
            ).all()
            verdicts = {
                v.staging_record_id: v
                for v in self.session.scalars(
                    select(VerificationResult).where(
                        VerificationResult.staging_record_id.in_([s.id for s in stagings])
                    )
                ).all()
            }

            def _staging_rows():
                for s in stagings:
                    v = verdicts.get(s.id)
                    yield {
                        **serialize_row(s),
                        "payload": s.payload,
                        "verification": (
                            {
                                "passed": v.passed,
                                "checks": v.checks,
                                "failures": v.failures,
                                "verified_at": v.verified_at.isoformat(),
                            }
                            if v
                            else None
                        ),
                    }

            data, count = jsonl_gz(_staging_rows())
            files["staging_records"] = self._store(
                month_key, "staging_records", f"staging_records__{month_key}.jsonl.gz",
                "application/x-ndjson+gzip", data, count, pipeline_run_id,
            )

            # 3. entities created this month
            entity_rows: list[dict[str, Any]] = []
            for model, label in (
                (Agency, "agency"), (Officer, "officer"), (Person, "person"),
                (Incident, "incident"), (Arrest, "arrest"), (Charge, "charge"),
                (CourtCase, "court_case"), (Document, "document"),
                (NewsArticle, "news_article"), (SurveillanceEvent, "surveillance_event"),
                (EntityLink, "entity_link"),
            ):
                for row in self.session.scalars(select(model)).all():
                    created = getattr(row, "created_at", None)
                    if created is not None:
                        if created.tzinfo is None:
                            created = created.replace(tzinfo=UTC)
                        if start <= created < end:
                            entry = {"entity": label, **serialize_row(row)}
                            if hasattr(row, "data") and row.data:
                                entry["data"] = row.data
                            if hasattr(row, "external_ids") and row.external_ids:
                                entry["external_ids"] = row.external_ids
                            entity_rows.append(entry)
            data, count = jsonl_gz(entity_rows)
            files["entities"] = self._store(
                month_key, "entities", f"entities__{month_key}.jsonl.gz",
                "application/x-ndjson+gzip", data, count, pipeline_run_id,
            )

            # 4. analytics snapshot (exact canonical payload)
            analytics = AnalyticsEngine(self.session).compute_month(month_key)
            from app.analytics.narrative import render_month_summary

            analytics["plain_language_summary"] = render_month_summary(analytics)
            # Strip volatile generation timestamp so identical data produces
            # byte-identical archives (deterministic content addressing);
            # the archive row's created_at preserves the true archival time.
            analytics.pop("generated_at", None)
            data, count = json_gz(analytics)
            files["analytics_snapshot"] = self._store(
                month_key, "analytics_snapshot", f"analytics_snapshot__{month_key}.json.gz",
                "application/json+gzip", data, count, pipeline_run_id,
            )

            # 5. anomaly findings log
            findings = self.session.scalars(
                select(OfficerAnomalyFinding).where(
                    OfficerAnomalyFinding.month_key == month_key
                )
            ).all()

            def _finding_rows():
                for f in findings:
                    yield {
                        **serialize_row(f),
                        "metric_records_basis": f.metric_records_basis,
                        "evidence": f.evidence,
                    }

            data, count = jsonl_gz(_finding_rows())
            files["anomaly_findings"] = self._store(
                month_key, "anomaly_findings", f"anomaly_findings__{month_key}.jsonl.gz",
                "application/x-ndjson+gzip", data, count, pipeline_run_id,
            )

            run.status = "completed"
            run.completed_at = _utcnow()
            run.files_written = sum(1 for f in files.values() if f.get("disposition") == "written")
            run.bytes_written = sum(
                f.get("size_bytes", 0) for f in files.values() if f.get("disposition") == "written"
            )
            run.stats = {
                "month_key": month_key,
                "finalize": finalize,
                "files": files,
                "raw_records": len(raws),
                "staging_records": len(stagings),
                "entities": len(entity_rows),
                "anomaly_findings": len(findings),
            }
            self.session.flush()
            return {
                "month_key": month_key,
                "finalized": finalize,
                "files": files,
                "monthly_refresh_run_id": run.id,
                "stats": run.stats,
            }
        except Exception as exc:
            run.status = "failed"
            run.error = str(exc)
            run.completed_at = _utcnow()
            self.session.flush()
            raise

    # -- read paths ----------------------------------------------------------

    def list_months(self) -> list[dict[str, Any]]:
        rows = self.session.scalars(
            select(MonthlyArchiveFile).order_by(MonthlyArchiveFile.month_key.desc())
        ).all()
        by_month: dict[str, dict[str, Any]] = {}
        for r in rows:
            entry = by_month.setdefault(
                r.month_key,
                {
                    "month": r.month_key,
                    "kinds": {},
                    "latest_refresh_at": None,
                    "total_bytes": 0,
                },
            )
            kind_entry = entry["kinds"].setdefault(
                r.kind,
                {
                    "filename": r.filename,
                    "sha256": r.sha256,
                    "size_bytes": r.size_bytes,
                    "record_count": r.record_count,
                    "created_at": r.created_at.isoformat(),
                    "versions": [],
                },
            )
            kind_entry["versions"].append(
                {
                    "filename": r.filename,
                    "sha256": r.sha256,
                    "size_bytes": r.size_bytes,
                    "created_at": r.created_at.isoformat(),
                }
            )
            entry["total_bytes"] += r.size_bytes
            refreshed = r.created_at.isoformat()
            if entry["latest_refresh_at"] is None or refreshed > entry["latest_refresh_at"]:
                entry["latest_refresh_at"] = refreshed
        for entry in by_month.values():
            for kind_entry in entry["kinds"].values():
                kind_entry["versions"].sort(key=lambda v: v["created_at"])
        return list(by_month.values())

    def latest_file(self, month_key: str, kind: str) -> MonthlyArchiveFile | None:
        rows = self.session.scalars(
            select(MonthlyArchiveFile)
            .where(MonthlyArchiveFile.month_key == month_key, MonthlyArchiveFile.kind == kind)
            .order_by(MonthlyArchiveFile.created_at.desc(), MonthlyArchiveFile.id.desc())
        ).all()
        return rows[0] if rows else None

    def read_analytics_snapshot(self, month_key: str) -> dict[str, Any] | None:
        row = self.latest_file(month_key, "analytics_snapshot")
        if row is None:
            return None
        payload = orjson.loads(gzip.decompress(row.payload))
        # Mark replay provenance without altering any archived value.
        payload["mode"] = "archived"
        payload["archive"] = {
            "sha256": row.sha256,
            "size_bytes": row.size_bytes,
            "archived_at": row.created_at.isoformat(),
            "filename": row.filename,
        }
        return payload

    def read_kind_lines(self, month_key: str, kind: str) -> list[dict[str, Any]] | None:
        row = self.latest_file(month_key, kind)
        if row is None:
            return None
        lines = gzip.decompress(row.payload).splitlines()
        return [orjson.loads(line) for line in lines if line.strip()]
