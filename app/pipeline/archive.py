"""Immutable monthly chron-archive.

Each month's complete lattice view is serialized once, hashed, and written as
a single sealed row in ``monthly_snapshots``. Sealing is append-only:

* a period that has already been sealed at the current revision is never
  rewritten — its bytes and hash stay exactly as first recorded;
* re-running a month that has since received new source data writes a **new**
  row with ``revision + 1`` and its own hash, and flips ``is_current`` so the
  UI can show that the month was amended and when.

Because the payload is produced by the same ``build_view`` that serves the
live month, an archived month renders with exact parity to the active view.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.analytics.engine import available_periods, build_view
from app.db import utcnow
from app.ingest.parsers import dumps
from app.models import MonthlySnapshot

logger = logging.getLogger(__name__)


def _hash_payload(payload: dict[str, Any]) -> str:
    """Content hash over the payload with volatile keys excluded."""
    stable = {k: v for k, v in payload.items() if k != "generated_at"}
    return hashlib.sha256(dumps(stable)).hexdigest()


def archive_month(session: Session, period: str, *, force: bool = False) -> dict[str, Any]:
    """Seal one month. Returns what was written, or why nothing was."""
    view = build_view(session, period)
    view.pop("archive", None)
    content_sha256 = _hash_payload(view)
    # The archive block is added back *after* hashing so that the sealed
    # payload has exactly the same top-level keys as a live view. That is
    # what makes a month swap unable to change the rendered structure.
    view["archive"] = {
        "period": period,
        "content_sha256": content_sha256,
        "sealed": True,
    }

    latest = session.scalar(
        select(MonthlySnapshot)
        .where(MonthlySnapshot.period == period)
        .order_by(MonthlySnapshot.revision.desc())
        .limit(1)
    )

    if latest is not None and latest.content_sha256 == content_sha256:
        return {
            "period": period,
            "action": "unchanged",
            "revision": latest.revision,
            "content_sha256": content_sha256,
            "sealed_at": latest.sealed_at.isoformat(),
        }

    revision = (latest.revision + 1) if latest is not None else 1
    if latest is not None:
        latest.is_current = False
        if not force:
            logger.info(
                "[%s] month content changed since revision %d — writing revision %d",
                period, latest.revision, revision,
            )

    snapshot = MonthlySnapshot(
        period=period,
        revision=revision,
        sealed_at=utcnow(),
        content_sha256=content_sha256,
        is_current=True,
        counts=view.get("counts", {}),
        payload=view,
    )
    session.add(snapshot)
    session.flush()

    return {
        "period": period,
        "action": "sealed",
        "revision": revision,
        "content_sha256": content_sha256,
        "sealed_at": snapshot.sealed_at.isoformat(),
        "counts": snapshot.counts,
    }


def archived_periods(session: Session) -> list[dict[str, Any]]:
    """Index of sealed months, newest first."""
    rows = session.execute(
        select(MonthlySnapshot).order_by(
            MonthlySnapshot.period.desc(), MonthlySnapshot.revision.desc()
        )
    ).scalars().all()
    return [
        {
            "period": s.period,
            "revision": s.revision,
            "is_current": bool(s.is_current),
            "content_sha256": s.content_sha256,
            "sealed_at": s.sealed_at.isoformat(),
            "counts": s.counts,
        }
        for s in rows
    ]


def load_archived_view(session: Session, period: str, revision: int | None = None):
    """Return a sealed month's payload, or ``None`` if it was never sealed."""
    query = select(MonthlySnapshot).where(MonthlySnapshot.period == period)
    if revision is not None:
        query = query.where(MonthlySnapshot.revision == revision)
    else:
        query = query.where(MonthlySnapshot.is_current.is_(True))
    snapshot = session.scalar(query.limit(1))
    return snapshot.payload if snapshot else None


def refresh_archive(session: Session, *, months: int | None = None) -> dict[str, Any]:
    """Seal every month present in the lattice.

    ``months`` bounds the sweep to the most recent N months; ``None`` seals
    the entire history, which is what the monthly refresh protocol does.
    """
    periods = available_periods(session)
    if months is not None:
        periods = periods[:months]
    results = [archive_month(session, period) for period in periods]
    session.commit()
    return {
        "periods_sealed": sum(1 for r in results if r["action"] == "sealed"),
        "periods_unchanged": sum(1 for r in results if r["action"] == "unchanged"),
        "detail": results,
    }
