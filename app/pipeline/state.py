from __future__ import annotations

from sqlalchemy.orm import Session

from app.models import PendingSynthesis, StagingRecord


def suspend_staging(
    session: Session,
    staging_id: int,
    reason: str,
    required_entity_type: str | None = None,
    required_key: str | None = None,
    required_value: str | None = None,
) -> StagingRecord:
    """Mark a staging record as suspended and optionally record a dependency."""
    staging = session.get(StagingRecord, staging_id)
    if not staging:
        raise ValueError(f"Staging record {staging_id} not found")

    staging.status = "suspended"
    staging.suspension_reason = reason
    session.add(staging)

    if required_entity_type and required_key and required_value:
        pending = PendingSynthesis(
            staging_record_id=staging_id,
            required_entity_type=required_entity_type,
            required_key=required_key,
            required_value=str(required_value),
            status="waiting",
        )
        session.add(pending)

    session.commit()
    return staging


def mark_ready(session: Session, staging_id: int) -> StagingRecord:
    """Mark a staging record as ready for synthesis."""
    staging = session.get(StagingRecord, staging_id)
    if staging:
        staging.status = "ready"
        staging.suspension_reason = None
        session.add(staging)
        session.commit()
    return staging


def mark_failed(session: Session, staging_id: int, error: str) -> StagingRecord:
    """Mark a staging record as failed with an error message."""
    staging = session.get(StagingRecord, staging_id)
    if staging:
        staging.status = "failed"
        staging.suspension_reason = error
        session.add(staging)
        session.commit()
    return staging
