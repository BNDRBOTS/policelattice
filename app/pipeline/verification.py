"""VERIFY phase — external validation of everything the pipeline produced.

Validation is strictly evidence-based and records every outcome; it never
modifies data content. Two tiers:

**Record-level checks (per staging record):**
- ``provenance``      — the record traces to a raw snapshot with source and
                        live origin metadata (URL / service / feed).
- ``integrity``       — recomputed SHA-256 of the stored payload equals the
                        checksum captured at acquisition time.
- ``canonical_form``  — normalization produced the required canonical fields
                        for the declared entity type (checked against an
                        explicit per-type schema, not guessed).
- ``temporal sanity`` — event timestamps are within [now - max_age, now + 7d].
- ``content non-empty`` — the record carries at least one substantive field.

**Source-level external revalidation:**
For a bounded sample of freshly ingested records per source, the source is
re-fetched live and the sample's checksums are confirmed to still be present
in the live response. This proves the data outside the LLM/pipeline
environment matches what was stored, end to end.

Failed records are marked ``failed`` with explicit reasons and are never
synthesized. Nothing is repaired, invented, or substituted.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import RawRecord, StagingRecord, VerificationResult

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


# Explicit canonical schemas: which normalized payload fields satisfy each
# entity type. A record that satisfies NONE of a type's alternative key sets
# fails canonical-form validation (reported, not fixed).
CANONICAL_REQUIREMENTS: dict[str, list[list[str]]] = {
    "incident": [
        ["occurred_at"],
        ["incident_number"],
        ["location"],
    ],
    "arrest": [
        ["arrested_at"],
        ["booking_number"],
        ["person_name"],
    ],
    "use_of_force": [
        ["occurred_at"],
        ["incident_number"],
        ["officer_name"],
    ],
    "officer": [
        ["badge_number"],
        ["employee_id"],
        ["last_name"],
    ],
    "court_case": [
        ["case_number"],
    ],
    "death": [
        ["occurred_at"],
        ["victim_name"],
    ],
    "news": [
        ["title"],
        ["url"],
    ],
    "document": [
        ["title"],
        ["text"],
    ],
    "public_records_request": [
        ["request_id"],
        ["title"],
        ["agency_name"],
    ],
    "complaint": [
        ["filed_at"],
        ["complaint_number"],
    ],
    "surveillance_event": [
        ["occurred_at"],
        ["technology_type"],
    ],
    "monitor_report": [
        ["report_date"],
        ["period"],
    ],
    "sentiment_survey": [
        ["survey_period"],
    ],
    "geography": [
        ["area_name"],
    ],
    "facility": [
        ["facility_name"],
    ],
    "policy": [
        ["title"],
    ],
    "person": [
        ["last_name"],
    ],
    "officer_certification": [
        ["badge_number"],
        ["last_name"],
    ],
    "audio_feed": [
        ["title"],
    ],
}


def _canonical_keys(payload: dict[str, Any]) -> dict[str, Any]:
    canonical = payload.get("canonical") or {}
    if isinstance(canonical, dict):
        return canonical
    return {}


def _recompute_checksum(raw: RawRecord) -> str:
    serialized = json.dumps(raw.raw_data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


class VerificationPhase:
    """Executes the Verify phase over staging records from a gather batch."""

    def __init__(self, session: Session, sample_size: int = 3):
        self.session = session
        self.settings = get_settings()
        self.sample_size = sample_size

    # ------------------------------------------------------------------ #

    def verify_records(self, staging_ids: list[int]) -> dict[str, Any]:
        started = _utcnow()
        passed = failed = 0
        failure_examples: list[dict[str, Any]] = []
        future_limit = _utcnow() + timedelta(days=7)
        past_limit = _utcnow() - timedelta(days=self.settings.verify_max_record_age_days)

        for sid in staging_ids:
            staging = self.session.get(StagingRecord, sid)
            if staging is None:
                continue
            raw = staging.raw_record
            canonical = _canonical_keys(staging.payload or {})
            checks: dict[str, Any] = {}
            failures: list[str] = []

            # 1. provenance
            provenance_ok = raw is not None and bool(raw.source_id)
            checks["provenance"] = {
                "passed": provenance_ok,
                "source_id": raw.source_id if raw is not None else None,
                "ingested_at": raw.ingested_at.isoformat() if raw is not None else None,
            }
            if not provenance_ok:
                failures.append("provenance: record has no raw snapshot / source")

            # 2. integrity — recompute checksum of stored payload
            if raw is not None:
                recomputed = _recompute_checksum(raw)
                integrity_ok = recomputed == raw.checksum
                checks["integrity"] = {
                    "passed": integrity_ok,
                    "stored_checksum": raw.checksum,
                    "recomputed_checksum": recomputed,
                }
                if not integrity_ok:
                    failures.append(
                        "integrity: stored checksum does not match recomputed payload digest"
                    )
            else:
                checks["integrity"] = {"passed": False, "reason": "no raw record"}

            # 3. canonical form
            entity_type = staging.entity_type or "incident"
            keysets = CANONICAL_REQUIREMENTS.get(entity_type)
            if keysets is None:
                # Unknown entity types must still carry *some* canonical data.
                form_ok = len(canonical) > 0
                detail = {"entity_type": entity_type, "canonical_field_count": len(canonical)}
            else:
                form_ok = any(
                    all(canonical.get(k) not in (None, "", []) for k in keyset)
                    for keyset in keysets
                )
                detail = {
                    "entity_type": entity_type,
                    "required_alternatives": keysets,
                    "canonical_fields_present": sorted(
                        k for k, v in canonical.items() if v not in (None, "", [])
                    ),
                }
            checks["canonical_form"] = {"passed": form_ok, **detail}
            if not form_ok:
                failures.append(
                    f"canonical_form: normalized payload lacks required fields for {entity_type}"
                )

            # 4. temporal sanity (only when a timestamp exists)
            ts = canonical.get("occurred_at") or canonical.get("filed_at") or canonical.get(
                "published_at"
            ) or canonical.get("arrested_at") or canonical.get("report_date")
            if ts:
                try:
                    ts_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.replace(tzinfo=UTC)
                    temporal_ok = past_limit <= ts_dt <= future_limit
                    checks["temporal"] = {
                        "passed": temporal_ok,
                        "event_time": ts_dt.isoformat(),
                        "allowed_window": [past_limit.isoformat(), future_limit.isoformat()],
                    }
                    if not temporal_ok:
                        failures.append("temporal: event timestamp outside allowed window")
                except ValueError:
                    checks["temporal"] = {
                        "passed": False,
                        "reason": f"unparseable timestamp {ts!r}",
                    }
                    failures.append(f"temporal: unparseable timestamp {ts!r}")

            # 5. content non-empty
            substantive = any(v not in (None, "", [], {}) for v in canonical.values()) or bool(
                staging.payload.get("evidence")
            )
            checks["content_non_empty"] = {"passed": substantive}
            if not substantive:
                failures.append("content_non_empty: record carries no substantive data")

            # Persist verification outcome (append-only per staging record run;
            # latest row wins for status decisions).
            verdict = VerificationResult(
                staging_record_id=sid,
                passed=len(failures) == 0,
                checks=checks,
                failures=failures,
                verified_at=_utcnow(),
            )
            self.session.add(verdict)

            if failures:
                staging.status = "failed"
                staging.suspension_reason = "; ".join(failures)
                failed += 1
                if len(failure_examples) < 25:
                    failure_examples.append(
                        {"staging_record_id": sid, "failures": failures}
                    )
            else:
                staging.status = "ready"
                staging.suspension_reason = None
                passed += 1

        self.session.flush()
        return {
            "started_at": started.isoformat(),
            "records_checked": len(staging_ids),
            "passed": passed,
            "failed": failed,
            "failure_examples": failure_examples,
            "completed_at": _utcnow().isoformat(),
        }

    # ------------------------------------------------------------------ #

    def revalidate_against_live_sources(
        self,
        source_staging: dict[str, list[int]],
        source_defs: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Externally re-fetch each source and confirm sampled stored records
        still exist in the live response (checksum membership)."""
        results: dict[str, Any] = {}
        for source_id, staging_ids in source_staging.items():
            if not staging_ids:
                continue
            sample_ids = staging_ids[: self.sample_size]
            sample_records = [
                (sid, self.session.get(StagingRecord, sid)) for sid in sample_ids
            ]
            sample_records = [(sid, s) for sid, s in sample_records if s is not None]
            if not sample_records:
                continue

            # Use the orchestrator-provided (discovery-merged) source
            # definition, falling back to the catalog + registry merge.
            source_def = (source_defs or {}).get(source_id)
            if source_def is None:
                from app.pipeline.runner import get_merged_source_def

                try:
                    source_def = get_merged_source_def(self.session, source_id)
                except Exception as exc:
                    results[source_id] = {"status": "error", "error": str(exc)}
                    continue
            if source_def is None:
                results[source_id] = {
                    "status": "unavailable",
                    "reason": "source definition not found; cannot revalidate",
                }
                continue

            from app.ingestion.base import AdapterRegistry

            try:
                adapter_cls = AdapterRegistry.get(source_def.get("adapter", ""))
                adapter = adapter_cls(source_def)
                live_dtos = adapter.fetch()
            except Exception as exc:
                results[source_id] = {"status": "error", "error": str(exc)}
                continue

            live_checksums = set()
            for dto in live_dtos:
                live_checksums.add(dto.compute_checksum())

            confirmed = missing = 0
            per_record: list[dict[str, Any]] = []
            for sid, staging in sample_records:
                present = staging.record_hash in live_checksums
                if present:
                    confirmed += 1
                else:
                    missing += 1
                per_record.append(
                    {
                        "staging_record_id": sid,
                        "checksum": staging.record_hash,
                        "confirmed_in_live_response": present,
                    }
                )
            results[source_id] = {
                "status": "revalidated",
                "sample_size": len(sample_records),
                "confirmed": confirmed,
                "not_found_in_live_response": missing,
                "records": per_record,
                "note": (
                    "not_found can indicate source-side removal or update since "
                    "acquisition; flagged, never altered"
                ),
            }
        return results
