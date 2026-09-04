"""Lattice schema.

Design rule enforced throughout: **no column has a default that could be
mistaken for source data.** Every provenance column is ``nullable=False``
because a record without a known source is not admissible, and every
descriptive column is nullable because the source may not state it.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class DataSource(Base):
    """A configured upstream source and its most recent verified state."""

    __tablename__ = "data_sources"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    kind: Mapped[str] = mapped_column(String(40))  # ckan|arcgis_hub|http_tabular|rss
    publisher: Mapped[str | None] = mapped_column(String(255))
    endpoint: Mapped[str | None] = mapped_column(Text)
    schedule: Mapped[str | None] = mapped_column(String(60))
    entity_type: Mapped[str | None] = mapped_column(String(60))

    # Verification state — written by the Verify phase, never assumed.
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_ok: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer)
    last_error: Mapped[str | None] = mapped_column(Text)
    rows_total_reported: Mapped[int | None] = mapped_column(Integer)

    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rows_fetched_last_run: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rows_new_last_run: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class FetchLog(Base):
    """One HTTP retrieval. This is the citation trail for every record."""

    __tablename__ = "fetch_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String(120), index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer)
    ok: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    duration_ms: Mapped[float | None] = mapped_column(Float)
    content_bytes: Mapped[int | None] = mapped_column(Integer)
    content_sha256: Mapped[str | None] = mapped_column(String(64), index=True)
    rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)


class RawRecord(Base):
    """An immutable, content-addressed copy of one upstream row."""

    __tablename__ = "raw_records"
    __table_args__ = (
        UniqueConstraint("source_id", "content_sha256", name="uq_raw_source_checksum"),
        Index("ix_raw_source_period", "source_id", "period"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    dataset: Mapped[str | None] = mapped_column(String(200))
    resource_id: Mapped[str | None] = mapped_column(String(120))
    external_id: Mapped[str | None] = mapped_column(String(200), index=True)

    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period: Mapped[str | None] = mapped_column(String(7), index=True)  # YYYY-MM
    synthesized: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class Agency(Base):
    __tablename__ = "agencies"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    jurisdiction: Mapped[str | None] = mapped_column(String(255))
    source_url: Mapped[str | None] = mapped_column(Text)

    officers: Mapped[list[OfficerRef]] = relationship(back_populates="agency")


class OfficerRef(Base):
    """A law-enforcement employee as identified by the source itself.

    Phoenix publishes ``UNIQUE_INCIDENT_OFFICER`` — a stable pseudonymous
    employee identifier. This table stores that identifier verbatim. No
    name, badge number or rank is ever invented: where the source does not
    publish one, the column stays ``None``.
    """

    __tablename__ = "officer_refs"
    __table_args__ = (
        UniqueConstraint("agency_id", "external_key", name="uq_officer_agency_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[str] = mapped_column(
        String(120), ForeignKey("agencies.id"), nullable=False, index=True
    )
    external_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    # Only ever populated from source fields that describe the employee.
    gender: Mapped[str | None] = mapped_column(String(60))
    race_group: Mapped[str | None] = mapped_column(String(120))
    rank: Mapped[str | None] = mapped_column(String(120))
    hire_year: Mapped[int | None] = mapped_column(Integer)

    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_url: Mapped[str | None] = mapped_column(Text)

    agency: Mapped[Agency] = relationship(back_populates="officers")


class Incident(Base):
    """A use-of-force, shooting, show-of-force or gun-pointing event."""

    __tablename__ = "incidents"
    __table_args__ = (
        UniqueConstraint("agency_id", "external_number", "kind", name="uq_incident_ext"),
        Index("ix_incident_period_kind", "period", "kind"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[str] = mapped_column(
        String(120), ForeignKey("agencies.id"), nullable=False, index=True
    )
    external_number: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    period: Mapped[str | None] = mapped_column(String(7), index=True)

    # Use-of-force taxonomy — populated only from source columns.
    force_level: Mapped[str | None] = mapped_column(String(120))
    highest_force_applied: Mapped[str | None] = mapped_column(String(160))
    armed_type: Mapped[str | None] = mapped_column(String(160))
    resistance: Mapped[str | None] = mapped_column(String(160))
    de_escalation: Mapped[str | None] = mapped_column(String(160))
    injury: Mapped[str | None] = mapped_column(String(120))
    highest_charge: Mapped[str | None] = mapped_column(String(200))
    outcome: Mapped[str | None] = mapped_column(String(160))

    subject_gender: Mapped[str | None] = mapped_column(String(60))
    subject_race_group: Mapped[str | None] = mapped_column(String(120))
    subject_age_group: Mapped[str | None] = mapped_column(String(60))

    location: Mapped[str | None] = mapped_column(Text)
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)
    precinct: Mapped[str | None] = mapped_column(String(120))

    # Provenance — mandatory.
    source_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class ForceEvent(Base):
    """The officer x incident edge carrying the accountability outcome."""

    __tablename__ = "force_events"
    __table_args__ = (
        UniqueConstraint("officer_ref_id", "incident_id", name="uq_force_event"),
        Index("ix_force_event_officer_period", "officer_ref_id", "period"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    officer_ref_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("officer_refs.id"), nullable=False, index=True
    )
    incident_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("incidents.id"), nullable=False, index=True
    )
    period: Mapped[str | None] = mapped_column(String(7), index=True)

    # ``EMP_WITHIN_POLICY`` in Phoenix's data: Yes / No / Not Available.
    within_policy: Mapped[str | None] = mapped_column(String(60), index=True)
    bwc_activated: Mapped[str | None] = mapped_column(String(60))
    force_applied: Mapped[str | None] = mapped_column(String(160))
    force_level: Mapped[str | None] = mapped_column(String(120))
    officers_on_scene: Mapped[int | None] = mapped_column(Integer)

    source_id: Mapped[str] = mapped_column(String(120), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class Arrest(Base):
    __tablename__ = "arrests"
    __table_args__ = (Index("ix_arrest_period", "period"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[str] = mapped_column(
        String(120), ForeignKey("agencies.id"), nullable=False, index=True
    )
    external_number: Mapped[str | None] = mapped_column(String(160), index=True)
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    period: Mapped[str | None] = mapped_column(String(7), index=True)

    charge: Mapped[str | None] = mapped_column(String(255))
    charge_code: Mapped[str | None] = mapped_column(String(120))
    disposition: Mapped[str | None] = mapped_column(String(160))
    subject_gender: Mapped[str | None] = mapped_column(String(60))
    subject_race_group: Mapped[str | None] = mapped_column(String(120))
    subject_age_group: Mapped[str | None] = mapped_column(String(60))
    location: Mapped[str | None] = mapped_column(Text)
    precinct: Mapped[str | None] = mapped_column(String(120))

    officer_ref_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("officer_refs.id"), index=True
    )

    source_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class Complaint(Base):
    """Internal-affairs complaint / reprimand / liability claim records."""

    __tablename__ = "complaints"
    __table_args__ = (Index("ix_complaint_period", "period"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[str] = mapped_column(
        String(120), ForeignKey("agencies.id"), nullable=False, index=True
    )
    external_number: Mapped[str | None] = mapped_column(String(160), index=True)
    category: Mapped[str | None] = mapped_column(String(200))
    allegation: Mapped[str | None] = mapped_column(Text)
    finding: Mapped[str | None] = mapped_column(String(200))
    discipline: Mapped[str | None] = mapped_column(String(200))
    status: Mapped[str | None] = mapped_column(String(200))
    amount_paid: Mapped[float | None] = mapped_column(Float)
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    period: Mapped[str | None] = mapped_column(String(7), index=True)

    officer_ref_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("officer_refs.id"), index=True
    )

    source_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class NewsItem(Base):
    """A news article retrieved from a live feed."""

    __tablename__ = "news_items"
    __table_args__ = (UniqueConstraint("url", name="uq_news_url"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    period: Mapped[str | None] = mapped_column(String(7), index=True)
    summary: Mapped[str | None] = mapped_column(Text)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)


class EntityLink(Base):
    """An explicit, evidenced join between two lattice entities."""

    __tablename__ = "entity_links"
    __table_args__ = (
        UniqueConstraint(
            "source_type", "source_id", "target_type", "target_id", "relation",
            name="uq_entity_link",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    target_id: Mapped[int] = mapped_column(Integer, nullable=False)
    relation: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    join_key: Mapped[str | None] = mapped_column(String(200))
    confidence: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    period: Mapped[str | None] = mapped_column(String(7), index=True)
    evidence: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class OfficerFinding(Base):
    """One statistically derived, plain-language statement about one officer."""

    __tablename__ = "officer_findings"
    __table_args__ = (
        UniqueConstraint(
            "officer_ref_id", "period", "finding_type", "metric", name="uq_finding"
        ),
        Index("ix_finding_period_severity", "period", "severity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    officer_ref_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("officer_refs.id"), nullable=False, index=True
    )
    agency_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    period: Mapped[str] = mapped_column(String(7), nullable=False, index=True)
    window_start: Mapped[str | None] = mapped_column(String(10))
    window_end: Mapped[str | None] = mapped_column(String(10))

    finding_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    metric: Mapped[str] = mapped_column(String(80), nullable=False)
    value: Mapped[float | None] = mapped_column(Float)
    numerator: Mapped[int | None] = mapped_column(Integer)
    denominator: Mapped[int | None] = mapped_column(Integer)
    peer_value: Mapped[float | None] = mapped_column(Float)
    peer_numerator: Mapped[int | None] = mapped_column(Integer)
    peer_denominator: Mapped[int | None] = mapped_column(Integer)
    peer_count: Mapped[int | None] = mapped_column(Integer)
    robust_z: Mapped[float | None] = mapped_column(Float)
    p_value: Mapped[float | None] = mapped_column(Float)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    narrative: Mapped[str] = mapped_column(Text, nullable=False)
    sources: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PipelineRun(Base):
    """Audit row for one Search->Gather->Organize->Process->Verify->Synthesize pass."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    trigger: Mapped[str] = mapped_column(String(40), nullable=False)
    ok: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    phases: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error: Mapped[str | None] = mapped_column(Text)


class MonthlySnapshot(Base):
    """One immutable, content-addressed archive of a whole month's lattice.

    Sealing makes a month append-only history: once ``sealed_at`` is set the
    row is never updated, and a later run for the same period writes a new
    row with ``revision + 1`` and its own hash. Nothing is ever overwritten.
    """

    __tablename__ = "monthly_snapshots"
    __table_args__ = (UniqueConstraint("period", "revision", name="uq_snapshot_rev"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    period: Mapped[str] = mapped_column(String(7), nullable=False, index=True)
    revision: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    counts: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class LatticeMeta(Base):
    """Single-row markers (schema version, last refresh)."""

    __tablename__ = "lattice_meta"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
