from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base

# Supports native JSONB on PostgreSQL and fallback JSON on SQLite/other dialects
JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


def _utcnow() -> datetime:
    """Return current UTC time with timezone awareness."""
    return datetime.now(UTC)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )


class DataSource(TimestampMixin, Base):
    __tablename__ = "data_sources"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    adapter: Mapped[str] = mapped_column(String(120), nullable=False)
    access_mode: Mapped[str] = mapped_column(String(60), nullable=False)
    schedule: Mapped[str | None] = mapped_column(String(120))
    availability_window: Mapped[str | None] = mapped_column(String(200))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)


class RawRecord(TimestampMixin, Base):
    __tablename__ = "raw_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), index=True)
    batch_id: Mapped[str | None] = mapped_column(String(120), index=True)
    content_type: Mapped[str] = mapped_column(String(120))
    raw_data: Mapped[dict[str, Any] | None] = mapped_column(JSON_TYPE)
    file_path: Mapped[str | None] = mapped_column(String(500))
    checksum: Mapped[str] = mapped_column(String(64), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    source: Mapped[DataSource] = relationship()


class StagingRecord(TimestampMixin, Base):
    __tablename__ = "staging_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_record_id: Mapped[int] = mapped_column(ForeignKey("raw_records.id"), index=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("data_sources.id"), index=True)
    entity_type: Mapped[str] = mapped_column(String(120), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE)
    record_hash: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(40), default="pending", index=True)
    suspension_reason: Mapped[str | None] = mapped_column(Text)
    synthesis_run_id: Mapped[int | None] = mapped_column(ForeignKey("synthesis_runs.id"))

    raw_record: Mapped[RawRecord] = relationship()
    synthesis_run: Mapped[SynthesisRun] = relationship(back_populates="staging_records")

    __table_args__ = (
        UniqueConstraint("raw_record_id", "record_hash", name="uq_staging_raw_hash"),
    )


class PendingSynthesis(TimestampMixin, Base):
    __tablename__ = "pending_synthesis"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    staging_record_id: Mapped[int] = mapped_column(ForeignKey("staging_records.id"), index=True)
    required_entity_type: Mapped[str] = mapped_column(String(120))
    required_key: Mapped[str] = mapped_column(String(120))
    required_value: Mapped[str] = mapped_column(String(300))
    status: Mapped[str] = mapped_column(String(40), default="waiting", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    staging_record: Mapped[StagingRecord] = relationship()


class Agency(TimestampMixin, Base):
    __tablename__ = "agencies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    state: Mapped[str | None] = mapped_column(String(2))
    jurisdiction: Mapped[str | None] = mapped_column(String(300))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)


class Officer(TimestampMixin, Base):
    __tablename__ = "officers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    first_name: Mapped[str | None] = mapped_column(String(120))
    last_name: Mapped[str | None] = mapped_column(String(120))
    badge_number: Mapped[str | None] = mapped_column(String(120), index=True)
    employee_id: Mapped[str | None] = mapped_column(String(120), index=True)
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)
    status: Mapped[str | None] = mapped_column(String(80))

    agency: Mapped[Agency] = relationship()


class Person(TimestampMixin, Base):
    __tablename__ = "persons"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str | None] = mapped_column(String(120))
    last_name: Mapped[str | None] = mapped_column(String(120))
    date_of_birth: Mapped[str | None] = mapped_column(String(40))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)


class Incident(TimestampMixin, Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    incident_type: Mapped[str] = mapped_column(String(120), index=True)
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    location: Mapped[str | None] = mapped_column(String(500))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)
    data: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    agency: Mapped[Agency] = relationship()


class Complaint(TimestampMixin, Base):
    __tablename__ = "complaints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    complaint_type: Mapped[str] = mapped_column(String(120))
    filed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str | None] = mapped_column(String(80))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    agency: Mapped[Agency] = relationship()


class Arrest(TimestampMixin, Base):
    __tablename__ = "arrests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    incident_id: Mapped[int | None] = mapped_column(ForeignKey("incidents.id"), index=True)
    person_id: Mapped[int | None] = mapped_column(ForeignKey("persons.id"), index=True)
    arrested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    booking_number: Mapped[str | None] = mapped_column(String(120), index=True)
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    incident: Mapped[Incident] = relationship()
    person: Mapped[Person] = relationship()


class Charge(TimestampMixin, Base):
    __tablename__ = "charges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    arrest_id: Mapped[int | None] = mapped_column(ForeignKey("arrests.id"), index=True)
    statute: Mapped[str | None] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[str | None] = mapped_column(String(80))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    arrest: Mapped[Arrest] = relationship()


class CourtCase(TimestampMixin, Base):
    __tablename__ = "court_cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_number: Mapped[str] = mapped_column(String(200), index=True, unique=True)
    court: Mapped[str | None] = mapped_column(String(200))
    filed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str | None] = mapped_column(String(80))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)


class Document(TimestampMixin, Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("data_sources.id"), index=True)
    doc_type: Mapped[str] = mapped_column(String(120))
    title: Mapped[str | None] = mapped_column(String(500))
    file_path: Mapped[str | None] = mapped_column(String(500))
    text: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    source: Mapped[DataSource] = relationship()


class NewsArticle(TimestampMixin, Base):
    __tablename__ = "news_articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("data_sources.id"), index=True)
    title: Mapped[str] = mapped_column(String(500))
    url: Mapped[str] = mapped_column(String(1000), index=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    content: Mapped[str | None] = mapped_column(Text)
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    source: Mapped[DataSource] = relationship()


class SurveillanceEvent(TimestampMixin, Base):
    __tablename__ = "surveillance_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(120))
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    location: Mapped[str | None] = mapped_column(String(500))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSON_TYPE, default=dict)

    agency: Mapped[Agency] = relationship()


class InternalAffairsCase(TimestampMixin, Base):
    __tablename__ = "internal_affairs_cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    officer_id: Mapped[int | None] = mapped_column(ForeignKey("officers.id"), index=True)
    case_number: Mapped[str | None] = mapped_column(String(120), index=True)
    status: Mapped[str | None] = mapped_column(String(80))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    external_ids: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    agency: Mapped[Agency] = relationship()
    officer: Mapped[Officer] = relationship()


class MonitorReport(TimestampMixin, Base):
    __tablename__ = "monitor_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agency_id: Mapped[int | None] = mapped_column(ForeignKey("agencies.id"), index=True)
    period: Mapped[str | None] = mapped_column(String(120))
    report_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    compliance_data: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)
    document_id: Mapped[int | None] = mapped_column(ForeignKey("documents.id"))

    agency: Mapped[Agency] = relationship()
    document: Mapped[Document] = relationship()


class EntityLink(TimestampMixin, Base):
    __tablename__ = "entity_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_entity: Mapped[str] = mapped_column(String(120))
    source_id: Mapped[int] = mapped_column(Integer)
    target_entity: Mapped[str] = mapped_column(String(120))
    target_id: Mapped[int] = mapped_column(Integer)
    relation_type: Mapped[str] = mapped_column(String(120))
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    join_key: Mapped[str | None] = mapped_column(String(200))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSON_TYPE, default=dict)

    __table_args__ = (
        Index("ix_entity_links_source", "source_entity", "source_id"),
        Index("ix_entity_links_target", "target_entity", "target_id"),
    )


class SynthesisRun(TimestampMixin, Base):
    __tablename__ = "synthesis_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(40), default="running")
    stats: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict)

    staging_records: Mapped[list[StagingRecord]] = relationship(back_populates="synthesis_run")
