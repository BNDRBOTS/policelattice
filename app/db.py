from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Ensure directory for SQLite database exists if applicable
if settings.database_url.startswith("sqlite:///"):
    db_file = settings.database_url.replace("sqlite:///", "")
    if not db_file.startswith(":memory:"):
        db_dir = os.path.dirname(os.path.abspath(db_file))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

connect_args = {}
if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False, "timeout": 30}

engine = create_engine(
    settings.database_url,
    connect_args=connect_args,
    pool_pre_ping=True,
    future=True,
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if settings.database_url.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


IMMUTABLE_TABLES = ("monthly_archive_files", "monthly_refresh_runs")

IMMUTABILITY_TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION police_lattice_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only immutable chron-log; UPDATE/DELETE forbidden',
        TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;
"""

IMMUTABLE_TRIGGER_DDL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = '{tname}_immutable_guard'
          AND tgrelid = '{tname}'::regclass
    ) THEN
        CREATE TRIGGER {tname}_immutable_guard
            BEFORE UPDATE OR DELETE ON {tname}
            FOR EACH ROW EXECUTE FUNCTION police_lattice_reject_mutation();
    END IF;
END $$;
"""


def install_immutability_guards(engine) -> None:
    """Install PostgreSQL triggers that make chron-log tables append-only.

    Only runs on PostgreSQL (the Railway database). On other dialects (e.g.
    SQLite used by unit tests) immutability is enforced by the application
    layer, which exposes no update/delete path for archive rows.
    """
    if engine.dialect.name != "postgresql":
        logger.info(
            "Immutability guards skipped (dialect=%s is not PostgreSQL)",
            engine.dialect.name,
        )
        return
    with engine.begin() as conn:
        conn.execute(text(IMMUTABILITY_TRIGGER_SQL))
        for table in IMMUTABLE_TABLES:
            conn.execute(text(IMMUTABLE_TRIGGER_DDL.format(tname=table)))
    logger.info("Append-only immutability guards installed on %s", ", ".join(IMMUTABLE_TABLES))


SCHEMA_PATCHES_POSTGRES = (
    # (description, idempotent SQL) — applied only on PostgreSQL deployments
    # that predate this schema version.
    (
        "news_articles.url nullable (feeds without links store NULL, not fabricated URLs)",
        "ALTER TABLE news_articles ALTER COLUMN url DROP NOT NULL",
    ),
)


def apply_schema_patches(engine) -> None:
    """Idempotent column patches for databases created before this version."""
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        for description, ddl in SCHEMA_PATCHES_POSTGRES:
            try:
                conn.execute(text(ddl))
                logger.info("Schema patch applied: %s", description)
            except Exception as exc:  # column already nullable, etc.
                logger.debug("Schema patch skipped (%s): %s", description, exc)


# ---------------------------------------------------------------------------
# Schema versioning & automatic purge of legacy (pre-zero-fabrication) data
# ---------------------------------------------------------------------------

# v1 (legacy): pipeline code that fabricated default values (agency
#              attribution, city/state, severity, ranks, statuses).
# v2: zero-fabrication pipeline (honest None / explicit "not recorded"
#              labeling) + verify-phase gating.
SCHEMA_VERSION = 2

MARKER_DDL = (
    "CREATE TABLE IF NOT EXISTS lattice_meta "
    "(key VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL)"
)


def _read_schema_version(engine) -> int | None:
    from sqlalchemy import inspect

    insp = inspect(engine)
    if not insp.has_table("lattice_meta"):
        return None
    with engine.begin() as conn:
        val = conn.execute(
            text("SELECT value FROM lattice_meta WHERE key = 'schema_version'")
        ).scalar()
    return int(val) if val is not None and str(val).isdigit() else None


def ensure_schema_current(engine) -> dict[str, Any]:
    """Guarantee the database matches the current zero-fabrication schema.

    If the schema-version marker is missing (a database written by legacy
    pipeline code) or outdated, ALL pipeline tables are dropped and
    recreated empty. Every row the pipeline stores derives from live,
    re-fetchable public sources, and legacy rows are known to contain
    fabricated values (default agency attribution, invented city/state,
    invented severity). Purging is therefore the only correct autonomous
    action: the startup pipeline re-ingests everything live immediately
    after. Current-version databases are left untouched.
    """
    from sqlalchemy import inspect

    import app.models  # noqa: F401 - register models for drop/create

    current = _read_schema_version(engine)
    if current == SCHEMA_VERSION:
        return {"action": "none", "purged": False}

    insp = inspect(engine)
    legacy_tables = [
        t
        for t in ("incidents", "staging_records", "raw_records", "officers", "news_articles")
        if insp.has_table(t)
    ]
    purged = bool(legacy_tables) or current is not None
    if purged:
        logger.warning(
            "Database schema version %s != current %d: purging ALL pipeline tables "
            "(%d tables) - legacy rows contain values fabricated by pre-v2 code; "
            "all data is re-ingested live by the startup pipeline",
            current,
            SCHEMA_VERSION,
            len(Base.metadata.sorted_tables),
        )
        Base.metadata.drop_all(bind=engine)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS lattice_meta"))

    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text(MARKER_DDL))
        conn.execute(
            text(
                "INSERT INTO lattice_meta (key, value) VALUES ('schema_version', :v)"
            ).bindparams(v=str(SCHEMA_VERSION))
        )
    logger.info("Database at schema version %d", SCHEMA_VERSION)
    return {"action": "purged" if purged else "initialized", "purged": purged}


def init_database_with_retry(max_retries: int = 10, retry_delay: int = 3) -> bool:
    """Attempt to connect to the database and initialize all tables.

    Retries up to `max_retries` times to handle cold starts and container
    orchestration delays in environments like Railway and Docker Compose.
    """
    import app.models  # noqa: F401 - Register all models with Base.metadata

    raw_url = settings.database_url
    db_target = raw_url.split("@")[-1] if "@" in raw_url else raw_url
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            ensure_schema_current(engine)
            install_immutability_guards(engine)
            apply_schema_patches(engine)
            logger.info("Database connection established and schema initialized (%s)", db_target)
            return True
        except Exception as exc:
            if attempt < max_retries:
                logger.warning(
                    "Database connection attempt %d/%d to %s failed: %s. Retrying in %ds...",
                    attempt, max_retries, db_target, exc, retry_delay,
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    "Failed to connect to database at %s after %d attempts: %s",
                    db_target, max_retries, exc,
                )
                raise
    return False


@contextmanager
def get_session():
    """Context manager for database sessions."""
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
