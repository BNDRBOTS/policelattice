from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager

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
            Base.metadata.create_all(bind=engine)
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
