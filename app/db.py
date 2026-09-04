"""Database engine, session factory, and schema-version enforcement."""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy import DateTime, create_engine, event, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import SCHEMA_VERSION, get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

if settings.database_url.startswith("sqlite:///"):
    _file = settings.database_url[len("sqlite:///"):]
    if not _file.startswith(":memory:"):
        os.makedirs(os.path.dirname(os.path.abspath(_file)) or ".", exist_ok=True)

_connect_args: dict = (
    {"check_same_thread": False, "timeout": 60}
    if settings.database_url.startswith("sqlite")
    else {}
)

engine = create_engine(
    settings.database_url,
    connect_args=_connect_args,
    pool_pre_ping=True,
    future=True,
)


@event.listens_for(engine, "connect")
def _sqlite_pragmas(dbapi_connection, _record):  # pragma: no cover - driver hook
    if settings.database_url.startswith("sqlite"):
        cur = dbapi_connection.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


class Base(DeclarativeBase):
    """Declarative base for every lattice table."""


_timezone_coercion_installed = False


def install_timezone_coercion() -> None:
    """Make every timezone-aware column come back timezone-aware.

    SQLite stores ``DATETIME`` without a UTC offset, so a row written with an
    aware datetime is read back naive — and comparing it against a freshly
    parsed aware datetime raises ``TypeError``. Postgres does not have this
    problem, which makes it a backend-dependent bug that only shows up in
    local runs. Coercing on load fixes it once, for every model and every
    code path, instead of at each comparison site.

    ``set_committed_value`` is used so the coercion does not mark the row
    dirty and trigger a pointless UPDATE.
    """
    global _timezone_coercion_installed
    if _timezone_coercion_installed:
        return

    from sqlalchemy.orm.attributes import set_committed_value

    import app.models  # noqa: F401 - registers mappers

    for mapper in Base.registry.mappers:
        model = mapper.class_
        table = getattr(mapper, "local_table", None)
        if table is None:
            continue
        # ``ColumnProperty.type`` is None in SQLAlchemy 2.x; the type lives on
        # the table column, so the timezone flag has to be read from there.
        keys = [
            column.name
            for column in table.columns
            if isinstance(column.type, DateTime) and column.type.timezone
        ]
        if not keys:
            continue

        @event.listens_for(model, "load")
        def _coerce(target, _context, _keys=keys):
            for key in _keys:
                value = getattr(target, key, None)
                if value is not None and value.tzinfo is None:
                    set_committed_value(target, key, value.replace(tzinfo=UTC))

    _timezone_coercion_installed = True


def utcnow() -> datetime:
    return datetime.now(UTC)


def _drop_all_tables() -> None:
    """Drop every table owned by this application.

    Used only by :func:`ensure_schema_current` when the stored schema
    version does not match the running build. All lattice data is
    re-fetchable from live sources, so a purge costs a re-ingest and
    nothing else.
    """
    import app.models  # noqa: F401 - registers mappers

    inspector = inspect(engine)
    existing = set(inspector.get_table_names())
    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF")) if settings.database_url.startswith(
            "sqlite"
        ) else None
        for table in reversed(Base.metadata.sorted_tables):
            if table.name in existing:
                table.drop(bind=conn, checkfirst=True)


def ensure_schema_current(max_retries: int = 10, retry_delay: int = 3) -> bool:
    """Connect, enforce schema version, create tables.

    A database carrying an older ``SCHEMA_VERSION`` is purged so that rows
    synthesized by superseded code cannot survive a deploy and continue to
    be presented as current.
    """
    import app.models  # noqa: F401 - registers mappers

    target = settings.database_url.split("@")[-1]
    last_exc: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            inspector = inspect(engine)
            if "lattice_meta" in inspector.get_table_names():
                with engine.connect() as conn:
                    row = conn.execute(
                        text("SELECT value FROM lattice_meta WHERE key = 'schema_version'")
                    ).first()
                stored = int(row[0]) if row else 0
                if stored != SCHEMA_VERSION:
                    logger.warning(
                        "Schema version mismatch (stored=%s running=%s) on %s — purging "
                        "superseded lattice rows and rebuilding.",
                        stored, SCHEMA_VERSION, target,
                    )
                    _drop_all_tables()

            Base.metadata.create_all(bind=engine)
            install_timezone_coercion()

            with SessionLocal() as session:
                existing = session.get(app.models.LatticeMeta, "schema_version")
                now = utcnow()
                if existing:
                    existing.value = str(SCHEMA_VERSION)
                    existing.updated_at = now
                else:
                    session.add(
                        app.models.LatticeMeta(
                            key="schema_version", value=str(SCHEMA_VERSION), updated_at=now
                        )
                    )
                session.commit()

            logger.info("Schema ready at version %s (%s)", SCHEMA_VERSION, target)
            return True
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "Database attempt %d/%d to %s failed: %s — retrying in %ss",
                    attempt, max_retries, target, exc, retry_delay,
                )
                time.sleep(retry_delay)

    raise RuntimeError(f"Could not initialize database at {target}: {last_exc}")


@contextmanager
def get_session():
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def set_meta(key: str, value: str) -> None:
    import app.models

    with get_session() as session:
        row = session.get(app.models.LatticeMeta, key)
        if row:
            row.value = value
            row.updated_at = utcnow()
        else:
            session.add(app.models.LatticeMeta(key=key, value=value, updated_at=utcnow()))


def get_meta(key: str) -> str | None:
    import app.models

    with SessionLocal() as session:
        row = session.get(app.models.LatticeMeta, key)
        return row.value if row else None
