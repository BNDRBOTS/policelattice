from __future__ import annotations

import os
from unittest.mock import patch

from app.config import Settings


def test_database_url_default():
    with patch.dict(os.environ, {}, clear=True):
        s = Settings()
        assert "sqlite:///" in s.database_url or "postgresql" in s.database_url


def test_database_url_postgres_scheme_normalization():
    s = Settings(database_url="postgres://user:pass@host:5432/dbname")
    assert s.database_url == "postgresql+psycopg2://user:pass@host:5432/dbname"


def test_database_url_postgresql_scheme_normalization():
    s = Settings(database_url="postgresql://user:pass@host:5432/dbname")
    assert s.database_url == "postgresql+psycopg2://user:pass@host:5432/dbname"


def test_database_url_fallback_from_database_private_url():
    with patch.dict(
        os.environ,
        {
            "DATABASE_PRIVATE_URL": "postgresql://railway_user:pw@postgres.railway.internal:5432/railway",
        },
        clear=True,
    ):
        s = Settings(database_url=None)
        assert s.database_url == "postgresql+psycopg2://railway_user:pw@postgres.railway.internal:5432/railway"


def test_database_url_fallback_from_pg_vars():
    with patch.dict(
        os.environ,
        {
            "PGHOST": "db.host",
            "PGPORT": "5432",
            "PGUSER": "user",
            "PGPASSWORD": "pwd",
            "PGDATABASE": "mydb",
        },
        clear=True,
    ):
        s = Settings(database_url=None)
        assert s.database_url == "postgresql+psycopg2://user:pwd@db.host:5432/mydb"
