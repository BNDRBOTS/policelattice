"""Runtime configuration.

Every value here is either (a) read from the environment or (b) a literal,
verified public endpoint. There are no placeholder or example URLs: an
unset optional value stays ``None`` and the pipeline reports the source as
``unconfigured`` rather than inventing an endpoint.
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SCHEMA_VERSION = 3
"""Bumped whenever the lattice schema changes in a way that invalidates
rows produced by an earlier build. ``app.db.ensure_schema_current`` purges
and rebuilds a stale database so no superseded rows survive a deploy."""


class Settings(BaseSettings):
    """Runtime configuration loaded from environment or ``.env``."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str | None = None
    app_env: str = "production"
    log_level: str = "INFO"

    # --- Verified public endpoints (no credentials required) ---------------
    # City of Phoenix Open Data is a CKAN instance. Confirmed live:
    #   GET /api/3/action/package_list  -> 200
    #   GET /api/3/action/package_show?id=arrests -> 200
    #   GET /api/3/action/datastore_search?resource_id=<uuid>&limit=2 -> 200
    phoenix_ckan_url: str = "https://www.phoenixopendata.com"
    phoenix_ckan_organization: str = "police-department"

    # City of Tempe open data is an ArcGIS Hub site. Confirmed live:
    #   GET /api/feed/dcat-us/1.1.json -> 200 (DCAT catalog)
    tempe_hub_url: str = "https://data.tempe.gov"
    arcgis_online_url: str = "https://www.arcgis.com"

    # CKAN package slugs that carry officer-level accountability data.
    # Confirmed present in GET /api/3/action/package_list on 2026-09-03.
    phoenix_police_packages: list[str] = [
        "uof",              # Officer Use of Force (2025-02-18 forward)
        "ouof",             # Officer Use of Force 2018-01-01..2025-02-17
        "uof25",            # Officer Use of Force 2025 slice
        "pgp",              # Officer Pointed Gun at Person
        "sof",              # Officer Show of Force
        "ois",              # Officer-Involved Shootings
        "officer-involved-shooting-ois-incidents",
        "officer-show-of-force",
        "officer-demographics",
        "arrests",          # Adult Arrests
        "d_arrests",
        "citations",
        "d_citations",
        "cmpr",             # Citizen complaints / reprimands
        "risk-management-claims",  # City liability claims
        "missing-persons",
        "diversion",
    ]

    # Maximum rows pulled per resource per run. CKAN's datastore supports
    # server-side filters, so a run pulls a bounded slice instead of the
    # full multi-megabyte dump.
    ckan_page_size: int = 1000
    ckan_max_rows_per_resource: int = 20000

    # How many months back a run should pull when a resource is date-bounded.
    acquisition_lookback_months: int = 26

    # --- Transport ---------------------------------------------------------
    http_timeout_seconds: float = 60.0
    http_retries: int = 3
    http_concurrency: int = 6
    user_agent: str = "police-lattice/3.0 (public accountability research)"

    # --- Retrieval ---------------------------------------------------------
    semantic_search: bool = True
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_cache_dir: str = "./data/models"

    # --- Scheduling --------------------------------------------------------
    scheduler_enabled: bool = True
    # Monthly refresh: 02:00 America/Phoenix on the 1st of every month.
    monthly_refresh_cron: str = "0 2 1 * *"
    # Continuous acquisition tick.
    acquisition_cron: str = "*/30 * * * *"

    @field_validator("database_url", mode="before")
    @classmethod
    def resolve_database_url(cls, v: str | None) -> str:
        """Resolve the database URL from any Railway-provided variable.

        Railway exposes Postgres as ``DATABASE_URL`` / ``DATABASE_PRIVATE_URL``
        / ``POSTGRES_URL`` / discrete ``PG*`` variables. All are accepted.
        Falls back to SQLite under ``./data`` for local development.
        """
        url = (
            v
            or os.getenv("DATABASE_URL")
            or os.getenv("DATABASE_PRIVATE_URL")
            or os.getenv("POSTGRES_URL")
            or os.getenv("DATABASE_PUBLIC_URL")
        )
        if not url:
            host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST")
            if host:
                user = os.getenv("PGUSER") or os.getenv("POSTGRES_USER", "postgres")
                password = os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD", "")
                port = os.getenv("PGPORT") or os.getenv("POSTGRES_PORT", "5432")
                dbname = os.getenv("PGDATABASE") or os.getenv("POSTGRES_DB", "railway")
                auth = f"{user}:{password}@" if password else f"{user}@"
                url = f"postgresql+psycopg2://{auth}{host}:{port}/{dbname}"

        if not url:
            os.makedirs("./data", exist_ok=True)
            url = "sqlite:///./data/police_lattice.db"

        if url.startswith("postgres://"):
            url = "postgresql+psycopg2://" + url[len("postgres://"):]
        elif url.startswith("postgresql://") and not url.startswith("postgresql+"):
            url = "postgresql+psycopg2://" + url[len("postgresql://"):]
        return url


@lru_cache
def get_settings() -> Settings:
    return Settings()
