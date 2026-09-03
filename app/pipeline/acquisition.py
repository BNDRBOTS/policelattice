from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.ingestion.base import AdapterRegistry, BaseAdapter, RawRecordDTO

logger = logging.getLogger(__name__)

# Default verified public URLs for autonomous acquisition
DEFAULT_PUBLIC_URLS: dict[str, str] = {
    # News Feeds
    "ABC15_RSS_URL": "https://www.abc15.com/news.rss",
    "FOX10_RSS_URL": "https://www.fox10phoenix.com/feeds/rss/category/news",
    "KTAR_RSS_URL": "https://ktar.com/feed/",
    "PROPUBLICA_RSS_URL": "https://www.propublica.org/feeds/propublica/main",
    "CRONKITE_RSS_URL": "https://cronkitenews.azpbs.org/feed/",
    "PHOENIX_NEW_TIMES_RSS_URL": "https://www.phoenixnewtimes.com/phoenix/Rss.xml",
    "AZ_FREE_NEWS_RSS_URL": "https://azfreenews.com/feed/",
    # Open Data Portals
    "PHX_OPEN_DATA_PORTAL_DOMAIN": "phoenixopendata.com",
    "TEMPE_OPEN_DATA_URL": "https://data.tempe.gov",
}


@dataclass
class AcquisitionResult:
    """Result of an autonomous acquisition execution across sources."""

    source_id: str
    record_count: int = 0
    records: list[RawRecordDTO] = field(default_factory=list)
    status: str = "success"
    error: str | None = None
    duration_ms: float = 0.0


class AutonomousAcquisitionManager:
    """Orchestrates autonomous acquisition across all configured data sources.

    Designed for high efficiency on Railway compute:
    - Bounded thread pool concurrency (max 4-5 workers)
    - Lightweight streaming and bounded timeouts
    - Graceful fallback on network unreachable states
    """

    def __init__(self, max_workers: int = 4, request_timeout: int = 8):
        self.max_workers = max_workers
        self.request_timeout = request_timeout

    def acquire_source(self, source_def: dict[str, Any]) -> AcquisitionResult:
        """Execute autonomous fetch for a single source definition."""
        source_id = source_def.get("id", "unknown")
        adapter_name = source_def.get("adapter", "manual")
        start_time = datetime.now(UTC)

        try:
            adapter_cls = AdapterRegistry.get(adapter_name)
            adapter: BaseAdapter = adapter_cls(source_def)

            # Check if default public URL can be injected if env var is missing
            inner_config = source_def.get("config", {})
            for env_key in ("url_env", "domain_env", "service_url_env"):
                env_var_name = inner_config.get(env_key)
                if env_var_name and env_var_name in DEFAULT_PUBLIC_URLS:
                    if not adapter.source_config.get(env_var_name.lower()):
                        pub_url = DEFAULT_PUBLIC_URLS[env_var_name]
                        adapter.source_config[env_var_name.lower()] = pub_url

            raw_records = adapter.fetch()
            elapsed = (datetime.now(UTC) - start_time).total_seconds() * 1000.0

            return AcquisitionResult(
                source_id=source_id,
                record_count=len(raw_records),
                records=raw_records,
                status="success",
                duration_ms=elapsed,
            )
        except Exception as exc:
            elapsed = (datetime.now(UTC) - start_time).total_seconds() * 1000.0
            logger.warning("[%s] Autonomous acquisition skipped/failed: %s", source_id, exc)
            return AcquisitionResult(
                source_id=source_id,
                record_count=0,
                records=[],
                status="error",
                error=str(exc),
                duration_ms=elapsed,
            )

    def acquire_all(
        self, sources: list[dict[str, Any]], parallel: bool = True
    ) -> dict[str, AcquisitionResult]:
        """Fetch all provided data sources concurrently with thread pool safety."""
        results: dict[str, AcquisitionResult] = {}

        if not parallel:
            for s in sources:
                res = self.acquire_source(s)
                results[s["id"]] = res
            return results

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_source = {
                executor.submit(self.acquire_source, s): s["id"] for s in sources
            }
            for future in as_completed(future_to_source):
                source_id = future_to_source[future]
                try:
                    res = future.result()
                    results[source_id] = res
                except Exception as exc:
                    results[source_id] = AcquisitionResult(
                        source_id=source_id,
                        status="error",
                        error=str(exc),
                    )

        return results
