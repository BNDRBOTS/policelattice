"""Shared live-data acquisition client for all ingestion adapters.

Best-in-class, benchmark-validated stack:
- ``httpx``: pooled connections, HTTP/2 support, hard per-request timeouts.
- ``orjson``: the fastest RFC-compliant JSON parser for Python (written in
  Rust; consistently tops keyword/serialization benchmarks).
- ``tenacity``: bounded exponential-backoff retries for transient transport
  failures only (connect/read timeouts, 5xx, 429). 4xx responses are treated
  as authoritative source answers and are never retried.

This module performs LIVE network I/O only. It contains no caching of
responses, no fabricated fallback bodies, and no offline simulation of any
kind. If a source cannot be reached, the failure propagates to the caller so
the Verify phase can record it against the source.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import orjson
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from app.config import get_settings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class LiveSourceError(RuntimeError):
    """Raised when a live source request fails after bounded retries."""


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, LiveSourceError):
        return exc.args[-1] if isinstance(exc.args, tuple) and exc.args else False
    return False


class LiveFetchClient:
    """One shared, pooled HTTP client for every adapter in the process."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: httpx.Client | None = None

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=httpx.Timeout(self.settings.fetch_timeout_seconds, connect=10.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                headers={"User-Agent": self.settings.fetch_user_agent},
                follow_redirects=True,
            )
        return self._client

    # -- core requests -----------------------------------------------------

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def get_bytes(self, url: str, *, headers: dict[str, str] | None = None) -> bytes:
        """GET a live URL and return the exact response body bytes."""
        try:
            resp = self.client.get(url, headers=headers or {})
        except httpx.HTTPError as exc:
            raise LiveSourceError(f"transport failure for {url}: {exc}", True) from exc
        if resp.status_code in RETRYABLE_STATUS:
            raise LiveSourceError(
                f"retryable HTTP {resp.status_code} for {url}", True
            )
        if resp.status_code >= 400:
            raise LiveSourceError(
                f"HTTP {resp.status_code} for {url} (authoritative source answer; not retried)",
                False,
            )
        return resp.content

    def get_json(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
        """GET a live URL and parse the body with orjson."""
        body = self.get_bytes(url, headers=headers)
        try:
            return orjson.loads(body)
        except orjson.JSONDecodeError as exc:
            raise LiveSourceError(f"invalid JSON from {url}: {exc}", False) from exc

    def get_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        return self.get_bytes(url, headers=headers).decode("utf-8", errors="replace")


# Process-wide shared client (created lazily; bounded pool for Railway compute)
_shared_client: LiveFetchClient | None = None


def get_fetch_client() -> LiveFetchClient:
    global _shared_client
    if _shared_client is None:
        _shared_client = LiveFetchClient()
    return _shared_client


def parse_json_bytes(body: bytes) -> Any:
    """Parse JSON bytes with orjson (best-in-class parser)."""
    return orjson.loads(body)
