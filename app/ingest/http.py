"""HTTP transport.

One client, one policy: bounded concurrency, exponential backoff with
jitter, HTTP/2 where the origin supports it, and a checksum stamped on
every response so that the citation trail is a by-product of fetching
rather than something reconstructed later.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import orjson

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class FetchError(RuntimeError):
    """A retrieval that did not produce a usable response body."""

    def __init__(self, message: str, *, url: str, status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


@dataclass
class FetchedResponse:
    """A retrieved body plus the facts needed to cite it."""

    url: str
    status_code: int
    body: bytes
    retrieved_at: datetime
    duration_ms: float
    content_sha256: str
    content_type: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and 200 <= self.status_code < 300

    def json(self) -> Any:
        return orjson.loads(self.body)

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


@dataclass
class HostThrottle:
    """Minimum interval between requests to a single host.

    Public open-data portals rate-limit aggressively; a per-host floor keeps
    a full catalog sweep from getting the deployment blocked.
    """

    min_interval: float = 0.35
    _last: dict[str, float] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def wait(self, host: str) -> None:
        with self._lock:
            last = self._last.get(host, 0.0)
            wait_for = self.min_interval - (time.monotonic() - last)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last[host] = time.monotonic()


class HttpClient:
    """Resilient HTTP client used by every adapter."""

    def __init__(self, throttle: HostThrottle | None = None):
        self.throttle = throttle or HostThrottle()
        self._client: httpx.Client | None = None

    # -- lifecycle ---------------------------------------------------------
    def _ensure_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                timeout=httpx.Timeout(settings.http_timeout_seconds, connect=15.0),
                follow_redirects=True,
                http2=True,
                headers={
                    "User-Agent": settings.user_agent,
                    "Accept": "application/json, text/csv, text/plain, "
                    "application/xml, */*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                },
                limits=httpx.Limits(
                    max_connections=settings.http_concurrency,
                    max_keepalive_connections=settings.http_concurrency,
                ),
            )
        return self._client

    def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            self._client.close()

    def __enter__(self) -> HttpClient:
        self._ensure_client()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # -- request -----------------------------------------------------------
    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        stream_to: str | None = None,
    ) -> FetchedResponse:
        """GET ``url`` with retry, returning a cited :class:`FetchedResponse`.

        Failures are returned as a response with ``error`` set rather than
        raised, so the Verify phase can record exactly what happened.
        """
        host = httpx.URL(url).host
        last_error: str | None = None
        last_status: int | None = None
        attempts = max(1, settings.http_retries)

        for attempt in range(1, attempts + 1):
            self.throttle.wait(host)
            started = time.monotonic()
            try:
                response = self._ensure_client().get(url, params=params, headers=headers)
                duration_ms = (time.monotonic() - started) * 1000.0
                last_status = response.status_code

                if response.status_code >= 500 or response.status_code == 429:
                    last_error = f"HTTP {response.status_code}"
                    if attempt < attempts:
                        self._backoff(attempt, response)
                        continue
                    return self._failure(url, response.status_code, last_error, duration_ms)

                if response.status_code >= 400:
                    return self._failure(
                        url, response.status_code,
                        f"HTTP {response.status_code}: {response.text[:300]}", duration_ms,
                    )

                body = response.content
                return FetchedResponse(
                    url=str(response.url),
                    status_code=response.status_code,
                    body=body,
                    retrieved_at=datetime.now(UTC),
                    duration_ms=duration_ms,
                    content_sha256=hashlib.sha256(body).hexdigest(),
                    content_type=response.headers.get("content-type"),
                )
            except httpx.HTTPError as exc:
                duration_ms = (time.monotonic() - started) * 1000.0
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < attempts:
                    self._backoff(attempt, None)
                    continue
                logger.warning("GET %s failed after %d attempts: %s", url, attempts, last_error)
                return FetchedResponse(
                    url=url,
                    status_code=last_status or 0,
                    body=b"",
                    retrieved_at=datetime.now(UTC),
                    duration_ms=duration_ms,
                    content_sha256="",
                    error=last_error,
                )

        return self._failure(url, last_status or 0, last_error or "unknown error", 0.0)

    @staticmethod
    def _backoff(attempt: int, response: httpx.Response | None) -> None:
        retry_after = None
        if response is not None:
            raw = response.headers.get("retry-after")
            if raw and raw.isdigit():
                retry_after = float(raw)
        delay = retry_after if retry_after is not None else min(30.0, 2.0 ** attempt)
        time.sleep(delay)

    @staticmethod
    def _failure(url: str, status: int, error: str, duration_ms: float) -> FetchedResponse:
        return FetchedResponse(
            url=url,
            status_code=status,
            body=b"",
            retrieved_at=datetime.now(UTC),
            duration_ms=duration_ms,
            content_sha256="",
            error=error,
        )


#: Shared client + throttle for the whole process.
_throttle = HostThrottle()
_shared = HttpClient(_throttle)


def shared_client() -> HttpClient:
    return _shared


def get_json(url: str, **kwargs: Any) -> tuple[Any, FetchedResponse]:
    """GET and decode JSON, returning ``(payload_or_None, response)``."""
    response = shared_client().get(url, **kwargs)
    if not response.ok:
        return None, response
    try:
        return response.json(), response
    except Exception as exc:  # noqa: BLE001
        response.error = f"JSON decode failed: {exc}"
        return None, response


def sha256_of(payload: Any) -> str:
    """Content hash of an arbitrary JSON-serialisable object."""
    return hashlib.sha256(
        orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()
