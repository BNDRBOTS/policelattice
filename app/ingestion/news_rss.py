"""Live RSS/Atom news ingestion.

``feedparser`` is the reference-standard Python RSS/Atom parser (robust against
malformed feeds, handles RSS 1.0/2.0, Atom, and RDF). Feeds are downloaded
live; entries are preserved in full (no truncation of summaries).
"""

from __future__ import annotations

import logging
import os

import feedparser

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class NewsRssAdapter(BaseAdapter):
    """Ingests RSS/Atom feeds live from configured public feed URLs."""

    name = "news_rss"
    access_mode = "rss"

    def fetch(self) -> list[RawRecordDTO]:
        url_env = self.source_config.get("url_env")
        url = None
        if url_env:
            url = (
                getattr(self.settings, url_env.lower(), None)
                or os.getenv(url_env)
                or self.source_config.get(url_env.lower())
            )
        if not url:
            url = self.source_config.get("url")
        if not url:
            self.log_skip(f"No RSS URL configured (env {url_env})")
            return []

        try:
            body = get_fetch_client().get_bytes(url)
        except LiveSourceError as exc:
            self.log_skip(f"RSS fetch failed for {url}: {exc}")
            return []

        feed = feedparser.parse(body)
        if feed.bozo and not feed.entries:
            self.log_skip(f"RSS parse error for {url}: {feed.bozo_exception}")
            return []

        records: list[RawRecordDTO] = []
        for entry in feed.entries:
            payload = {
                "title": entry.get("title"),
                "link": entry.get("link"),
                "published": entry.get("published"),
                "updated": entry.get("updated"),
                "summary": entry.get("summary"),
                "author": entry.get("author"),
                "tags": [t.get("term") for t in entry.get("tags", []) if t.get("term")],
            }
            records.append(
                RawRecordDTO(
                    content_type="application/rss+xml",
                    payload={"entry": payload},
                    source_id=self.source_config.get("id"),
                    metadata={
                        "adapter": self.name,
                        "feed_url": url,
                        "live_url": entry.get("link") or url,
                    },
                )
            )
        return records
