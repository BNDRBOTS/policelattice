from __future__ import annotations

import os

import feedparser

from app.ingestion.base import BaseAdapter, RawRecordDTO


class NewsRssAdapter(BaseAdapter):
    """Ingests RSS feeds from news outlets.

    Requires an explicitly configured RSS URL for each source. If the URL is
    missing, the adapter skips and logs a manual action.
    """

    name = "news_rss"
    access_mode = "rss"

    def fetch(self) -> list[RawRecordDTO]:
        url_env = self.source_config.get("url_env")
        if not url_env:
            self.log_skip("No url_env configured")
            return []

        url = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
        if not url:
            self.log_skip(f"RSS URL env {url_env} not set")
            return []

        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            self.log_skip(f"RSS parse error for {url}: {feed.bozo_exception}")
            return []

        records = []
        for entry in feed.entries:
            payload = {
                "title": entry.get("title"),
                "link": entry.get("link"),
                "published": entry.get("published"),
                "summary": entry.get("summary"),
                "author": entry.get("author"),
            }
            records.append(
                RawRecordDTO(
                    content_type="application/rss+xml",
                    payload={"entry": payload},
                    source_id=self.source_config.get("id"),
                    metadata={"adapter": self.name, "feed_url": url},
                )
            )
        return records
