from __future__ import annotations

import os

import feedparser
import requests

from app.ingestion.base import BaseAdapter, RawRecordDTO


class NewsRssAdapter(BaseAdapter):
    """Ingests RSS feeds from news outlets.

    Autonomously uses configured environment variables or verified public feed
    endpoints with strict timeout protection for efficient compute.
    """

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

        if not url and url_env:
            from app.pipeline.acquisition import DEFAULT_PUBLIC_URLS
            url = DEFAULT_PUBLIC_URLS.get(url_env)

        if not url:
            self.log_skip(f"RSS URL env {url_env} not set")
            return []

        try:
            resp = requests.get(
                url,
                timeout=6,
                headers={"User-Agent": "PoliceLattice/1.0 (+https://github.com/BNDRBOTS/policelattice)"},
            )
            if resp.status_code != 200:
                self.log_skip(f"RSS HTTP {resp.status_code} for {url}")
                return []
            feed = feedparser.parse(resp.content)
        except Exception as exc:
            self.log_skip(f"RSS fetch error for {url}: {exc}")
            return []

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
