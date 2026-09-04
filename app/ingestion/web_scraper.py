"""Live web-page scraper for public portals without a documented API.

HTML parsing uses ``BeautifulSoup`` over the ``lxml`` engine — libxml2-backed
and the fastest production-grade HTML parser available in Python (dominates
html.parser in parsing benchmarks while remaining lenient with real-world
markup).
"""

from __future__ import annotations

import logging
import os

from bs4 import BeautifulSoup

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class WebScraperAdapter(BaseAdapter):
    """Fetches a live public web page and extracts its text and links."""

    name = "web_scraper"
    access_mode = "api"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        url_env = config.get("url_env")
        url = None
        if url_env:
            url = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
        url = url or config.get("url")
        if not url:
            self.log_skip("No live URL configured for scraper")
            return []

        try:
            html = get_fetch_client().get_text(url)
        except LiveSourceError as exc:
            self.log_skip(f"Scrape failed for {url}: {exc}")
            return []

        soup = BeautifulSoup(html, "lxml")
        title = soup.title.get_text(strip=True) if soup.title else ""
        # Full text extraction — no truncation of extracted content.
        text = soup.get_text(" ", strip=True)

        links = []
        for a in soup.find_all("a", href=True):
            label = a.get_text(strip=True)
            if label:
                links.append({"label": label, "href": a["href"]})

        return [
            RawRecordDTO(
                content_type="text/html",
                payload={
                    "url": url,
                    "title": title,
                    "text": text,
                    "links": links,
                },
                source_id=self.source_config.get("id"),
                metadata={"adapter": self.name, "url": url, "live_url": url},
            )
        ]
