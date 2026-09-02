from __future__ import annotations

import os

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class WebScraperAdapter(BaseAdapter):
    """Minimal web scraper for public portals that do not expose an API.

    If no `url_env` or `url` is configured, the adapter skips. It is not
    intended to replace browser automation; manual exports should be used for
    JavaScript-heavy portals.
    """

    name = "web_scraper"
    access_mode = "manual"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=10))
    def _get(self, url: str) -> requests.Response:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "police-lattice/0.1"})
        resp.raise_for_status()
        return resp

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        url_env = config.get("url_env")
        url = None
        if url_env:
            url = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
        else:
            url = config.get("url")
        if not url:
            self.log_skip("No URL configured for scraper")
            return []

        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.string if soup.title else url
        text = soup.get_text(" ", strip=True)[:5000]

        return [
            RawRecordDTO(
                content_type="text/html",
                payload={"url": url, "title": title, "text": text},
                source_id=self.source_config.get("id"),
                metadata={"adapter": self.name, "url": url},
            )
        ]
