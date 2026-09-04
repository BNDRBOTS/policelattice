"""Live flat-file ingestion adapter (CSV / TSV / XLSX / JSON / NDJSON).

Files are downloaded LIVE from configured URLs on every run — there is no
manual drop directory and no local placeholder path. Parsers:

- CSV/TSV: ``pandas`` (the benchmark-standard Python tabular engine) with
  dtype-inference disabled for identifiers (kept as strings to preserve
  leading zeros in badge/case numbers).
- XLSX: ``pandas`` + ``openpyxl``.
- JSON / NDJSON: ``orjson`` (fastest RFC-compliant JSON parser).
"""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client, parse_json_bytes

logger = logging.getLogger(__name__)


class FlatFileAdapter(BaseAdapter):
    """Downloads and parses a flat file from a live URL."""

    name = "flatfile"
    access_mode = "api"

    def _resolve_urls(self) -> list[str]:
        config = self.source_config
        import os

        urls: list[str] = []
        url_env = config.get("url_env")
        if url_env:
            val = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
            if val:
                urls.append(val)
        for u in config.get("urls", []) or []:
            urls.append(u)
        url = config.get("url")
        if url:
            urls.append(url)
        if not urls:
            self.log_skip("No live flat-file URL configured")
        return urls

    def fetch(self) -> list[RawRecordDTO]:
        client = get_fetch_client()
        records: list[RawRecordDTO] = []
        for url in self._resolve_urls():
            try:
                body = client.get_bytes(url)
            except LiveSourceError as exc:
                self.log_skip(f"Live download failed for {url}: {exc}")
                continue
            records.extend(
                self._parse_body(body, url, filename_hint=url.rsplit("/", 1)[-1] or url)
            )
        return records

    def _parse_body(
        self, body: bytes, url: str, filename_hint: str
    ) -> list[RawRecordDTO]:
        name = filename_hint.lower()
        base_meta = {"adapter": self.name, "live_url": url}

        if name.endswith(".json"):
            return self._parse_json(parse_json_bytes(body), url, base_meta)
        if name.endswith(".ndjson"):
            out = []
            for line in body.decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    out.append(
                        RawRecordDTO(
                            content_type="application/x-ndjson",
                            payload={"row": parse_json_bytes(line.encode())},
                            source_id=self.source_config.get("id"),
                            metadata={**base_meta, "format": "ndjson"},
                        )
                    )
            return out
        if name.endswith((".csv", ".tsv", ".txt")) or b"," in body[:2048]:
            sep = "\t" if name.endswith(".tsv") else ","
            df = pd.read_csv(
                io.BytesIO(body),
                sep=sep,
                dtype=str,
                keep_default_na=False,
                engine="c",
            )
            out = []
            for row in df.to_dict(orient="records"):
                out.append(
                    RawRecordDTO(
                        content_type="text/csv",
                        payload={"row": row},
                        source_id=self.source_config.get("id"),
                        metadata={**base_meta, "format": "csv"},
                    )
                )
            return out
        if name.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(body), dtype=str)
            out = []
            for row in df.to_dict(orient="records"):
                out.append(
                    RawRecordDTO(
                        content_type="application/vnd.ms-excel",
                        payload={"row": row},
                        source_id=self.source_config.get("id"),
                        metadata={**base_meta, "format": "xlsx"},
                    )
                )
            return out

        self.log_skip(f"Unrecognized flat-file format for {url}")
        return []

    def _parse_json(
        self, data: Any, url: str, base_meta: dict[str, Any]
    ) -> list[RawRecordDTO]:
        items = data if isinstance(data, list) else [data]
        # Flatten common live-API envelope shapes without inventing content.
        if isinstance(data, dict):
            for key in ("data", "results", "rows", "records", "items"):
                if isinstance(data.get(key), list):
                    items = data[key]
                    break
        return [
            RawRecordDTO(
                content_type="application/json",
                payload={"row": item} if isinstance(item, dict) else {"value": item},
                source_id=self.source_config.get("id"),
                metadata={**base_meta, "format": "json"},
            )
            for item in items
        ]
