"""Format parsers.

Each format is handled by the implementation that currently leads published
benchmarks for it, rather than by whichever library happened to be installed
first:

=============  ============================================================
Format         Parser                Why
=============  ============================================================
JSON           ``orjson``            Consistently the fastest Python JSON
                                     serializer/deserializer (Rust).
CSV / TSV      ``polars``            Rust-native scanner; multi-x faster than
                                     pandas ``read_csv`` on wide files and
                                     streams without a full DataFrame.
XLSX / XLS     ``python-calamine``   Rust reader; the fastest Python Excel
                                     reader in published comparisons, and the
                                     engine pandas/polars select for xlsx.
HTML           ``selectolax``        lexbor-based DOM; an order of magnitude
                                     faster than BeautifulSoup+lxml.
RSS / Atom     ``feedparser``        The de-facto reference feed parser;
                                     handles the malformed feeds real news
                                     outlets actually emit.
PDF            ``pypdfium2``         PDFium bindings; fastest non-commercial
                                     text/raster extractor available.
=============  ============================================================

Every parser returns plain Python primitives. ``None`` is preserved as
``None``; no parser substitutes a placeholder for a missing value.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import UTC, datetime
from typing import Any

import orjson
import polars as pl

logger = logging.getLogger(__name__)

_WHITESPACE = re.compile(r"\s+")

#: Values that a source uses to mean "no value". Normalising these to ``None``
#: prevents the string ``"N/A"`` from being treated as data downstream.
NULL_TOKENS = frozenset(
    {
        "", "n/a", "na", "nan", "none", "null", "nil", "-", "--", "unknown",
        "not available", "not provided", "not applicable", "?",
    }
)


def clean_scalar(value: Any) -> Any:
    """Collapse whitespace and map source null tokens to ``None``.

    Never invents a replacement value — the result is either the cleaned
    source value or ``None``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        text = _WHITESPACE.sub(" ", value).strip()
        return None if text.lower() in NULL_TOKENS else text
    if isinstance(value, float) and value != value:  # NaN
        return None
    return value


def clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: clean_scalar(v) for k, v in row.items()}


def rows_to_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [clean_row(r) for r in rows]


# --------------------------------------------------------------------------
# JSON
# --------------------------------------------------------------------------

def parse_json(body: bytes) -> Any:
    """Decode JSON with orjson."""
    return orjson.loads(body)


def dumps(payload: Any) -> bytes:
    """Canonical, sorted-key JSON used for hashing and archiving."""
    return orjson.dumps(payload, option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS)


# --------------------------------------------------------------------------
# CSV / TSV
# --------------------------------------------------------------------------

def parse_csv(body: bytes, *, separator: str | None = None) -> list[dict[str, Any]]:
    """Parse CSV/TSV with polars.

    All columns are read as strings so that identifiers such as
    ``INCIDENT_NUM = "202600077490"`` are never coerced into a number and
    lose leading zeros or gain scientific notation.
    """
    if not body.strip():
        return []
    kwargs: dict[str, Any] = {"infer_schema_length": 0, "ignore_errors": True}
    if separator:
        kwargs["separator"] = separator
    frame = pl.read_csv(io.BytesIO(body), **kwargs)
    return [dict(zip(frame.columns, row)) for row in frame.iter_rows()]


def parse_ndjson(body: bytes) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in body.splitlines():
        if line.strip():
            out.append(orjson.loads(line))
    return out


# --------------------------------------------------------------------------
# Excel
# --------------------------------------------------------------------------

def parse_xlsx(body: bytes, *, sheet: int | str | None = None) -> list[dict[str, Any]]:
    """Parse a workbook with python-calamine (Rust)."""
    frame = pl.read_excel(io.BytesIO(body), engine="calamine", sheet_id=sheet or 0)
    return [dict(zip(frame.columns, row)) for row in frame.iter_rows()]


# --------------------------------------------------------------------------
# HTML
# --------------------------------------------------------------------------

def parse_html_links(body: bytes, base_url: str) -> list[dict[str, str]]:
    """Extract anchor href/text pairs with selectolax."""
    from urllib.parse import urljoin

    from selectolax.lexbor import LexborHTMLParser

    tree = LexborHTMLParser(body.decode("utf-8", errors="replace"))
    out: list[dict[str, str]] = []
    for node in tree.css("a[href]"):
        href = node.attributes.get("href") or ""
        if not href or href.startswith(("javascript:", "#", "mailto:")):
            continue
        out.append(
            {
                "url": urljoin(base_url, href),
                "text": _WHITESPACE.sub(" ", node.text(separator=" ")).strip(),
            }
        )
    return out


def parse_html_tables(body: bytes) -> list[list[dict[str, Any]]]:
    """Extract every ``<table>`` as a list of row dicts, via selectolax."""
    from selectolax.lexbor import LexborHTMLParser

    tree = LexborHTMLParser(body.decode("utf-8", errors="replace"))
    tables: list[list[dict[str, Any]]] = []
    for table in tree.css("table"):
        rows = table.css("tr")
        if len(rows) < 2:
            continue
        headers = [
            _WHITESPACE.sub(" ", cell.text(separator=" ")).strip()
            for cell in rows[0].css("th,td")
        ]
        parsed: list[dict[str, Any]] = []
        for row in rows[1:]:
            cells = [
                _WHITESPACE.sub(" ", cell.text(separator=" ")).strip()
                for cell in row.css("td")
            ]
            if not any(cells):
                continue
            parsed.append(
                {headers[i] if i < len(headers) else f"col_{i}": v for i, v in enumerate(cells)}
            )
        if parsed:
            tables.append(parsed)
    return tables


# --------------------------------------------------------------------------
# RSS / Atom
# --------------------------------------------------------------------------

def parse_feed(body: bytes) -> list[dict[str, Any]]:
    """Parse an RSS/Atom feed with feedparser."""
    import feedparser

    parsed = feedparser.parse(body)
    entries: list[dict[str, Any]] = []
    for entry in parsed.entries:
        published = entry.get("published_parsed") or entry.get("updated_parsed")
        published_at = (
            datetime(*published[:6], tzinfo=UTC).isoformat() if published else None
        )
        entries.append(
            {
                "title": clean_scalar(entry.get("title")),
                "link": clean_scalar(entry.get("link")),
                "published_at": published_at,
                "summary": clean_scalar(
                    entry.get("summary") or entry.get("description")
                ),
                "author": clean_scalar(entry.get("author")),
            }
        )
    return entries


# --------------------------------------------------------------------------
# PDF
# --------------------------------------------------------------------------

def parse_pdf_text(body: bytes) -> str:
    """Extract text from a PDF with PDFium."""
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(body)
    try:
        pages = []
        for page in pdf:
            textpage = page.get_textpage()
            try:
                pages.append(textpage.get_text_range())
            finally:
                textpage.close()
        return "\n".join(pages)
    finally:
        pdf.close()


def parse_any(body: bytes, *, content_type: str | None, url: str = "") -> list[dict[str, Any]]:
    """Dispatch on content type / URL suffix, returning a list of row dicts."""
    ctype = (content_type or "").split(";")[0].strip().lower()
    probe = url.lower()

    if ctype == "application/json" or probe.endswith(".json"):
        payload = parse_json(body)
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("records", "data", "results", "features", "dataset"):
                inner = payload.get(key)
                if isinstance(inner, list):
                    return [r for r in inner if isinstance(r, dict)]
            return [payload]
        return []

    if ctype == "application/x-ndjson" or probe.endswith(".ndjson"):
        return parse_ndjson(body)

    if "excel" in ctype or probe.endswith((".xlsx", ".xls")):
        return parse_xlsx(body)

    if probe.endswith(".pdf") or ctype == "application/pdf":
        return [{"text": parse_pdf_text(body)}]

    if "xml" in ctype or "rss" in ctype or probe.endswith((".xml", ".rss", ".atom")):
        return parse_feed(body)

    if "html" in ctype or probe.endswith((".html", ".htm")):
        tables = parse_html_tables(body)
        if tables:
            return tables[0]
        return [{"text": body.decode("utf-8", errors="replace")}]

    # Default: CSV. polars rejects non-tabular input rather than guessing.
    return parse_csv(body)
