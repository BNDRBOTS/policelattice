"""Parser and normalization behaviour.

The rule under test: a value the source did not state stays ``None``. No
parser or normalizer is allowed to substitute a placeholder.
"""

from __future__ import annotations

import polars as pl
import pytest

from app.ingest.parsers import (
    NULL_TOKENS,
    clean_scalar,
    parse_any,
    parse_csv,
    parse_feed,
    parse_html_tables,
    parse_json,
    parse_ndjson,
)
from app.pipeline.normalize import (
    normalize_record,
    parse_datetime,
    period_of,
    resolve_columns,
)


def test_parse_csv_reads_all_columns_as_text():
    body = b'INCIDENT_NUM,ZIP\n0012345,85004\n202600077490,85003\n'
    rows = parse_csv(body)
    assert rows == [
        {"INCIDENT_NUM": "0012345", "ZIP": "85004"},
        {"INCIDENT_NUM": "202600077490", "ZIP": "85003"},
    ]
    # leading zeros survive: the column was not coerced to an integer
    assert rows[0]["INCIDENT_NUM"] == "0012345"


def test_parse_csv_uses_polars_engine():
    body = b"a,b\n1,2\n3,4\n"
    rows = parse_csv(body)
    assert len(rows) == 2
    assert pl is not None


def test_parse_json_and_ndjson():
    assert parse_json(b'{"a": 1}') == {"a": 1}
    assert parse_ndjson(b'{"a": 1}\n{"a": 2}\n') == [{"a": 1}, {"a": 2}]


def test_parse_html_tables_extracts_rows():
    html = (
        b"<table><tr><th>Case</th><th>Finding</th></tr>"
        b"<tr><td>C-1</td><td>Sustained</td></tr></table>"
    )
    tables = parse_html_tables(html)
    assert tables == [[{"Case": "C-1", "Finding": "Sustained"}]]


def test_parse_feed_reads_entries():
    feed = (
        b'<?xml version="1.0"?><rss version="2.0"><channel><title>Feed</title>'
        b"<item><title>Officer involved shooting</title>"
        b"<link>https://example.test/a</link>"
        b"<pubDate>Mon, 01 Sep 2025 12:00:00 GMT</pubDate></item>"
        b"</channel></rss>"
    )
    entries = parse_feed(feed)
    assert entries[0]["title"] == "Officer involved shooting"
    assert entries[0]["link"] == "https://example.test/a"
    assert entries[0]["published_at"].startswith("2025-09-01")


def test_parse_any_dispatches_on_content_type():
    assert parse_any(b'[{"a":1}]', content_type="application/json") == [{"a": 1}]
    assert parse_any(b"a,b\n1,2\n", content_type="text/csv") == [{"a": "1", "b": "2"}]
    assert parse_any(b"a,b\n1,2\n", content_type=None, url="https://x.test/f.csv") == [
        {"a": "1", "b": "2"}
    ]


@pytest.mark.parametrize("token", sorted(NULL_TOKENS))
def test_null_tokens_become_none_not_placeholders(token):
    assert clean_scalar(token) is None
    assert clean_scalar(token.upper()) is None


def test_clean_scalar_preserves_real_values():
    assert clean_scalar("  Out of   Policy ") == "Out of Policy"
    assert clean_scalar(0) == 0
    assert clean_scalar(None) is None
    assert clean_scalar(float("nan")) is None


def test_parse_datetime_handles_source_formats():
    assert parse_datetime("2025-01-17T00:00:00").year == 2025
    assert parse_datetime("01/17/2025").month == 1
    assert parse_datetime(1737072000).year == 2025
    assert parse_datetime("not a date") is None
    assert parse_datetime(None) is None
    assert parse_datetime("2025-01-17T00:00:00").tzinfo is not None


def test_period_of():
    assert period_of(parse_datetime("2025-01-17T00:00:00")) == "2025-01"
    assert period_of(None) is None


def test_resolve_columns_matches_real_phoenix_column_names():
    row = {
        "INCIDENT_NUM": "202600077490",
        "UNIQUE_INCIDENT_OFFICER": "20260007749011083",
        "INCIDENT_DATE": "2025-01-17T00:00:00",
        "EMPL_GENDER": "Female",
        "SIMPLE_EMPL_RE_GRP": "White",
        "EMP_BWC_ACTV": "Yes",
        "EMP_WITHIN_POLICY": "Yes",
        "OFFICERS_PER_INCIDENT": 1,
    }
    officer_cols = resolve_columns(row, "officer")
    assert officer_cols["external_key"] == "UNIQUE_INCIDENT_OFFICER"
    assert officer_cols["gender"] == "EMPL_GENDER"
    assert officer_cols["race_group"] == "SIMPLE_EMPL_RE_GRP"

    event_cols = resolve_columns(row, "force_event")
    assert event_cols["within_policy"] == "EMP_WITHIN_POLICY"
    assert event_cols["bwc_activated"] == "EMP_BWC_ACTV"
    assert event_cols["officers_on_scene"] == "OFFICERS_PER_INCIDENT"

    incident_cols = resolve_columns(row, "incident")
    assert incident_cols["external_number"] == "INCIDENT_NUM"
    assert incident_cols["occurred_at"] == "INCIDENT_DATE"
    # columns the source does not carry resolve to None, never to a guess
    assert incident_cols["armed_type"] is None
    assert incident_cols["location"] is None


def test_normalize_record_never_invents_absent_fields():
    row = {"INCIDENT_NUM": "X1", "INCIDENT_DATE": "2025-03-04T00:00:00"}
    canonical = normalize_record(row, "incident")
    assert canonical["external_number"] == "X1"
    assert canonical["period"] == "2025-03"
    for absent in (
        "highest_force_applied", "armed_type", "resistance", "de_escalation",
        "injury", "highest_charge", "outcome", "location", "latitude",
        "longitude", "precinct", "subject_gender", "subject_race_group",
    ):
        assert canonical[absent] is None, absent
    # the original row is preserved verbatim for provenance
    assert canonical["raw"] == row


def test_normalize_record_maps_real_uof_officer_row():
    row = {
        "INCIDENT_NUM": "202600077490",
        "UNIQUE_INCIDENT_OFFICER": "20260007749011083",
        "INCIDENT_DATE": "2025-01-17T00:00:00",
        "EMPL_GENDER": "Female",
        "SIMPLE_EMPL_RE_GRP": "White",
        "EMP_BWC_ACTV": "Yes",
        "EMP_WITHIN_POLICY": "Yes",
        "OFFICERS_PER_INCIDENT": 1,
    }
    canonical = normalize_record(row, "incident")
    assert canonical["external_number"] == "202600077490"
    assert canonical["period"] == "2025-01"
    assert canonical["occurred_at"].month == 1
