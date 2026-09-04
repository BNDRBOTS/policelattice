from __future__ import annotations

import json

from app.ingestion.arcgis import ArcGISAdapter
from app.ingestion.base import AdapterRegistry, RawRecordDTO
from app.ingestion.courtlistener import CourtListenerAdapter
from app.ingestion.flatfile import FlatFileAdapter
from app.ingestion.muckrock import MuckRockAdapter
from app.ingestion.news_rss import NewsRssAdapter
from app.ingestion.pdf_ocr import PdfOcrAdapter
from app.ingestion.socrata import SocrataAdapter


def test_raw_record_dto_checksum():
    dto1 = RawRecordDTO(content_type="application/json", payload={"a": 1, "b": 2})
    dto2 = RawRecordDTO(content_type="application/json", payload={"b": 2, "a": 1})
    assert dto1.compute_checksum() == dto2.compute_checksum()


def test_adapter_registry_contains_all_live_adapters():
    for name in (
        "arcgis", "courtlistener", "flatfile", "generic_rest",
        "muckrock", "news_rss", "pdf_ocr", "socrata", "web_scraper",
    ):
        assert AdapterRegistry.get(name) is not None


def test_adapters_yield_nothing_when_unconfigured():
    """Unconfigured adapters must skip explicitly, never fabricate records."""
    assert ArcGISAdapter({"id": "a1", "config": {}}).fetch() == []
    assert SocrataAdapter({"id": "s1", "config": {}}).fetch() == []
    assert FlatFileAdapter({"id": "f1", "config": {}}).fetch() == []
    assert NewsRssAdapter({"id": "n1", "config": {}}).fetch() == []
    assert PdfOcrAdapter({"id": "p1", "config": {}}).fetch() == []


def test_courtlistener_adapter_no_token():
    adapter = CourtListenerAdapter({"id": "cl1"})
    adapter.settings.courtlistener_token = None
    assert adapter.fetch() == []


def test_muckrock_adapter_no_token():
    adapter = MuckRockAdapter({"id": "mr1"})
    adapter.settings.muckrock_token = None
    assert adapter.fetch() == []


def test_flatfile_parses_live_csv_bytes():
    """CSV parsing path (pandas, dtype=str to preserve identifier fidelity)."""
    adapter = FlatFileAdapter({"id": "ff1", "config": {}})
    csv_body = (
        b"badge,last,first,agency\n"
        b"0123,Smith,John,Phoenix Police Department\n"
        b"0456,Jones,Sara,Tempe Police Department\n"
    )
    records = adapter._parse_body(csv_body, "https://example.test/x.csv", "x.csv")
    assert len(records) == 2
    assert records[0].payload["row"]["badge"] == "0123"  # leading zero preserved


def test_flatfile_parses_live_json_envelope():
    adapter = FlatFileAdapter({"id": "ff2", "config": {}})
    body = json.dumps({"data": [{"id": 1}, {"id": 2}]}).encode()
    records = adapter._parse_body(body, "https://example.test/x.json", "x.json")
    assert len(records) == 2
    assert records[0].payload == {"row": {"id": 1}}


def test_socrata_discovery_parses_catalog_response(monkeypatch):
    adapter = SocrataAdapter({"id": "s2", "config": {}})

    class FakeClient:
        def get_bytes(self, url, headers=None):
            assert "api.us.socrata.com/api/catalog/v1" in url
            return json.dumps(
                {
                    "results": [
                        {
                            "resource": {
                                "id": "abcd-1234",
                                "name": "Police Incidents",
                                "description": "d",
                                "attribution": "Tempe",
                                "rows_updated_at": 1,
                            },
                            "metadata": {"domain": "data.tempe.gov"},
                        }
                    ]
                }
            ).encode()

    monkeypatch.setattr("app.ingestion.socrata.get_fetch_client", lambda: FakeClient())
    found = adapter.discover_datasets("data.tempe.gov", "police")
    assert found == [
        {
            "id": "abcd-1234",
            "name": "Police Incidents",
            "description": "d",
            "attribution": "Tempe",
            "rows_updated_at": 1,
            "domain": "data.tempe.gov",
        }
    ]
