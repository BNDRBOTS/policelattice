from __future__ import annotations

import os
from unittest.mock import patch

from app.ingestion.arcgis import ArcGISAdapter
from app.ingestion.base import AdapterRegistry, RawRecordDTO
from app.ingestion.courtlistener import CourtListenerAdapter
from app.ingestion.generic_rest import GenericRestAdapter
from app.ingestion.muckrock import MuckRockAdapter


def test_raw_record_dto_checksum():
    dto1 = RawRecordDTO(content_type="application/json", payload={"a": 1, "b": 2})
    dto2 = RawRecordDTO(content_type="application/json", payload={"b": 2, "a": 1})
    assert dto1.compute_checksum() == dto2.compute_checksum()


def test_adapter_registry():
    cls = AdapterRegistry.get("arcgis")
    assert cls == ArcGISAdapter


def test_arcgis_adapter_missing_env():
    adapter = ArcGISAdapter({"id": "arc1", "config": {}})
    records = adapter.fetch()
    assert records == []


def test_generic_rest_adapter_mock():
    adapter = GenericRestAdapter({"id": "gr1", "url_env": "TEST_URL"})
    with patch.object(adapter, "_get", return_value=[{"id": 1}, {"id": 2}]):
        with patch.dict(os.environ, {"TEST_URL": "https://api.example.com/data"}):
            records = adapter.fetch()
            assert len(records) == 2
            assert records[0].payload == {"row": {"id": 1}}


def test_courtlistener_adapter_no_token():
    adapter = CourtListenerAdapter({"id": "cl1"})
    adapter.settings.courtlistener_token = None
    assert adapter.fetch() == []


def test_muckrock_adapter_no_token():
    adapter = MuckRockAdapter({"id": "mr1"})
    adapter.settings.muckrock_token = None
    assert adapter.fetch() == []
