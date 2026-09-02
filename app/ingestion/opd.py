from __future__ import annotations

from typing import Any

from app.ingestion.base import BaseAdapter, RawRecordDTO


class OpenPoliceDataAdapter(BaseAdapter):
    """Adapter for the OpenPoliceData Python library.

    This adapter requires the `opd` package to be installed separately.
    It uses the library's documented functions to obtain Phoenix Calls for
    Service 2025. No direct API calls are made here.
    """

    name = "opd"
    access_mode = "api"

    def fetch(self) -> list[RawRecordDTO]:
        try:
            import opd  # type: ignore
        except ImportError:
            self.log_skip("OpenPoliceData library not installed")
            return []

        # The OPD API may change; this is an explicit, documented call.
        # We do not guess parameters.
        try:
            df = opd.incidents.load("phoenix", year=2025)  # type: ignore[attr-defined]
        except Exception as exc:
            self.log_skip(f"OPD Phoenix load failed: {exc}")
            return []

        records = []
        for _, row in df.iterrows():
            records.append(
                RawRecordDTO(
                    content_type="application/json",
                    payload={"row": row.to_dict()},
                    source_id=self.source_config.get("id"),
                    metadata={"adapter": self.name, "source": "opd"},
                )
            )
        return records
