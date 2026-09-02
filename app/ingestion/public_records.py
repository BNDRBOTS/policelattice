from __future__ import annotations

from pathlib import Path
from app.ingestion.base import BaseAdapter, RawRecordDTO


class PublicRecordsAdapter(BaseAdapter):
    """Tracks public records requests and manual portal responses.

    This adapter does not automate portals like GovQA, OAT, or department
    records systems. It monitors a manual drop directory for exported files and
    creates raw records from them. It also can ingest CSV/JSON exports from
    public records portals if the user places them in the drop directory.
    """

    name = "public_records"
    access_mode = "manual"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        drop_dir = getattr(self.settings, "manual_drop_dir", None) or config.get("drop_dir", "./data/manual_drops")
        pattern = config.get("filename_pattern", "*")
        path = Path(drop_dir)
        if not path.exists():
            self.log_skip(f"Manual drop directory missing: {drop_dir}")
            return []

        records: list[RawRecordDTO] = []
        for file_path in path.glob(pattern):
            if file_path.suffix.lower() in (".csv", ".json", ".xlsx", ".txt", ".pdf"):
                # Reuse flatfile / pdf via simple heuristic
                if file_path.suffix.lower() == ".pdf":
                    from app.ingestion.pdf_ocr import PdfOcrAdapter
                    adapter = PdfOcrAdapter({**config, "filename_pattern": file_path.name})
                    records.extend(adapter.fetch())
                else:
                    from app.ingestion.flatfile import FlatFileAdapter
                    adapter = FlatFileAdapter({**config, "filename_pattern": file_path.name})
                    records.extend(adapter.fetch())
        return records
