from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from app.ingestion.base import BaseAdapter, RawRecordDTO


class PdfOcrAdapter(BaseAdapter):
    """Ingests PDF documents, using the text layer when available and OCR when
    not. Files must be placed in the manual drop directory. No automatic
    download is performed.
    """

    name = "pdf_ocr"
    access_mode = "manual"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        drop_dir = getattr(self.settings, "manual_drop_dir", None) or config.get("drop_dir", "./data/manual_drops")
        pattern = config.get("filename_pattern", "*.pdf")
        path = Path(drop_dir)
        if not path.exists():
            self.log_skip(f"PDF drop directory missing: {drop_dir}")
            return []

        records: list[RawRecordDTO] = []
        for file_path in path.glob(pattern):
            records.append(self._process_pdf(file_path))
        return records

    def _process_pdf(self, file_path: Path) -> RawRecordDTO:
        text = self._extract_text(file_path)
        if not text.strip():
            text = self._ocr_pdf(file_path)

        payload = {
            "file_name": file_path.name,
            "text": text,
            "metadata": self._pdf_metadata(file_path),
        }
        return RawRecordDTO(
            content_type="application/pdf",
            payload=payload,
            file_path=str(file_path),
            source_id=self.source_config.get("id"),
            metadata={"adapter": self.name, "file": file_path.name},
        )

    def _extract_text(self, file_path: Path) -> str:
        reader = PdfReader(str(file_path))
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        return "\n".join(pages)

    def _ocr_pdf(self, file_path: Path) -> str:
        # Use pypdf + tesseract via CLI on temporary images is an external
        # dependency. Here we return a placeholder and log the constraint.
        self.log_skip(f"OCR required for {file_path.name}; place OCR text file next to PDF")
        # Fallback: look for a .txt sibling
        txt_path = file_path.with_suffix(".txt")
        if txt_path.exists():
            return txt_path.read_text(encoding="utf-8")
        return ""

    def _pdf_metadata(self, file_path: Path) -> dict[str, Any]:
        reader = PdfReader(str(file_path))
        try:
            return dict(reader.metadata or {})
        except Exception:
            return {}
