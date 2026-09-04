"""Live PDF ingestion with best-in-class text extraction.

- Text-layer extraction: ``pypdfium2`` (Google PDFium bindings — the fastest
  permissively-licensed PDF text extractor; benchmark-proven vs. pypdf and
  pdfminer.six).
- Scanned pages: ``pytesseract`` OCR (Tesseract is the benchmark-reference
  open-source OCR engine) when a text layer is absent.

PDFs are downloaded from configured live URLs. No manual file drops.
"""

from __future__ import annotations

import io
import logging
import os

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class PdfOcrAdapter(BaseAdapter):
    """Downloads live PDF documents and extracts their full text."""

    name = "pdf_ocr"
    access_mode = "api"

    def _resolve_urls(self) -> list[str]:
        config = self.source_config
        urls: list[str] = []
        url_env = config.get("url_env")
        if url_env:
            val = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
            if val:
                urls.extend([val] if isinstance(val, str) else list(val))
        urls.extend(config.get("urls", []) or [])
        if config.get("url"):
            urls.append(config["url"])
        if not urls:
            self.log_skip("No live PDF URL configured")
        return urls

    def fetch(self) -> list[RawRecordDTO]:
        client = get_fetch_client()
        records: list[RawRecordDTO] = []
        for url in self._resolve_urls():
            try:
                body = client.get_bytes(url)
            except LiveSourceError as exc:
                self.log_skip(f"PDF download failed for {url}: {exc}")
                continue
            text, method, page_count = self._extract_text(body)
            records.append(
                RawRecordDTO(
                    content_type="application/pdf",
                    payload={
                        "url": url,
                        "file_name": url.rsplit("/", 1)[-1] or "document.pdf",
                        "text": text,
                        "extraction_method": method,
                        "page_count": page_count,
                    },
                    source_id=self.source_config.get("id"),
                    metadata={
                        "adapter": self.name,
                        "url": url,
                        "live_url": url,
                        "extraction_method": method,
                        "page_count": page_count,
                    },
                )
            )
        return records

    def _extract_text(self, body: bytes) -> tuple[str, str, int]:
        """Extract full text from PDF bytes.

        Uses the PDFium text layer first; OCRs pages that have no text layer.
        Returns (text, method, page_count) where method records exactly how
        the text was obtained ('pdfium-text-layer', 'tesseract-ocr', or
        'mixed:<n>-ocr/<total>').
        """
        import pypdfium2 as pdfium

        pdf = pdfium.PdfDocument(io.BytesIO(body))
        n_pages = len(pdf)
        page_texts: list[str] = []
        ocr_pages = 0
        for i in range(n_pages):
            page = pdf[i]
            textpage = page.get_textpage()
            text = textpage.get_text_bounded() or ""
            if not text.strip():
                ocr_text = self._ocr_page(page)
                if ocr_text:
                    ocr_pages += 1
                    text = ocr_text
            page_texts.append(text)
        pdf.close()

        if ocr_pages == 0:
            method = "pdfium-text-layer"
        elif ocr_pages == n_pages:
            method = "tesseract-ocr"
        else:
            method = f"mixed:{ocr_pages}-ocr/{n_pages}"
        return "\n".join(page_texts), method, n_pages

    def _ocr_page(self, page) -> str:
        """OCR a single PDFium page with Tesseract (benchmark-reference OCR)."""
        try:
            import pytesseract  # noqa: PLC0415 - optional at import time
            from PIL import Image  # noqa: F401, PLC0415 - availability check: OCR needs Pillow
        except ImportError:
            self.log_skip("pytesseract/Pillow not installed; page has no text layer")
            return ""
        try:
            settings = self.settings
            if settings.tesseract_cmd:
                pytesseract.pytesseract.tesseract_cmd = settings.tesseract_cmd
            bitmap = page.render(scale=2.0)
            pil_image = bitmap.to_pil()
            return pytesseract.image_to_string(pil_image)
        except Exception as exc:  # OCR failure must not crash the pipeline
            self.log_skip(f"OCR failed for one page: {exc}")
            return ""
