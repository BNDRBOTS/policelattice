from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache
def get_dashboard_html() -> str:
    """Load and cache the web dashboard HTML."""
    template_path = Path(__file__).parent.parent / "templates" / "index.html"
    return template_path.read_text(encoding="utf-8")
