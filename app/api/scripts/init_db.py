"""Create or migrate the lattice schema. Safe to run repeatedly."""

from __future__ import annotations

import logging

from app.db import ensure_schema_current

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if __name__ == "__main__":
    ensure_schema_current()
