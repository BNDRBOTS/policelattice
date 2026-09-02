from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.config import get_settings


@dataclass
class RawRecordDTO:
    """Normalized raw record returned by an adapter."""

    content_type: str
    payload: dict[str, Any] | list[Any]
    file_path: str | None = None
    checksum: str | None = None
    source_id: str | None = None
    batch_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def compute_checksum(self) -> str:
        if self.checksum:
            return self.checksum
        serialized = json.dumps(self.payload, sort_keys=True, default=str)
        self.checksum = hashlib.sha256(serialized.encode()).hexdigest()
        return self.checksum


class BaseAdapter:
    """Base class for every ingestion adapter.

    Adapters must implement `fetch` and return an iterable of `RawRecordDTO`.
    They must not fabricate access. If a source is manual, `fetch` returns an
    empty list and logs that manual ingestion is required.
    """

    name: str = "base"
    access_mode: str = "manual"

    def __init__(self, source_config: dict[str, Any] | None = None):
        self.source_config = source_config or {}
        self.settings = get_settings()

    def fetch(self) -> list[RawRecordDTO]:
        """Return raw records for this source.

        Subclasses must implement this method. Manual adapters should return
        an empty list, not raise.
        """
        raise NotImplementedError

    def log_skip(self, reason: str) -> None:
        """Log skip without traceback."""
        print(f"[{self.name}] SKIP: {reason}")


class AdapterRegistry:
    """Maps adapter names to classes."""

    _adapters: dict[str, type[BaseAdapter]] = {}

    @classmethod
    def register(cls, name: str):
        def wrapper(adapter_cls: type[BaseAdapter]):
            cls._adapters[name] = adapter_cls
            return adapter_cls
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseAdapter]:
        try:
            return cls._adapters[name]
        except KeyError as exc:
            raise ValueError(f"Unknown adapter: {name}") from exc
