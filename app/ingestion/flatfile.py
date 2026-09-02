from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from app.ingestion.base import BaseAdapter, RawRecordDTO


class FlatFileAdapter(BaseAdapter):
    """Ingests flat-file drops: CSV, Excel, JSON, NDJSON.

    Physical constraint: files must be placed in the configured manual drop
    directory. The adapter does not attempt to download them.
    """

    name = "flatfile"
    access_mode = "file_drop"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        drop_dir_env = config.get("drop_dir_env", "MANUAL_DROP_DIR")
        drop_dir = getattr(self.settings, drop_dir_env.lower(), None) or os.getenv(drop_dir_env)
        if not drop_dir:
            self.log_skip("No drop_dir_env configured")
            return []

        source_id = config.get("id", "")
        pattern = config.get("filename_pattern") or (f"*{source_id}*" if source_id else "*")
        path = Path(drop_dir)
        if not path.exists():
            self.log_skip(f"Drop directory does not exist: {drop_dir}")
            return []

        records: list[RawRecordDTO] = []
        for file_path in path.glob(pattern):
            records.extend(self._parse_file(file_path))
        return records

    def _parse_file(self, file_path: Path) -> list[RawRecordDTO]:
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(file_path)
        elif suffix in (".xlsx", ".xls"):
            df = pd.read_excel(file_path)
        elif suffix == ".json":
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [
                    RawRecordDTO(
                        content_type="application/json",
                        payload={"row": item},
                        source_id=self.source_config.get("id"),
                        file_path=str(file_path),
                        metadata={"adapter": self.name, "file": file_path.name},
                    )
                    for item in data
                ]
            else:
                return [
                    RawRecordDTO(
                        content_type="application/json",
                        payload={"row": data},
                        source_id=self.source_config.get("id"),
                        file_path=str(file_path),
                        metadata={"adapter": self.name, "file": file_path.name},
                    )
                ]
        elif suffix == ".ndjson":
            records = []
            with file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        records.append(
                            RawRecordDTO(
                                content_type="application/x-ndjson",
                                payload={"row": json.loads(line)},
                                source_id=self.source_config.get("id"),
                                file_path=str(file_path),
                                metadata={"adapter": self.name, "file": file_path.name},
                            )
                        )
            return records
        else:
            self.log_skip(f"Unsupported file type: {file_path.name}")
            return []

        records = []
        for _, row in df.iterrows():
            records.append(
                RawRecordDTO(
                    content_type="application/json",
                    payload={"row": row.to_dict()},
                    source_id=self.source_config.get("id"),
                    file_path=str(file_path),
                    metadata={"adapter": self.name, "file": file_path.name},
                )
            )
        return records
