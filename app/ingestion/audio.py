from __future__ import annotations

from pathlib import Path

from app.ingestion.base import BaseAdapter, RawRecordDTO


class AudioAdapter(BaseAdapter):
    """Ingests audio feed metadata and optional audio files.

    Live scanner feeds are not recorded by default. If an audio file is placed
    in the manual drop directory, its metadata is captured. Transcription is
    intentionally not performed unless a speech-to-text worker is later added.
    """

    name = "audio"
    access_mode = "manual"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        drop_dir = (
            getattr(self.settings, "manual_drop_dir", None)
            or config.get("drop_dir", "./data/manual_drops")
        )
        path = Path(drop_dir)
        if not path.exists():
            self.log_skip(f"Manual drop directory missing: {drop_dir}")
            return []

        records = []
        for file_path in path.glob("*.mp3"):
            records.append(
                RawRecordDTO(
                    content_type="audio/mpeg",
                    payload={
                        "file_name": file_path.name,
                        "size_bytes": file_path.stat().st_size,
                        "duration_seconds": None,
                    },
                    file_path=str(file_path),
                    source_id=self.source_config.get("id"),
                    metadata={"adapter": self.name},
                )
            )
        return records
