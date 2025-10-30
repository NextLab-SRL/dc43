"""Dataset record store implementations used by the contracts services."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Protocol, Sequence

logger = logging.getLogger(__name__)


def _coerce_mapping(entry: object) -> MutableMapping[str, object] | None:
    """Best-effort conversion of ``entry`` into a mutable mapping."""

    if isinstance(entry, MutableMapping):
        return dict(entry)
    if hasattr(entry, "__dict__") and not isinstance(entry, Mapping):
        return dict(vars(entry))
    return None


class DatasetRecordStore(Protocol):
    """Minimal persistence interface for dataset history records."""

    def load_records(self) -> list[MutableMapping[str, object]]:
        """Return previously persisted dataset record payloads."""

    def save_records(self, records: Iterable[Mapping[str, object]]) -> None:
        """Persist ``records`` for later retrieval."""


class InMemoryDatasetRecordStore:
    """Volatile dataset record storage used as the default implementation."""

    __slots__ = ("_records",)

    def __init__(self) -> None:
        self._records: list[MutableMapping[str, object]] = []

    def load_records(self) -> list[MutableMapping[str, object]]:
        return [dict(record) for record in self._records]

    def save_records(self, records: Iterable[Mapping[str, object]]) -> None:
        serialised: list[MutableMapping[str, object]] = []
        for entry in records:
            mapping = _coerce_mapping(entry) if not isinstance(entry, Mapping) else dict(entry)
            if mapping is None:
                continue
            serialised.append(mapping)
        self._records = serialised


class FilesystemDatasetRecordStore:
    """Persist dataset records to a JSON file on disk."""

    __slots__ = ("_path",)

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).expanduser()

    def load_records(self) -> list[MutableMapping[str, object]]:
        try:
            payload = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except OSError as exc:  # pragma: no cover - defensive logging around unreadable files
            logger.warning("Unable to read dataset record file %s (%s)", self._path, exc)
            return []

        try:
            raw = json.loads(payload) or []
        except json.JSONDecodeError:
            logger.warning("Dataset record file %s contained invalid JSON", self._path)
            return []

        if not isinstance(raw, Sequence):
            return []

        records: list[MutableMapping[str, object]] = []
        for entry in raw:
            if isinstance(entry, Mapping):
                records.append(dict(entry))
        return records

    def save_records(self, records: Iterable[Mapping[str, object]]) -> None:
        serialised: list[MutableMapping[str, object]] = []
        for entry in records:
            mapping = _coerce_mapping(entry) if not isinstance(entry, Mapping) else dict(entry)
            if mapping is None:
                continue
            serialised.append(mapping)

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(serialised, indent=2, sort_keys=True),
            encoding="utf-8",
        )


__all__ = [
    "DatasetRecordStore",
    "FilesystemDatasetRecordStore",
    "InMemoryDatasetRecordStore",
]
