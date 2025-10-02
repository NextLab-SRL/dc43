"""dc43 — modular data-contract services and integrations."""

__all__: list[str] = []
from importlib import metadata
from pathlib import Path

try:
    __version__ = metadata.version("dc43")
except metadata.PackageNotFoundError:  # pragma: no cover - fallback for local source checkouts
    _ROOT = Path(__file__).resolve().parents[2]
    __version__ = (_ROOT / "VERSION").read_text("utf-8").strip()
    del _ROOT
