"""Test utilities for the dc43-service-clients package."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_local_src_on_path() -> None:
    """Add the package's ``src`` directory to ``sys.path`` if needed."""

    src_dir = Path(__file__).resolve().parents[1] / "src"
    src_str = str(src_dir)
    if src_dir.exists() and src_str not in sys.path:
        sys.path.insert(0, src_str)


_ensure_local_src_on_path()
