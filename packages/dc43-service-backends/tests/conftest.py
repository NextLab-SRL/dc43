"""Test utilities for the dc43-service-backends package."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


pytest.importorskip("tomlkit")


def _ensure_local_src_on_path() -> None:
    """Add first-party ``src`` directories to ``sys.path`` for tests."""

    here = Path(__file__).resolve()
    project_root = here.parents[3]

    candidate_dirs = [here.parents[1] / "src", project_root / "src"]

    packages_root = project_root / "packages"
    if packages_root.exists():
        candidate_dirs.extend(
            src_dir for src_dir in packages_root.glob("*/src") if src_dir.is_dir()
        )

    for src_dir in candidate_dirs:
        src_str = str(src_dir)
        if src_dir.exists() and src_str not in sys.path:
            sys.path.insert(0, src_str)


_ensure_local_src_on_path()
