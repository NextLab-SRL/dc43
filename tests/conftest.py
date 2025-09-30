"""Pytest configuration for the root test suite."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the in-repo package sources are importable without requiring a full
# editable installation of every wheel that dc43 depends on.  This mirrors
# what ``pip install -e .`` does, but keeps ``pytest`` usable in lightweight
# environments (for example, GitHub Actions matrix jobs that build packages in
# parallel).
ROOT = Path(__file__).resolve().parent.parent
PACKAGE_SRC_DIRS = [
    ROOT / "src",
    ROOT / "packages" / "dc43-service-clients" / "src",
    ROOT / "packages" / "dc43-service-backends" / "src",
    ROOT / "packages" / "dc43-integrations" / "src",
    ROOT / "packages" / "dc43-contracts-app" / "src",
]

for src_dir in PACKAGE_SRC_DIRS:
    if src_dir.exists():
        str_path = str(src_dir)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)
