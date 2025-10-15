#!/usr/bin/env python3
"""Start the contracts app server with the repository sources on sys.path."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import uvicorn


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _pythonpath_entries() -> list[str]:
    root = _repo_root()
    return [
        str(root / "src"),
        str(root / "packages" / "dc43-contracts-app" / "src"),
        str(root / "packages" / "dc43-demo-app" / "src"),
    ]


def _ensure_sys_path() -> None:
    for entry in reversed(_pythonpath_entries()):
        if entry not in sys.path:
            sys.path.insert(0, entry)


def _update_pythonpath_env() -> None:
    existing = os.environ.get("PYTHONPATH", "")
    entries = [value for value in existing.split(os.pathsep) if value]
    entries.extend(path for path in _pythonpath_entries() if path not in entries)
    os.environ["PYTHONPATH"] = os.pathsep.join(entries)


def main() -> None:
    _ensure_sys_path()
    _update_pythonpath_env()

    host = os.environ.get("SETUP_WIZARD_HOST", "127.0.0.1")
    port = int(os.environ.get("SETUP_WIZARD_PORT", "8002"))

    uvicorn.run("dc43_contracts_app.server:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
