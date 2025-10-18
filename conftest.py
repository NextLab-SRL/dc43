"""Pytest configuration shared across in-repo test suites."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_local_package(module_name: str, local_root: Path) -> None:
    """Force ``module_name`` to load from ``local_root`` during tests."""

    module = sys.modules.get(module_name)
    if module is not None:
        module_file = getattr(module, "__file__", None)
        if module_file is not None:
            try:
                module_path = Path(module_file).resolve()
                if module_path.is_relative_to(local_root):
                    return
            except (OSError, RuntimeError):
                pass

        # Remove any previously imported module (and submodules) so that Python
        # re-imports it from the in-repo source path we add below.
        for loaded in list(sys.modules):
            if loaded == module_name or loaded.startswith(f"{module_name}."):
                sys.modules.pop(loaded, None)

    # Import once more to surface a clear error if the local package is missing
    # or shadowed by an installed distribution.
    module = __import__(module_name)
    module_file = getattr(module, "__file__", None)
    if module_file is None or not Path(module_file).resolve().is_relative_to(local_root):
        raise ModuleNotFoundError(
            f"Local package for {module_name!r} not found at {local_root}",
        )

# Ensure the in-repo package sources are importable without requiring a full
# editable installation of every wheel that dc43 depends on.  This mirrors
# what ``pip install -e .`` does, but keeps ``pytest`` usable in lightweight
# environments (for example, GitHub Actions matrix jobs that build packages in
# parallel).
ROOT = Path(__file__).resolve().parent
LOCAL_CLIENTS_ROOT = ROOT / "packages" / "dc43-service-clients" / "src" / "dc43_service_clients"
PACKAGE_SRC_DIRS = [
    ROOT / "src",
    ROOT / "packages" / "dc43-service-clients" / "src",
    ROOT / "packages" / "dc43-service-backends" / "src",
    ROOT / "packages" / "dc43-integrations" / "src",
    ROOT / "packages" / "dc43-contracts-app" / "src",
    ROOT / "packages" / "dc43-demo-app" / "src",
]

for src_dir in PACKAGE_SRC_DIRS:
    if src_dir.exists():
        str_path = str(src_dir)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)

if LOCAL_CLIENTS_ROOT.exists():
    _ensure_local_package("dc43_service_clients", LOCAL_CLIENTS_ROOT)
