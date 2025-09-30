from __future__ import annotations

"""Helpers to bootstrap the contracts app workspace with demo assets."""

import os
import shutil
from pathlib import Path
from typing import Tuple

from importlib import import_module

from open_data_contract_standard.model import OpenDataContractStandard

contracts_package = import_module("dc43_contracts_app")
contracts_server = import_module("dc43_contracts_app.server")

configure_workspace = getattr(contracts_package, "configure_workspace")
current_workspace = getattr(contracts_package, "current_workspace")

_CONFIGURE_BACKEND = getattr(contracts_package, "configure_backend", None)
if _CONFIGURE_BACKEND is None:  # pragma: no cover - legacy wheels
    _CONFIGURE_BACKEND = getattr(contracts_server, "configure_backend", None)
_INITIALISE_BACKEND = getattr(contracts_server, "_initialise_backend", None)

refresh_dataset_aliases = getattr(contracts_server, "refresh_dataset_aliases")
set_active_version = getattr(contracts_server, "set_active_version")

from dc43_contracts_app.workspace import ContractsAppWorkspace, workspace_from_env

from .scenarios import _DEFAULT_SLICE, _INVALID_SLICE

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_ROOT = BASE_DIR / "demo_data"


def _copy_tree(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    if destination.exists():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copytree(source, destination)


def _prepare_contracts(workspace: ContractsAppWorkspace) -> None:
    contracts_src = SAMPLE_ROOT / "contracts"
    if not contracts_src.exists():
        return

    for src in contracts_src.rglob("*.json"):
        model = OpenDataContractStandard.model_validate_json(src.read_text())
        for server in model.servers or []:
            path = Path(server.path or "")
            if not path.is_absolute():
                path = (workspace.root / path).resolve()
            base = path.parent if path.suffix else path
            base.mkdir(parents=True, exist_ok=True)
            server.path = str(path)
        dest = workspace.contracts_dir / src.relative_to(contracts_src)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            model.model_dump_json(indent=2, by_alias=True, exclude_none=True),
            encoding="utf-8",
        )


def prepare_demo_workspace() -> Tuple[ContractsAppWorkspace, bool]:
    """Ensure demo data is available and configure the contracts workspace."""

    workspace, created = workspace_from_env()
    os.environ.setdefault("DC43_DEMO_WORK_DIR", str(workspace.root))
    configure_workspace(workspace)

    data_src = SAMPLE_ROOT / "data"
    records_src = SAMPLE_ROOT / "records"

    _copy_tree(data_src, workspace.data_dir)
    _copy_tree(records_src, workspace.records_dir)
    _prepare_contracts(workspace)

    refresh_dataset_aliases()
    for dataset, version in {**_DEFAULT_SLICE, **_INVALID_SLICE}.items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue

    backend_url = (
        os.getenv("DC43_CONTRACTS_APP_BACKEND_URL")
        or os.getenv("DC43_DEMO_BACKEND_URL")
        or None
    )
    if callable(_CONFIGURE_BACKEND):
        if backend_url:
            _CONFIGURE_BACKEND(base_url=backend_url)
        else:
            _CONFIGURE_BACKEND()
    elif callable(_INITIALISE_BACKEND):  # pragma: no cover - compatibility
        _INITIALISE_BACKEND(base_url=backend_url)
    elif backend_url:  # pragma: no cover - final fallback
        os.environ.setdefault("DC43_CONTRACTS_APP_BACKEND_URL", backend_url)

    return workspace, created


__all__ = ["prepare_demo_workspace", "current_workspace"]
