from __future__ import annotations

"""Helpers to bootstrap the contracts app workspace with demo assets."""

import os
import shutil
from pathlib import Path
from typing import Tuple

from open_data_contract_standard.model import OpenDataContractStandard

from dc43_contracts_app import (
    configure_backend,
    configure_workspace,
    current_workspace,
)
from dc43_contracts_app.workspace import ContractsAppWorkspace, workspace_from_env
from dc43_contracts_app.server import refresh_dataset_aliases, set_active_version

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

    configure_backend()

    return workspace, created


__all__ = ["prepare_demo_workspace", "current_workspace"]
