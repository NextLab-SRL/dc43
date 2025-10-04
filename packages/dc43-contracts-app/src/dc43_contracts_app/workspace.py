from __future__ import annotations

"""Workspace utilities for the contracts application."""

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple
import os
import tempfile


@dataclass(slots=True)
class ContractsAppWorkspace:
    """Filesystem layout backing the contracts and datasets UI."""

    root: Path
    contracts_dir: Path
    data_dir: Path
    records_dir: Path
    datasets_file: Path
    dq_status_dir: Path
    data_products_file: Path

    def ensure(self) -> None:
        """Create all directories and default files required by the UI."""

        self.contracts_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.dq_status_dir.mkdir(parents=True, exist_ok=True)
        if not self.datasets_file.exists():
            self.datasets_file.write_text("[]", encoding="utf-8")
        if not self.data_products_file.exists():
            self.data_products_file.parent.mkdir(parents=True, exist_ok=True)
            self.data_products_file.write_text("[]", encoding="utf-8")


def workspace_from_env(default_root: str | None = None) -> Tuple[ContractsAppWorkspace, bool]:
    """Return (workspace, created) using environment defaults when available."""

    env_root = os.getenv("DC43_CONTRACTS_APP_WORK_DIR") or os.getenv("DC43_DEMO_WORK_DIR")
    root_value = env_root or default_root
    created = False
    if root_value:
        root = Path(root_value).expanduser()
        if not root.exists():
            created = True
            root.mkdir(parents=True, exist_ok=True)
    else:
        root = Path(tempfile.mkdtemp(prefix="dc43_contracts_app_"))
        created = True
    workspace = ContractsAppWorkspace(
        root=root,
        contracts_dir=root / "contracts",
        data_dir=root / "data",
        records_dir=root / "records",
        datasets_file=root / "records" / "datasets.json",
        dq_status_dir=root / "records" / "dq_state" / "status",
        data_products_file=root / "records" / "data_products.json",
    )
    workspace.ensure()
    return workspace, created


__all__ = ["ContractsAppWorkspace", "workspace_from_env"]
