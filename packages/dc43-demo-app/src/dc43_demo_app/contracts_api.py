"""Compatibility facade exposing contracts helpers for the demo pipeline."""

from __future__ import annotations

from pathlib import Path

from dc43_service_clients.contracts.client.local import LocalContractServiceClient
from dc43_service_clients.data_products.client.local import LocalDataProductServiceClient
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance.client.local import (
    LocalGovernanceServiceClient,
    build_local_governance_service,
)
from dc43_service_backends.data_products import FilesystemDataProductServiceBackend

from .contracts_records import (
    DatasetRecord,
    dq_version_records,
    get_store,
    load_contract_meta,
    load_records,
    pop_flash,
    queue_flash,
    save_records,
    scenario_run_rows,
)
from .contracts_workspace import (
    current_workspace,
    prepare_demo_workspace,
    refresh_dataset_aliases,
    register_dataset_version as _register_dataset_version,
    set_active_version as _set_active_version,
)

prepare_demo_workspace()
_WORKSPACE = current_workspace()

DATA_DIR: Path = _WORKSPACE.data_dir
DATASETS_FILE: Path = _WORKSPACE.datasets_file
DATA_PRODUCTS_DIR: Path = _WORKSPACE.data_products_dir

store = get_store()
contract_service = LocalContractServiceClient(store=store)
dq_service = LocalDataQualityServiceClient()
governance_service: LocalGovernanceServiceClient = build_local_governance_service(store)
_DATA_PRODUCT_BACKEND = FilesystemDataProductServiceBackend(DATA_PRODUCTS_DIR)
data_product_service = LocalDataProductServiceClient(backend=_DATA_PRODUCT_BACKEND)


def register_dataset_version(dataset: str, version: str, source: Path) -> None:
    _register_dataset_version(current_workspace(), dataset, version, source)


def set_active_version(dataset: str, version: str) -> None:
    _set_active_version(current_workspace(), dataset, version)


__all__ = [
    "DATA_DIR",
    "DATASETS_FILE",
    "DATA_PRODUCTS_DIR",
    "DatasetRecord",
    "contract_service",
    "dq_service",
    "governance_service",
    "data_product_service",
    "load_records",
    "save_records",
    "queue_flash",
    "pop_flash",
    "scenario_run_rows",
    "dq_version_records",
    "load_contract_meta",
    "register_dataset_version",
    "set_active_version",
    "refresh_dataset_aliases",
    "store",
]

