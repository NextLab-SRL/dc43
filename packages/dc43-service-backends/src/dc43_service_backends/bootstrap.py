"""Factory helpers for wiring contract and data product backends."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .config import ContractStoreConfig, DataProductStoreConfig, ServiceBackendsConfig
from .contracts.backend import LocalContractServiceBackend
from .contracts.backend.interface import ContractServiceBackend
from .contracts.backend.stores import (
    CollibraContractStore,
    DeltaContractStore,
    FSContractStore,
    HttpCollibraContractAdapter,
    StubCollibraContractAdapter,
)
from .contracts.backend.stores.interface import ContractStore
from .data_products import (
    CollibraDataProductServiceBackend,
    DataProductServiceBackend,
    DeltaDataProductServiceBackend,
    FilesystemDataProductServiceBackend,
    LocalDataProductServiceBackend,
    StubCollibraDataProductAdapter,
)

if TYPE_CHECKING:  # pragma: no cover - help type-checkers without importing pyspark
    from pyspark.sql import SparkSession as SparkSessionType
else:
    SparkSessionType = object

try:  # pragma: no cover - optional dependency resolved at runtime
    from pyspark.sql import SparkSession as _SparkSession
except ModuleNotFoundError:  # pragma: no cover - resolved via monkeypatch in tests
    _SparkSession = None

# Expose ``SparkSession`` so tests can monkeypatch it even when ``pyspark`` is
# unavailable. Calls must use ``_get_spark_session`` to enforce runtime checks.
SparkSession = _SparkSession  # type: ignore[assignment]

__all__ = [
    "build_contract_store",
    "build_contract_backend",
    "build_data_product_backend",
    "build_backends",
]


def _resolve_collibra_store(config: ContractStoreConfig) -> ContractStore:
    base_path = config.base_path or config.root
    path = Path(base_path).expanduser() if base_path else None
    catalog = config.catalog or None
    adapter = StubCollibraContractAdapter(
        base_path=str(path) if path else None,
        catalog=catalog,
    )
    return CollibraContractStore(
        adapter,
        default_status=config.default_status,
        status_filter=config.status_filter,
    )


def _resolve_collibra_http_store(config: ContractStoreConfig) -> ContractStore:
    if not config.base_url:
        raise RuntimeError(
            "contract_store.base_url is required when type is 'collibra_http'",
        )
    adapter = HttpCollibraContractAdapter(
        config.base_url,
        token=config.token,
        timeout=config.timeout,
        contract_catalog=config.catalog or None,
        contracts_endpoint_template=(
            config.contracts_endpoint_template
            or "/rest/2.0/dataproducts/{data_product}/ports/{port}/contracts"
        ),
    )
    return CollibraContractStore(
        adapter,
        default_status=config.default_status,
        status_filter=config.status_filter,
    )


def _get_spark_session(config_section: str) -> "SparkSessionType":
    if SparkSession is None:
        raise RuntimeError(
            f"pyspark is required when {config_section}.type is 'delta'.",
        )

    return SparkSession.builder.getOrCreate()


def build_contract_store(config: ContractStoreConfig) -> ContractStore:
    """Instantiate a contract store matching ``config``."""

    store_type = (config.type or "filesystem").lower()

    if store_type == "filesystem":
        root = config.root
        path = Path(root) if root else Path.cwd() / "contracts"
        path.mkdir(parents=True, exist_ok=True)
        return FSContractStore(str(path))

    if store_type == "delta":
        spark = _get_spark_session("contract_store")
        table = config.table or None
        base_path = config.base_path or config.root
        if not (table or base_path):
            raise RuntimeError(
                "contract_store.table or contract_store.base_path must be configured for the delta store",
            )
        return DeltaContractStore(
            spark,
            table=table,
            path=str(base_path) if base_path and not table else None,
        )

    if store_type == "collibra_stub":
        return _resolve_collibra_store(config)

    if store_type == "collibra_http":
        return _resolve_collibra_http_store(config)

    raise RuntimeError(f"Unsupported contract store type: {store_type}")


def build_contract_backend(config: ContractStoreConfig) -> ContractServiceBackend:
    """Return a local contract backend wired against ``config``."""

    store = build_contract_store(config)
    return LocalContractServiceBackend(store)


def build_data_product_backend(config: DataProductStoreConfig) -> DataProductServiceBackend:
    """Instantiate a data product backend matching ``config``."""

    store_type = (config.type or "memory").lower()

    if store_type in {"memory", "local"}:
        return LocalDataProductServiceBackend()

    if store_type == "filesystem":
        root = config.root
        path = Path(root) if root else Path.cwd() / "data-products"
        path.mkdir(parents=True, exist_ok=True)
        return FilesystemDataProductServiceBackend(path)

    if store_type == "delta":
        spark = _get_spark_session("data_product_store")
        table = config.table or None
        base_path = config.base_path or config.root
        if not (table or base_path):
            raise RuntimeError(
                "data_product_store.table or data_product_store.base_path must be configured for the delta store",
            )
        return DeltaDataProductServiceBackend(
            spark,
            table=table,
            path=str(base_path) if base_path and not table else None,
        )

    if store_type == "collibra_stub":
        base_path = config.base_path or config.root
        adapter = StubCollibraDataProductAdapter(str(base_path) if base_path else None)
        return CollibraDataProductServiceBackend(adapter)

    raise RuntimeError(f"Unsupported data product store type: {store_type}")


def build_backends(config: ServiceBackendsConfig) -> tuple[ContractServiceBackend, DataProductServiceBackend]:
    """Construct contract and data product backends using ``config``."""

    contract_backend = build_contract_backend(config.contract_store)
    data_product_backend = build_data_product_backend(config.data_product_store)
    return contract_backend, data_product_backend
