"""Factory helpers for wiring contract, data product, and DQ backends."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

from .config import (
    ContractStoreConfig,
    DataProductStoreConfig,
    DataQualityBackendConfig,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
)
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
from .data_quality.backend import (
    DataQualityManager,
    DataQualityServiceBackend,
    LocalDataQualityServiceBackend,
    RemoteDataQualityServiceBackend,
)
from .data_quality.backend.engines import (
    DataQualityExecutionEngine,
    GreatExpectationsEngine,
    NativeDataQualityEngine,
    SodaEngine,
)
from .governance.backend import GovernanceServiceBackend, LocalGovernanceServiceBackend
from .governance.bootstrap import build_dataset_contract_link_hooks
from .governance.hooks import DatasetContractLinkHook
from .governance.backend.stores import (
    GovernanceStore,
    InMemoryGovernanceStore,
    SQLGovernanceStore,
)
from .governance.backend.stores._table_names import derive_related_table_name
from .governance.backend.stores.filesystem import FilesystemGovernanceStore

try:  # pragma: no cover - optional dependencies
    from .governance.backend.stores.delta import DeltaGovernanceStore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    DeltaGovernanceStore = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependencies
    from .governance.backend.stores.http import HttpGovernanceStore
except ModuleNotFoundError:  # pragma: no cover - httpx optional
    HttpGovernanceStore = None  # type: ignore[assignment]

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
    "BackendSuite",
    "build_contract_store",
    "build_contract_backend",
    "build_data_product_backend",
    "build_data_quality_backend",
    "build_governance_store",
    "build_backends",
]


@dataclass(slots=True)
class BackendSuite:
    """Bundle of service backends resolved from configuration."""

    contract: ContractServiceBackend
    data_product: DataProductServiceBackend
    data_quality: DataQualityServiceBackend
    governance: GovernanceServiceBackend
    governance_store: GovernanceStore
    contract_store: ContractStore | None = None
    link_hooks: tuple[DatasetContractLinkHook, ...] = ()

    def __iter__(self):
        yield self.contract
        yield self.data_product
        yield self.data_quality
        yield self.governance
        yield self.governance_store


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
            log_sql=config.log_sql,
        )

    if store_type == "sql":
        try:
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:  # pragma: no cover - handled in tests
            raise RuntimeError(
                "sqlalchemy is required when contract_store.type is 'sql'.",
            ) from exc

        from .contracts.backend.stores.sql import SQLContractStore

        if not config.dsn:
            raise RuntimeError(
                "contract_store.dsn must be configured when type is 'sql'",
            )

        engine = create_engine(config.dsn, echo=bool(config.log_sql))
        table_name = config.table or "contracts"
        return SQLContractStore(engine, table_name=table_name, schema=config.schema)

    if store_type == "collibra_stub":
        return _resolve_collibra_store(config)

    if store_type == "collibra_http":
        return _resolve_collibra_http_store(config)

    raise RuntimeError(f"Unsupported contract store type: {store_type}")


def build_contract_backend(
    config: ContractStoreConfig,
    *,
    store: ContractStore | None = None,
) -> ContractServiceBackend:
    """Return a local contract backend wired against ``config``."""

    resolved_store = store or build_contract_store(config)
    return LocalContractServiceBackend(resolved_store)


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

    if store_type == "sql":
        try:
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:  # pragma: no cover - handled in tests
            raise RuntimeError(
                "sqlalchemy is required when data_product_store.type is 'sql'.",
            ) from exc

        from .data_products.backend.stores.sql import SQLDataProductStore

        if not config.dsn:
            raise RuntimeError(
                "data_product_store.dsn must be configured when type is 'sql'",
            )

        engine = create_engine(config.dsn, echo=bool(config.log_sql))
        table_name = config.table or "data_products"
        store = SQLDataProductStore(engine, table_name=table_name, schema=config.schema)
        return LocalDataProductServiceBackend(store=store)

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
            log_sql=config.log_sql,
        )

    if store_type == "collibra_stub":
        base_path = config.base_path or config.root
        adapter = StubCollibraDataProductAdapter(str(base_path) if base_path else None)
        return CollibraDataProductServiceBackend(adapter)

    raise RuntimeError(f"Unsupported data product store type: {store_type}")


def _coerce_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"1", "true", "yes", "on"}:
            return True
        if normalised in {"0", "false", "no", "off"}:
            return False
    return default


def _build_engine_from_config(
    name: str, payload: Mapping[str, object]
) -> tuple[str, DataQualityExecutionEngine]:
    engine_type = str(payload.get("type") or name).strip().lower()
    engine_name = name.strip().lower()

    if engine_type in {"native", "builtin"}:
        strict_types = _coerce_bool(payload.get("strict_types"), True)
        allow_extra = _coerce_bool(payload.get("allow_extra_columns"), True)
        severity = str(payload.get("expectation_severity") or "error")
        engine = NativeDataQualityEngine(
            strict_types=strict_types,
            allow_extra_columns=allow_extra,
            expectation_severity=severity,
        )
        return engine_name, engine

    if engine_type in {"great_expectations", "ge"}:
        metrics_key = str(payload.get("metrics_key") or engine_name or "great_expectations")
        suite_path_raw = payload.get("suite_path") or payload.get("expectations_path")
        suite_path = str(suite_path_raw).strip() if isinstance(suite_path_raw, str) and suite_path_raw.strip() else None
        engine = GreatExpectationsEngine(metrics_key=metrics_key, suite_path=suite_path)
        return engine_name, engine

    if engine_type == "soda":
        metrics_key = str(payload.get("metrics_key") or engine_name or "soda")
        checks_raw = (
            payload.get("checks_path")
            or payload.get("suite_path")
            or payload.get("expectations_path")
        )
        checks_path = str(checks_raw).strip() if isinstance(checks_raw, str) and checks_raw.strip() else None
        engine = SodaEngine(metrics_key=metrics_key, checks_path=checks_path)
        return engine_name, engine

    raise RuntimeError(f"Unsupported data-quality engine type: {engine_type}")


def _build_local_dq_manager(config: DataQualityBackendConfig) -> DataQualityManager:
    engine_mapping: dict[str, DataQualityExecutionEngine] = {}
    for name, payload in config.engines.items():
        if not isinstance(payload, Mapping):
            continue
        engine_name, engine = _build_engine_from_config(name, payload)
        engine_mapping[engine_name] = engine
    return DataQualityManager(
        default_engine=config.default_engine,
        engines=engine_mapping,
    )


def build_data_quality_backend(
    config: DataQualityBackendConfig,
) -> DataQualityServiceBackend:
    """Instantiate a data-quality backend matching ``config``."""

    backend_type = (config.type or "local").lower()

    if backend_type in {"local", "filesystem"}:
        manager = _build_local_dq_manager(config)
        return LocalDataQualityServiceBackend(manager)

    if backend_type in {"remote", "http"}:
        if not config.base_url:
            raise RuntimeError(
                "data_quality_backend.base_url is required when type is 'http'",
            )
        try:
            from dc43_service_clients.data_quality.client.remote import (
                RemoteDataQualityServiceClient,
            )
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "httpx is required when data_quality_backend.type is 'http'. "
                "Install 'dc43-service-clients[http]' or ensure httpx is available.",
            ) from exc

        headers = dict(config.headers)
        client = RemoteDataQualityServiceClient(
            base_url=config.base_url,
            headers=headers or None,
            token=config.token,
            token_header=(
                config.token_header
                if config.token_header is not None
                else "Authorization"
            ),
            token_scheme=(
                config.token_scheme
                if config.token_scheme is not None
                else "Bearer"
            ),
        )
        return RemoteDataQualityServiceBackend(client)

    raise RuntimeError(f"Unsupported data quality backend type: {backend_type}")


def build_governance_store(config: GovernanceStoreConfig) -> GovernanceStore:
    """Instantiate a governance metadata store matching ``config``."""

    store_type = (config.type or "memory").lower()

    if store_type in {"memory", "local"}:
        return InMemoryGovernanceStore()

    if store_type == "filesystem":
        base = config.root or config.base_path or Path.cwd() / "governance"
        path = Path(base).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return FilesystemGovernanceStore(str(path))

    if store_type == "sql":
        try:
            from sqlalchemy import create_engine
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "sqlalchemy is required when governance_store.type is 'sql'.",
            ) from exc
        if not config.dsn:
            raise RuntimeError(
                "governance_store.dsn must be configured when type is 'sql'",
            )
        engine = create_engine(config.dsn, echo=bool(config.log_sql))
        status_table = config.status_table or "dq_status"
        activity_table = config.activity_table or "dq_activity"
        link_table = config.link_table or "dq_dataset_contract_links"
        metrics_table = config.metrics_table
        if not metrics_table and status_table:
            metrics_table = derive_related_table_name(status_table, "metrics")
        return SQLGovernanceStore(
            engine,
            schema=config.schema,
            status_table=status_table,
            activity_table=activity_table,
            link_table=link_table,
            metrics_table=metrics_table,
        )

    if store_type == "delta":
        if DeltaGovernanceStore is None:
            raise RuntimeError(
                "pyspark is required when governance_store.type is 'delta'.",
            )
        spark = _get_spark_session("governance_store")
        base_path = config.base_path or config.root
        if not (base_path or (config.status_table and config.activity_table and config.link_table)):
            raise RuntimeError(
                "Configure governance_store.base_path or explicit Delta table names",
            )
        return DeltaGovernanceStore(
            spark,
            base_path=str(base_path) if base_path else None,
            status_table=config.status_table,
            activity_table=config.activity_table,
            link_table=config.link_table,
            metrics_table=config.metrics_table,
            log_sql=config.log_sql,
        )

    if store_type in {"http", "remote"}:
        if HttpGovernanceStore is None:
            raise RuntimeError(
                "httpx is required when governance_store.type is 'http'.",
            )
        if not config.base_url:
            raise RuntimeError(
                "governance_store.base_url is required when type is 'http'",
            )
        headers = dict(config.headers)
        return HttpGovernanceStore(
            config.base_url,
            headers=headers or None,
            token=config.token,
            token_header=(
                config.token_header
                if config.token_header is not None
                else "Authorization"
            ),
            token_scheme=(
                config.token_scheme
                if config.token_scheme is not None
                else "Bearer"
            ),
            timeout=config.timeout,
        )

    raise RuntimeError(f"Unsupported governance store type: {store_type}")


def build_backends(config: ServiceBackendsConfig) -> BackendSuite:
    """Construct service backends and governance hooks using ``config``."""

    contract_store = build_contract_store(config.contract_store)
    contract_backend = build_contract_backend(
        config.contract_store, store=contract_store
    )
    data_product_backend = build_data_product_backend(config.data_product_store)
    dq_backend = build_data_quality_backend(config.data_quality)
    governance_store = build_governance_store(config.governance_store)
    link_hooks = build_dataset_contract_link_hooks(config)
    governance_backend = LocalGovernanceServiceBackend(
        contract_client=contract_backend,
        dq_client=dq_backend,
        data_product_client=data_product_backend,
        draft_store=contract_store,
        link_hooks=link_hooks,
        store=governance_store,
    )
    return BackendSuite(
        contract=contract_backend,
        data_product=data_product_backend,
        data_quality=dq_backend,
        governance=governance_backend,
        governance_store=governance_store,
        contract_store=contract_store,
        link_hooks=link_hooks,
    )
