from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple

import pytest

from open_data_contract_standard.model import DataQuality, OpenDataContractStandard

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_integrations.spark.io import (
    read_from_data_product,
    read_with_contract,
    read_with_governance,
    write_to_data_product,
    write_with_contract,
    write_with_governance,
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    StaticDatasetLocator,
    ContractVersionLocator,
    DatasetResolution,
    DefaultReadStatusStrategy,
    BatchReadExecutor,
    BatchWriteExecutor,
)
from dc43_integrations.spark.violation_strategy import (
    SplitWriteViolationStrategy,
    NoOpWriteViolationStrategy,
)
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance import (
    GovernanceReadContext,
    build_local_governance_service,
)
from dc43_service_clients.governance.models import ResolvedReadPlan, ResolvedWritePlan
from dc43_service_clients.data_products import DataProductInputBinding, DataProductOutputBinding
from dc43_service_clients.odps import OpenDataProductStandard
from dc43_service_backends.data_products import DataProductRegistrationResult
from dc43_service_backends.core.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard as DataProductDoc,
)
from .helpers.orders import build_orders_contract, materialise_orders
from datetime import datetime
import logging


@dataclass
class _StubContract:
    id: str
    version: str
    status: str = "active"
    servers: list = field(default_factory=list)


def persist_contract(
    tmp_path: Path, contract: OpenDataContractStandard
) -> Tuple[FSContractStore, LocalContractServiceClient, LocalDataQualityServiceClient]:
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return store, LocalContractServiceClient(store), LocalDataQualityServiceClient()


def _gov_read_request(
    contract: Optional[OpenDataContractStandard] = None,
    *,
    context_overrides: Optional[Mapping[str, Any]] = None,
    **overrides: Any,
) -> GovernanceSparkReadRequest:
    context: dict[str, Any] = {}
    if context_overrides:
        context.update(context_overrides)
    if contract is not None and "contract" not in context:
        context["contract"] = {
            "contract_id": contract.id,
            "contract_version": contract.version,
        }
    return GovernanceSparkReadRequest(context=context, **overrides)


def _gov_write_request(
    contract: Optional[OpenDataContractStandard] = None,
    *,
    context_overrides: Optional[Mapping[str, Any]] = None,
    **overrides: Any,
) -> GovernanceSparkWriteRequest:
    context: dict[str, Any] = {}
    if context_overrides:
        context.update(context_overrides)
    if contract is not None and "contract" not in context:
        context["contract"] = {
            "contract_id": contract.id,
            "contract_version": contract.version,
        }
    return GovernanceSparkWriteRequest(context=context, **overrides)


def test_governance_wrappers_require_only_governance_client(
    spark, tmp_path: Path
) -> None:
    contract_path = tmp_path / "orders"
    contract = build_orders_contract(contract_path)
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    source_path = tmp_path / "source"
    materialise_orders(spark, source_path)
    df = spark.read.format("parquet").load(str(source_path))

    validation, status = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context={
                "contract": {
                    "contract_id": contract.id,
                    "contract_version": contract.version,
                }
            },
            path=str(contract_path),
            format="parquet",
        ),
        governance_service=governance,
        return_status=True,
    )

    assert validation.ok
    assert validation.details.get("observation_scope") == "pre_write_dataframe"
    assert status is not None and status.ok
    assert status.details.get("observation_scope") == "pre_write_dataframe"
    assert status.details.get("observation_operation") == "write"

    read_df, read_status = read_with_governance(
        spark,
        GovernanceSparkReadRequest(
            context=GovernanceReadContext(
                contract={
                    "contract_id": contract.id,
                    "contract_version": contract.version,
                }
            ),
            path=str(contract_path),
            format="parquet",
        ),
        governance_service=governance,
        return_status=True,
    )

    assert read_status is not None and read_status.ok
    assert read_status.details.get("observation_scope") == "input_slice"
    assert read_status.details.get("observation_operation") == "read"
    assert read_df.count() == df.count()


class StubDataProductService:
    def __init__(
        self,
        contract_ref: tuple[str, str] | Mapping[str, tuple[str, str]] | None = None,
        *,
        registration_changed: bool = True,
        products: Mapping[str, Sequence[DataProductDoc]] | None = None,
    ) -> None:
        from dc43_service_backends.core.versioning import version_key

        self.contract_ref = contract_ref
        self.input_calls: list[dict[str, Any]] = []
        self.output_calls: list[dict[str, Any]] = []
        self.registration_changed = registration_changed
        self._products: dict[str, dict[str, DataProductDoc]] = {}
        self._version_key = version_key
        if products:
            for product_id, versions in products.items():
                store = self._products.setdefault(product_id, {})
                for doc in versions:
                    version = doc.version or ""
                    store[version] = doc.clone()

    def _product_versions(self, data_product_id: str) -> dict[str, DataProductDoc]:
        return self._products.setdefault(data_product_id, {})

    def _store_product(self, product: DataProductDoc) -> None:
        version = product.version or ""
        if not version:
            return
        store = self._product_versions(product.id)
        store[version] = product.clone()

    def get(self, data_product_id: str, version: str) -> DataProductDoc:
        store = self._product_versions(data_product_id)
        if version not in store:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return store[version].clone()

    def latest(self, data_product_id: str) -> Optional[DataProductDoc]:
        store = self._product_versions(data_product_id)
        if not store:
            return None
        version = max(store, key=self._version_key)
        return store[version].clone()

    def list_versions(self, data_product_id: str) -> list[str]:
        return sorted(self._product_versions(data_product_id), key=self._version_key)

    def register_input_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[dict[str, Any]] = None,
        source_data_product: Optional[str] = None,
        source_output_port: Optional[str] = None,
    ) -> DataProductRegistrationResult:
        self.input_calls.append(
            {
                "data_product_id": data_product_id,
                "port_name": port_name,
                "contract_id": contract_id,
                "contract_version": contract_version,
                "source_data_product": source_data_product,
                "source_output_port": source_output_port,
            }
        )
        if not self.registration_changed:
            existing = self.latest(data_product_id)
            doc = existing.clone() if existing is not None else DataProductDoc(
                id=data_product_id,
                status="active",
                version="1.0.0",
            )
            doc.input_ports.append(
                DataProductInputPort(name=port_name, version=contract_version, contract_id=contract_id)
            )
            self._store_product(doc)
            return DataProductRegistrationResult(product=doc, changed=False)

        status = "draft"
        doc = DataProductDoc(id=data_product_id, status=status, version="0.1.0-draft")
        doc.input_ports.append(
            DataProductInputPort(name=port_name, version=contract_version, contract_id=contract_id)
        )
        self._store_product(doc)
        return DataProductRegistrationResult(product=doc, changed=True)

    def register_output_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[dict[str, Any]] = None,
    ) -> DataProductRegistrationResult:
        self.output_calls.append(
            {
                "data_product_id": data_product_id,
                "port_name": port_name,
                "contract_id": contract_id,
                "contract_version": contract_version,
            }
        )
        if not self.registration_changed:
            existing = self.latest(data_product_id)
            doc = existing.clone() if existing is not None else DataProductDoc(
                id=data_product_id,
                status="active",
                version="1.0.0",
            )
            doc.output_ports.append(
                DataProductOutputPort(name=port_name, version=contract_version, contract_id=contract_id)
            )
            self._store_product(doc)
            return DataProductRegistrationResult(product=doc, changed=False)

        status = "draft"
        doc = DataProductDoc(id=data_product_id, status=status, version="0.1.0-draft")
        doc.output_ports.append(
            DataProductOutputPort(name=port_name, version=contract_version, contract_id=contract_id)
        )
        self._store_product(doc)
        return DataProductRegistrationResult(product=doc, changed=True)

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        if isinstance(self.contract_ref, Mapping):
            return self.contract_ref.get(port_name)
        if self.contract_ref is not None:
            return self.contract_ref
        latest = self.latest(data_product_id)
        if latest is None:
            return None
        port = latest.find_output_port(port_name)
        if port is None:
            return None
        return port.contract_id, port.version


def test_read_blocks_on_draft_contract_status(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    contract.status = "draft"
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    with pytest.raises(ValueError, match="draft"):
        read_with_contract(
            spark,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            data_quality_service=dq_service,
            governance_service=governance,
        )


def test_read_allows_draft_contract_with_strategy(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    contract.status = "draft"
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    df, status = read_with_contract(
        spark,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq_service,
        governance_service=governance,
        status_strategy=DefaultReadStatusStrategy(
            allowed_contract_statuses=("active", "draft"),
        ),
    )

    assert df.count() == 2
    assert status is not None


def test_read_with_governance_blocks_on_draft_contract_status(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-draft")
    contract = build_orders_contract(str(data_dir))
    contract.status = "draft"
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    with pytest.raises(ValueError, match="draft"):
        read_with_governance(
            spark,
            _gov_read_request(contract),
            governance_service=governance,
        )


def test_read_with_governance_allows_draft_contract_with_strategy(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-draft-allowed")
    contract = build_orders_contract(str(data_dir))
    contract.status = "draft"
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    df, status = read_with_governance(
        spark,
        _gov_read_request(
            contract,
            status_strategy=DefaultReadStatusStrategy(
                allowed_contract_statuses=("active", "draft"),
            ),
        ),
        governance_service=governance,
        return_status=True,
    )

    assert df.count() == 2
    assert status is not None


def test_read_registers_data_product_input_port(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService()

    with pytest.raises(RuntimeError, match="requires review"):
        read_with_contract(
            spark,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            data_quality_service=dq_service,
            governance_service=governance,
            data_product_service=dp_service,
            data_product_input={"data_product": "dp.analytics"},
        )

    assert dp_service.input_calls
    assert dp_service.input_calls[0]["data_product_id"] == "dp.analytics"


def test_read_skips_registration_when_input_port_exists(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService(registration_changed=False)

    df, status = read_with_contract(
        spark,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq_service,
        governance_service=governance,
        data_product_service=dp_service,
        data_product_input={"data_product": "dp.analytics"},
    )

    assert df.count() == 2
    assert status is not None
    assert dp_service.input_calls
    assert dp_service.input_calls[0]["data_product_id"] == "dp.analytics"


def test_read_resolves_contract_from_data_product_port(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService(
        contract_ref=(contract.id, contract.version), registration_changed=False
    )

    df, status = read_with_contract(
        spark,
        contract_service=contract_service,
        expected_contract_version=None,
        data_quality_service=dq_service,
        governance_service=governance,
        data_product_service=dp_service,
        data_product_input={
            "data_product": "dp.analytics",
            "source_data_product": "dp.analytics",
            "source_output_port": "primary",
        },
    )

    assert df.count() == 2
    assert status is not None
    assert dp_service.input_calls
    assert dp_service.input_calls[0]["source_output_port"] == "primary"


def test_read_with_governance_registers_input_binding(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-input")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    dp_service = StubDataProductService()
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    with pytest.raises(RuntimeError, match="requires review"):
        read_with_governance(
            spark,
            _gov_read_request(
                contract,
                context_overrides={"input_binding": {"data_product": "dp.analytics"}},
            ),
            governance_service=governance,
        )

    assert dp_service.input_calls
    assert dp_service.input_calls[0]["data_product_id"] == "dp.analytics"


def test_read_with_governance_blocks_on_existing_draft_product(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-input-draft")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    dp_service = StubDataProductService()
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    with pytest.raises(RuntimeError, match="requires review"):
        read_with_governance(
            spark,
            _gov_read_request(
                contract,
                context_overrides={"input_binding": {"data_product": "dp.analytics"}},
            ),
            governance_service=governance,
        )

    dp_service.registration_changed = False

    with pytest.raises(ValueError, match="status"):
        read_with_governance(
            spark,
            _gov_read_request(
                contract,
                context_overrides={"input_binding": {"data_product": "dp.analytics"}},
            ),
            governance_service=governance,
        )


def test_read_with_governance_skips_registration_when_input_exists(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-input-existing")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    doc = DataProductDoc(id="dp.analytics", status="active", version="1.0.0")
    dp_service = StubDataProductService(registration_changed=False, products={"dp.analytics": [doc]})
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    df, status = read_with_governance(
        spark,
        _gov_read_request(
            contract,
            context_overrides={"input_binding": {"data_product": "dp.analytics"}},
        ),
        governance_service=governance,
        return_status=True,
    )

    assert df.count() == 2
    assert status is not None
    assert dp_service.input_calls
    assert dp_service.input_calls[0]["data_product_id"] == "dp.analytics"


def test_read_with_governance_enforces_data_product_version_constraint(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-input-version")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    existing_doc = DataProductDoc(id="dp.analytics", status="active", version="2.0.0")
    dp_service = StubDataProductService(
        registration_changed=False,
        products={"dp.analytics": [existing_doc]},
    )
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    with pytest.raises(ValueError, match="version"):
        read_with_governance(
            spark,
            _gov_read_request(
                contract,
                context_overrides={
                    "input_binding": {
                        "data_product": "dp.analytics",
                        "data_product_version": "==1.0.0",
                    }
                },
            ),
            governance_service=governance,
        )


def test_read_with_contract_uses_requested_data_product_version(
    spark, tmp_path: Path, caplog
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "contract-input-requested-version")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    historical = DataProductDoc(id="dp.analytics", status="active", version="0.1.0")
    latest = DataProductDoc(id="dp.analytics", status="active", version="0.2.0")
    dp_service = StubDataProductService(
        registration_changed=False,
        products={"dp.analytics": [historical, latest]},
    )

    caplog.set_level(logging.WARNING, "dc43_integrations.spark.io")

    df, status = read_with_contract(
        spark,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        path=str(data_dir),
        format="parquet",
        data_quality_service=dq_service,
        data_product_service=dp_service,
        data_product_input={
            "data_product": "dp.analytics",
            "data_product_version": "0.1.0",
        },
        return_status=True,
    )

    assert df.count() == 2
    messages = [record.getMessage() for record in caplog.records]
    assert not any("does not satisfy" in message for message in messages)
    assert not any("is not available for input registration" in message for message in messages)


def test_read_with_contract_errors_when_data_product_version_missing(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "contract-input-missing-version")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    existing_doc = DataProductDoc(id="dp.analytics", status="active", version="0.2.0")
    dp_service = StubDataProductService(
        registration_changed=False,
        products={"dp.analytics": [existing_doc]},
    )

    with pytest.raises(ValueError, match="0.9.9"):
        read_with_contract(
            spark,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            path=str(data_dir),
            format="parquet",
            data_quality_service=dq_service,
            data_product_service=dp_service,
            data_product_input={
                "data_product": "dp.analytics",
                "data_product_version": "0.9.9",
            },
        )


def test_read_with_governance_resolves_contract_from_input_binding(
    spark, tmp_path: Path
) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-dp-binding")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    dp_service = StubDataProductService(
        contract_ref={"primary": (contract.id, contract.version)}, registration_changed=False
    )
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    df, status = read_with_governance(
        spark,
        _gov_read_request(
            None,
            context_overrides={
                "input_binding": {
                    "data_product": "dp.analytics",
                    "port_name": "primary",
                    "source_data_product": "dp.analytics",
                    "source_output_port": "primary",
                }
            },
            path=str(data_dir),
            format="parquet",
        ),
        governance_service=governance,
        return_status=True,
    )

    assert df.count() == 2
    assert status is not None
    assert dp_service.input_calls
    assert dp_service.input_calls[0]["source_output_port"] == "primary"


def test_write_blocks_on_deprecated_contract_status(spark, tmp_path: Path) -> None:
    dest_dir = tmp_path / "dq"
    contract = build_orders_contract(str(dest_dir))
    contract.status = "deprecated"
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    with pytest.raises(ValueError, match="deprecated"):
        write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            mode="overwrite",
            data_quality_service=dq_service,
        )


def test_write_allows_deprecated_contract_with_relaxed_strategy(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "relaxed"
    contract = build_orders_contract(str(dest_dir))
    contract.status = "deprecated"
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        data_quality_service=dq_service,
        violation_strategy=NoOpWriteViolationStrategy(
            allowed_contract_statuses=("active", "deprecated"),
        ),
    )

    assert result.ok


def test_write_with_governance_blocks_on_deprecated_contract_status(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "gov-dq"
    contract = build_orders_contract(str(dest_dir))
    contract.status = "deprecated"
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    with pytest.raises(ValueError, match="deprecated"):
        write_with_governance(
            df=df,
            request=_gov_write_request(
                contract,
                path=str(dest_dir),
                format="parquet",
                mode="overwrite",
            ),
            governance_service=governance,
        )


def test_write_with_governance_allows_deprecated_contract_with_relaxed_strategy(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "gov-relaxed"
    contract = build_orders_contract(str(dest_dir))
    contract.status = "deprecated"
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    result = write_with_governance(
        df=df,
        request=_gov_write_request(
            contract,
            path=str(dest_dir),
            format="parquet",
            mode="overwrite",
        ),
        governance_service=governance,
        violation_strategy=NoOpWriteViolationStrategy(
            allowed_contract_statuses=("active", "deprecated"),
        ),
        return_status=False,
    )

    assert result.ok


def test_dq_integration_blocks(spark, tmp_path: Path) -> None:
    data_dir = tmp_path / "parquet"
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    # Prepare data with one enum violation for currency
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "INR"),  # violation
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    df.write.mode("overwrite").format("parquet").save(str(data_dir))

    governance = build_local_governance_service(store)
    # enforce=False to avoid raising on validation expectation failures
    _, status = read_with_contract(
        spark,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        enforce=False,
        data_quality_service=dq_service,
        governance_service=governance,
        return_status=True,
    )
    assert status is not None
    assert status.status == "block"
    details = status.details or {}
    errors = details.get("errors") or []
    assert errors
    assert any("currency" in str(message) for message in errors)


def test_read_with_governance_dq_integration_blocks(spark, tmp_path: Path) -> None:
    data_dir = tmp_path / "gov-parquet"
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "INR"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    df.write.mode("overwrite").format("parquet").save(str(data_dir))

    _, status = read_with_governance(
        spark,
        _gov_read_request(contract),
        governance_service=governance,
        enforce=False,
        return_status=True,
    )

    assert status is not None
    assert status.status == "block"
    details = status.details or {}
    errors = details.get("errors") or []
    assert errors
    assert any("currency" in str(message) for message in errors)


def test_write_violation_blocks_by_default(spark, tmp_path: Path) -> None:
    dest_dir = tmp_path / "dq"
    contract = build_orders_contract(str(dest_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
            (2, 102, datetime(2024, 1, 2, 10, 0, 0), -5.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    governance = build_local_governance_service(store)
    result, status = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
        governance_service=governance,
        return_status=True,
    )
    assert status is not None
    assert status.status == "block"
    details = status.details or {}
    errors = details.get("errors") or []
    assert errors
    assert any("amount" in str(message) for message in errors)
    assert not result.ok  # violations surface as blocking failures


def test_write_with_governance_violation_blocks_by_default(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "gov-dq"
    contract = build_orders_contract(str(dest_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
            (2, 102, datetime(2024, 1, 2, 10, 0, 0), -5.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    result, status = write_with_governance(
        df=df,
        request=_gov_write_request(
            contract,
            path=str(dest_dir),
            format="parquet",
            mode="overwrite",
        ),
        governance_service=governance,
        enforce=False,
        return_status=True,
    )

    assert status is not None
    assert status.status == "block"
    details = status.details or {}
    errors = details.get("errors") or []
    assert errors
    assert any("amount" in str(message) for message in errors)
    assert not result.ok


def test_write_with_contract_uses_requested_data_product_version(
    spark, tmp_path: Path, caplog
) -> None:
    dest_dir = tmp_path / "contract-output-requested-version"
    contract = build_orders_contract(str(dest_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    historical = DataProductDoc(id="dp.analytics", status="active", version="0.1.0")
    latest = DataProductDoc(id="dp.analytics", status="active", version="0.2.0")
    dp_service = StubDataProductService(
        registration_changed=False,
        products={"dp.analytics": [historical, latest]},
    )
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    caplog.set_level(logging.WARNING, "dc43_integrations.spark.io")

    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        path=str(dest_dir),
        mode="overwrite",
        data_quality_service=dq_service,
        data_product_service=dp_service,
        data_product_output={
            "data_product": "dp.analytics",
            "port_name": "primary",
            "data_product_version": "0.1.0",
        },
    )

    assert result.ok
    messages = [record.getMessage() for record in caplog.records]
    assert not any("does not satisfy" in message for message in messages)
    assert not any("is not available for output registration" in message for message in messages)


def test_write_with_contract_errors_when_data_product_version_missing(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "contract-output-missing-version"
    contract = build_orders_contract(str(dest_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    existing_doc = DataProductDoc(id="dp.analytics", status="active", version="0.2.0")
    dp_service = StubDataProductService(
        registration_changed=False,
        products={"dp.analytics": [existing_doc]},
    )
    df = spark.createDataFrame(
        [
            (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        ],
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    with pytest.raises(ValueError, match="0.9.9"):
        write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            path=str(dest_dir),
            mode="overwrite",
            data_quality_service=dq_service,
            data_product_service=dp_service,
            data_product_output={
                "data_product": "dp.analytics",
                "port_name": "primary",
                "data_product_version": "0.9.9",
            },
        )


def test_write_validation_result_on_mismatch(spark, tmp_path: Path):
    dest_dir = tmp_path / "out"
    contract = build_orders_contract(str(dest_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    # Missing required column 'currency' to trigger validation error
    data = [(1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0)]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount"])
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,  # continue writing despite mismatch
        data_quality_service=dq_service,
    )
    assert not result.ok
    assert result.errors
    assert any("currency" in err.lower() for err in result.errors)


def test_inferred_contract_id_simple(spark, tmp_path: Path):
    dest = tmp_path / "out" / "sample" / "1.0.0"
    df = spark.createDataFrame([(1,)], ["a"])
    # Without a contract the function simply writes the dataframe.
    result = write_with_contract(
        df=df,
        path=str(dest),
        format="parquet",
        mode="overwrite",
        enforce=False,
    )
    assert result.ok
    assert not result.errors


def test_write_warn_on_path_mismatch(spark, tmp_path: Path):
    expected_dir = tmp_path / "expected"
    actual_dir = tmp_path / "actual"
    contract = build_orders_contract(str(expected_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        path=str(actual_dir),
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
    )
    assert any("does not match" in w for w in result.warnings)


def test_write_path_version_under_contract_root(spark, tmp_path: Path, caplog):
    base_dir = tmp_path / "data"
    contract_path = base_dir / "orders_enriched.parquet"
    contract = build_orders_contract(str(contract_path))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    target = base_dir / "orders_enriched" / "1.0.0"
    with caplog.at_level(logging.WARNING):
        result = write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            path=str(target),
            mode="overwrite",
            enforce=False,
            data_quality_service=dq_service,
        )
    assert not any("does not match contract server path" in msg for msg in caplog.messages)
    assert not any("does not match" in w for w in result.warnings)


def test_read_warn_on_format_mismatch(spark, tmp_path: Path, caplog):
    data_dir = tmp_path / "json"
    contract = build_orders_contract(str(data_dir), fmt="parquet")
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    df.write.mode("overwrite").json(str(data_dir))
    with caplog.at_level(logging.WARNING):
        read_with_contract(
            spark,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            format="json",
            enforce=False,
            data_quality_service=dq_service,
    )
    assert any(
        "format json does not match contract server format parquet" in m
        for m in caplog.messages
    )


def test_read_with_governance_warn_on_format_mismatch(
    spark, tmp_path: Path, caplog
) -> None:
    data_dir = tmp_path / "gov-json"
    contract = build_orders_contract(str(data_dir), fmt="parquet")
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    df.write.mode("overwrite").json(str(data_dir))
    with caplog.at_level(logging.WARNING):
        read_with_governance(
            spark,
            _gov_read_request(contract, format="json"),
            governance_service=governance,
            enforce=False,
        )
    assert any(
        "format json does not match contract server format parquet" in m
        for m in caplog.messages
    )


def test_write_warn_on_format_mismatch(spark, tmp_path: Path, caplog):
    dest_dir = tmp_path / "out"
    contract = build_orders_contract(str(dest_dir), fmt="parquet")
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    with caplog.at_level(logging.WARNING):
        result = write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            path=str(dest_dir),
            format="json",
            mode="overwrite",
            enforce=False,
            data_quality_service=dq_service,
        )
    assert any(
        "Format json does not match contract server format parquet" in w
        for w in result.warnings
    )
    assert any(
        "format json does not match contract server format parquet" in m.lower()
        for m in caplog.messages
    )


def test_write_with_governance_warn_on_format_mismatch(
    spark, tmp_path: Path, caplog
) -> None:
    dest_dir = tmp_path / "gov-out"
    contract = build_orders_contract(str(dest_dir), fmt="parquet")
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    with caplog.at_level(logging.WARNING):
        result = write_with_governance(
            df=df,
            request=_gov_write_request(
                contract,
                path=str(dest_dir),
                format="json",
                mode="overwrite",
            ),
            governance_service=governance,
            enforce=False,
        )
    assert any(
        "Format json does not match contract server format parquet" in w
        for w in result.warnings
    )
    assert any(
        "format json does not match contract server format parquet" in m.lower()
        for m in caplog.messages
    )


def test_write_split_strategy_creates_auxiliary_datasets(spark, tmp_path: Path):
    base_dir = tmp_path / "split"
    contract = build_orders_contract(str(base_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 15.5, "INR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    strategy = SplitWriteViolationStrategy()
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
        violation_strategy=strategy,
    )

    assert not result.ok
    assert any("outside enum" in error for error in result.errors)
    assert any("Valid subset written" in warning for warning in result.warnings)
    assert any("Rejected subset written" in warning for warning in result.warnings)

    valid_path = base_dir / strategy.valid_suffix
    reject_path = base_dir / strategy.reject_suffix

    valid_df = spark.read.parquet(str(valid_path))
    reject_df = spark.read.parquet(str(reject_path))

    assert valid_df.count() == 1
    assert reject_df.count() == 1
    assert {row.currency for row in valid_df.collect()} == {"EUR"}
    assert {row.currency for row in reject_df.collect()} == {"INR"}


def test_write_dq_violation_reports_status(spark, tmp_path: Path):
    dest_dir = tmp_path / "dq_out"
    contract = build_orders_contract(str(dest_dir))
    # Tighten quality rule to trigger a violation for the sample data below.
    contract.schema_[0].properties[3].quality = [DataQuality(mustBeGreaterThan=100)]
    store, contract_service, dq_service = persist_contract(tmp_path, contract)

    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 50.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 60.0, "USD"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    governance = build_local_governance_service(store)
    locator = StaticDatasetLocator(dataset_version="dq-out")
    result, status = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
        governance_service=governance,
        dataset_locator=locator,
        return_status=True,
    )

    assert not result.ok
    assert status is not None
    assert status.status == "block"
    assert status.details and status.details.get("violations", 0) > 0
    with pytest.raises(ValueError):
        write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            mode="overwrite",
            enforce=True,
            data_quality_service=dq_service,
            governance_service=governance,
            dataset_locator=locator,
        )


def test_write_with_governance_dq_violation_reports_status(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "gov-dq-out"
    contract = build_orders_contract(str(dest_dir))
    contract.schema_[0].properties[3].quality = [DataQuality(mustBeGreaterThan=100)]
    store, _, _ = persist_contract(tmp_path, contract)

    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 50.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 60.0, "USD"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    governance = build_local_governance_service(store)
    locator = StaticDatasetLocator(dataset_version="dq-out")
    result, status = write_with_governance(
        df=df,
        request=_gov_write_request(
            contract,
            path=str(dest_dir),
            format="parquet",
            mode="overwrite",
            dataset_locator=locator,
        ),
        governance_service=governance,
        enforce=False,
        return_status=True,
    )

    assert not result.ok
    assert status is not None
    assert status.status == "block"
    assert status.details and status.details.get("violations", 0) > 0
    with pytest.raises(ValueError):
        write_with_governance(
            df=df,
            request=_gov_write_request(
                contract,
                path=str(dest_dir),
                format="parquet",
                mode="overwrite",
                dataset_locator=locator,
            ),
            governance_service=governance,
            enforce=True,
        )


def test_write_keeps_existing_link_for_contract_upgrade(spark, tmp_path: Path):
    dest_dir = tmp_path / "upgrade"
    contract_v1 = build_orders_contract(str(dest_dir))
    data_ok = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 500.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 11, 0, 0), 750.0, "USD"),
    ]
    df_ok = spark.createDataFrame(
        data_ok,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    store = FSContractStore(str(tmp_path / "upgrade_contracts"))
    store.put(contract_v1)
    contract_service = LocalContractServiceClient(store)
    dq_service = LocalDataQualityServiceClient()
    governance = build_local_governance_service(store)
    upgrade_locator = StaticDatasetLocator(
        dataset_version="2024-01-01",
        dataset_id=f"path:{dest_dir}",
    )
    _, status_ok = write_with_contract(
        df=df_ok,
        contract_id=contract_v1.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract_v1.version}",
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
        governance_service=governance,
        dataset_locator=upgrade_locator,
        return_status=True,
    )

    assert status_ok is not None
    assert status_ok.status == "ok"

    dataset_ref = f"path:{dest_dir}"
    assert (
        governance.get_linked_contract_version(dataset_id=dataset_ref)
        == f"{contract_v1.id}:{contract_v1.version}"
    )
    assert (
        governance.get_linked_contract_version(
            dataset_id=dataset_ref,
            dataset_version="2024-01-01",
        )
        == f"{contract_v1.id}:{contract_v1.version}"
    )


def test_write_with_governance_keeps_existing_link_for_contract_upgrade(
    spark, tmp_path: Path
) -> None:
    dest_dir = tmp_path / "gov-upgrade"
    contract_v1 = build_orders_contract(str(dest_dir))
    data_ok = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 500.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 11, 0, 0), 750.0, "USD"),
    ]
    df_ok = spark.createDataFrame(
        data_ok,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    store = FSContractStore(str(tmp_path / "gov_upgrade_contracts"))
    store.put(contract_v1)
    governance = build_local_governance_service(store)
    locator = StaticDatasetLocator(
        dataset_version="2024-01-01",
        dataset_id=f"path:{dest_dir}",
    )
    result, status_ok = write_with_governance(
        df=df_ok,
        request=_gov_write_request(
            contract_v1,
            path=str(dest_dir),
            format="parquet",
            mode="overwrite",
            dataset_locator=locator,
        ),
        governance_service=governance,
        enforce=False,
        return_status=True,
    )

    assert result.ok
    assert status_ok is not None
    assert status_ok.status == "ok"

    dataset_ref = f"path:{dest_dir}"
    assert (
        governance.get_linked_contract_version(dataset_id=dataset_ref)
        == f"{contract_v1.id}:{contract_v1.version}"
    )
    assert (
        governance.get_linked_contract_version(
            dataset_id=dataset_ref,
            dataset_version="2024-01-01",
        )
        == f"{contract_v1.id}:{contract_v1.version}"
    )


def test_write_registers_data_product_output_port(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService()
    df = spark.read.parquet(str(data_dir))

    with pytest.raises(RuntimeError, match="requires review"):
        write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_service=contract_service,
            expected_contract_version=f"=={contract.version}",
            path=str(data_dir),
            mode="overwrite",
            data_quality_service=dq_service,
            governance_service=governance,
            data_product_service=dp_service,
            data_product_output={"data_product": "dp.analytics", "port_name": "primary"},
            return_status=True,
        )

    assert dp_service.output_calls
    assert dp_service.output_calls[0]["port_name"] == "primary"


def test_write_with_governance_registers_output_binding(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-output")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    dp_service = StubDataProductService()
    governance = build_local_governance_service(store, data_product_backend=dp_service)
    df = spark.read.parquet(str(data_dir))

    with pytest.raises(RuntimeError, match="requires review"):
        write_with_governance(
            df=df,
            request=_gov_write_request(
                contract,
                path=str(data_dir),
                format="parquet",
                mode="overwrite",
                context_overrides={
                    "output_binding": {"data_product": "dp.analytics", "port_name": "primary"}
                },
            ),
            governance_service=governance,
        )

    assert dp_service.output_calls
    assert dp_service.output_calls[0]["port_name"] == "primary"


def test_write_skips_registration_when_output_exists(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "data")
    contract = build_orders_contract(str(data_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService(registration_changed=False)
    df = spark.read.parquet(str(data_dir))

    result, status = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        path=str(data_dir),
        mode="overwrite",
        data_quality_service=dq_service,
        governance_service=governance,
        data_product_service=dp_service,
        data_product_output={"data_product": "dp.analytics", "port_name": "primary"},
        return_status=True,
    )

    assert result.ok
    assert status is not None
    assert dp_service.output_calls
    assert dp_service.output_calls[0]["port_name"] == "primary"


def test_write_with_governance_skips_output_registration(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path / "gov-output-existing")
    contract = build_orders_contract(str(data_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    dp_service = StubDataProductService(registration_changed=False)
    governance = build_local_governance_service(store, data_product_backend=dp_service)
    df = spark.read.parquet(str(data_dir))

    result, status = write_with_governance(
        df=df,
        request=_gov_write_request(
            contract,
            path=str(data_dir),
            format="parquet",
            mode="overwrite",
            context_overrides={
                "output_binding": {"data_product": "dp.analytics", "port_name": "primary"}
            },
        ),
        governance_service=governance,
        return_status=True,
    )

    assert result.ok
    assert status is not None
    assert dp_service.output_calls
    assert dp_service.output_calls[0]["port_name"] == "primary"


def test_data_product_pipeline_roundtrip(spark, tmp_path: Path) -> None:
    source_dir = materialise_orders(spark, tmp_path / "source")
    source_contract = build_orders_contract(str(source_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, source_contract)
    governance = build_local_governance_service(store)
    dp_service = StubDataProductService(
        contract_ref={"primary": (source_contract.id, source_contract.version)},
        registration_changed=False,
    )

    df_stage1, _ = read_from_data_product(
        spark,
        data_product_service=dp_service,
        data_product_input={
            "data_product": "dp.analytics",
            "source_data_product": "dp.analytics",
            "source_output_port": "primary",
        },
        contract_service=contract_service,
        data_quality_service=dq_service,
        governance_service=governance,
        return_status=True,
    )

    stage_dir = tmp_path / "stage"
    intermediate_contract = build_orders_contract(str(stage_dir))
    intermediate_contract.id = "dp.analytics.stage"
    store.put(intermediate_contract)

    write_with_contract(
        df=df_stage1,
        contract_id=intermediate_contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={intermediate_contract.version}",
        path=str(stage_dir),
        mode="overwrite",
        data_quality_service=dq_service,
    )

    stage_df, _ = read_with_contract(
        spark,
        contract_id=intermediate_contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={intermediate_contract.version}",
        data_quality_service=dq_service,
        governance_service=governance,
        return_status=True,
    )

    final_dir = tmp_path / "final"
    final_contract = build_orders_contract(str(final_dir))
    final_contract.id = "dp.analytics.final"
    store.put(final_contract)

    result = write_to_data_product(
        df=stage_df,
        data_product_service=dp_service,
        data_product_output={"data_product": "dp.analytics", "port_name": "primary"},
        contract_id=final_contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={final_contract.version}",
        path=str(final_dir),
        mode="overwrite",
        data_quality_service=dq_service,
        governance_service=governance,
    )

    assert result.ok
    assert dp_service.input_calls
    assert dp_service.output_calls


def test_data_product_pipeline_roundtrip_with_governance(spark, tmp_path: Path) -> None:
    source_dir = materialise_orders(spark, tmp_path / "gov-source")
    source_contract = build_orders_contract(str(source_dir))
    store, _, _ = persist_contract(tmp_path, source_contract)
    dp_service = StubDataProductService(
        contract_ref={"primary": (source_contract.id, source_contract.version)},
        registration_changed=False,
    )
    governance = build_local_governance_service(store, data_product_backend=dp_service)

    df_stage1, _ = read_with_governance(
        spark,
        _gov_read_request(
            None,
            context_overrides={
                "input_binding": {
                    "data_product": "dp.analytics",
                    "source_data_product": "dp.analytics",
                    "source_output_port": "primary",
                }
            },
            path=str(source_dir),
            format="parquet",
        ),
        governance_service=governance,
        return_status=True,
    )

    stage_dir = tmp_path / "gov-stage"
    intermediate_contract = build_orders_contract(str(stage_dir))
    intermediate_contract.id = "dp.analytics.stage"
    store.put(intermediate_contract)

    write_with_governance(
        df=df_stage1,
        request=_gov_write_request(
            intermediate_contract,
            path=str(stage_dir),
            format="parquet",
            mode="overwrite",
        ),
        governance_service=governance,
    )

    stage_df, _ = read_with_governance(
        spark,
        _gov_read_request(intermediate_contract),
        governance_service=governance,
        return_status=True,
    )

    final_dir = tmp_path / "gov-final"
    final_contract = build_orders_contract(str(final_dir))
    final_contract.id = "dp.analytics.final"
    store.put(final_contract)

    result = write_with_governance(
        df=stage_df,
        request=_gov_write_request(
            final_contract,
            path=str(final_dir),
            format="parquet",
            mode="overwrite",
            context_overrides={
                "output_binding": {"data_product": "dp.analytics", "port_name": "primary"}
            },
        ),
        governance_service=governance,
    )

    assert result.ok
    assert dp_service.input_calls
    assert dp_service.output_calls


def test_governance_service_persists_draft_context(spark, tmp_path: Path) -> None:
    dest_dir = tmp_path / "handles"
    contract = build_orders_contract(str(dest_dir))
    store, contract_service, dq_service = persist_contract(tmp_path, contract)

    # Missing the 'currency' column to trigger a draft proposal.
    data = [
        (1, 101, datetime(2024, 1, 1, 12, 0, 0), 25.0),
        (2, 102, datetime(2024, 1, 2, 15, 30, 0), 40.0),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount"],
    )

    governance = build_local_governance_service(store)
    locator = StaticDatasetLocator(dataset_version="handles-run")

    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=contract_service,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        data_quality_service=dq_service,
        governance_service=governance,
        dataset_locator=locator,
        pipeline_context={"job": "governance-bundle"},
    )

    assert not result.ok

    versions = [ver for ver in store.list_versions(contract.id) if ver != contract.version]
    assert versions
    draft_contract = store.get(contract.id, versions[0])
    properties = {
        prop.property: prop.value
        for prop in draft_contract.customProperties or []
    }
    context = properties.get("draft_context") or {}
    assert context.get("job") == "governance-bundle"
    assert context.get("io") == "write"
    assert context.get("dataset_version") == "handles-run"
    assert properties.get("draft_pipeline")


class _DummyLocator:
    def __init__(self, resolution: DatasetResolution) -> None:
        self._resolution = resolution

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        return self._resolution

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        return self._resolution


def test_contract_version_locator_sets_delta_version_option():
    base_resolution = DatasetResolution(
        path="/tmp/delta/orders",
        table=None,
        format="delta",
        dataset_id="orders",
        dataset_version=None,
    )
    locator = ContractVersionLocator(dataset_version="7", base=_DummyLocator(base_resolution))
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="delta",
        path=base_resolution.path,
        table=None,
    )
    assert merged.path == base_resolution.path
    assert merged.read_options == {"versionAsOf": "7"}


def test_contract_version_locator_timestamp_sets_delta_option():
    base_resolution = DatasetResolution(
        path="/tmp/delta/orders",
        table=None,
        format="delta",
        dataset_id="orders",
        dataset_version=None,
    )
    locator = ContractVersionLocator(
        dataset_version="2024-05-31T10:00:00Z",
        base=_DummyLocator(base_resolution),
    )
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="delta",
        path=base_resolution.path,
        table=None,
    )
    assert merged.read_options == {"timestampAsOf": "2024-05-31T10:00:00Z"}


def test_contract_version_locator_latest_skips_delta_option():
    base_resolution = DatasetResolution(
        path="/tmp/delta/orders",
        table=None,
        format="delta",
        dataset_id="orders",
        dataset_version=None,
    )
    locator = ContractVersionLocator(dataset_version="latest", base=_DummyLocator(base_resolution))
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="delta",
        path=base_resolution.path,
        table=None,
    )
    assert merged.read_options is None


def test_contract_version_locator_expands_versioning_paths(tmp_path: Path) -> None:
    base_dir = tmp_path / "orders"
    (base_dir / "2024-01-01").mkdir(parents=True)
    (base_dir / "2024-01-02").mkdir()
    for version in ("2024-01-01", "2024-01-02"):
        target = base_dir / version / "orders.json"
        target.write_text("[]", encoding="utf-8")

    resolution = DatasetResolution(
        path=str(base_dir),
        table=None,
        format="json",
        dataset_id="orders",
        dataset_version=None,
        custom_properties={
            "dc43.core.versioning": {
                "mode": "delta",
                "includePriorVersions": True,
                "subfolder": "{version}",
                "filePattern": "orders.json",
                "readOptions": {"recursiveFileLookup": True},
            }
        },
    )
    locator = ContractVersionLocator(dataset_version="2024-01-02", base=_DummyLocator(resolution))
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="json",
        path=str(base_dir),
        table=None,
    )
    assert merged.path == str(base_dir)
    assert merged.load_paths
    assert set(merged.load_paths) == {
        str(base_dir / "2024-01-01" / "orders.json"),
        str(base_dir / "2024-01-02" / "orders.json"),
    }
    assert merged.read_options and merged.read_options.get("recursiveFileLookup") == "true"


def test_contract_version_locator_snapshot_paths(tmp_path: Path) -> None:
    base_dir = tmp_path / "customers"
    (base_dir / "2024-01-01").mkdir(parents=True)
    (base_dir / "2024-02-01").mkdir()
    for version in ("2024-01-01", "2024-02-01"):
        target = base_dir / version / "customers.json"
        target.write_text("[]", encoding="utf-8")

    resolution = DatasetResolution(
        path=str(base_dir),
        table=None,
        format="json",
        dataset_id="customers",
        dataset_version=None,
        custom_properties={
            "dc43.core.versioning": {
                "mode": "snapshot",
                "includePriorVersions": False,
                "subfolder": "{version}",
                "filePattern": "customers.json",
            }
        },
    )
    locator = ContractVersionLocator(dataset_version="2024-02-01", base=_DummyLocator(resolution))
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="json",
        path=str(base_dir),
        table=None,
    )
    assert merged.load_paths == [str(base_dir / "2024-02-01" / "customers.json")]


def test_contract_version_locator_latest_respects_active_alias(tmp_path: Path) -> None:
    base_dir = tmp_path / "orders"
    versions = ["2023-12-31", "2024-01-01", "2025-09-28"]
    for version in versions:
        folder = base_dir / version
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "orders.json").write_text("[]", encoding="utf-8")
        (folder / ".dc43_version").write_text(version, encoding="utf-8")

    latest_target = base_dir / "2024-01-01"
    latest_link = base_dir / "latest"
    latest_link.symlink_to(latest_target)

    resolution = DatasetResolution(
        path=str(base_dir),
        table=None,
        format="json",
        dataset_id="orders",
        dataset_version=None,
        custom_properties={
            "dc43.core.versioning": {
                "mode": "delta",
                "includePriorVersions": True,
                "subfolder": "{version}",
                "filePattern": "orders.json",
            }
        },
    )

    locator = ContractVersionLocator(dataset_version="latest", base=_DummyLocator(resolution))
    merged = locator.for_read(
        contract=None,
        spark=None,
        format="json",
        path=str(base_dir),
        table=None,
    )

    assert merged.load_paths
    assert set(merged.load_paths) == {
        str(base_dir / "2023-12-31" / "orders.json"),
        str(base_dir / "2024-01-01" / "orders.json"),
    }


def test_read_write_with_governance_only(spark, tmp_path: Path) -> None:
    source_dir = materialise_orders(spark, tmp_path / "source")
    target_dir = tmp_path / "target"
    contract = build_orders_contract(str(target_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    df = spark.read.format("parquet").load(str(source_dir))
    result = write_with_governance(
        df=df,
        request=_gov_write_request(
            contract,
            path=str(target_dir),
            format="parquet",
            mode="overwrite",
        ),
        governance_service=governance,
        enforce=True,
        auto_cast=True,
    )

    assert result.ok

    read_df, status = read_with_governance(
        spark,
        _gov_read_request(contract, path=str(target_dir), format="parquet"),
        governance_service=governance,
        return_status=True,
    )

    assert read_df.count() == 2
    assert status is not None


def test_read_executor_applies_plan_data_product_status(spark) -> None:
    contract = _StubContract(id="sales.orders", version="1.0.0")
    binding = DataProductInputBinding(
        data_product="Sales.KPIs",
        port_name="orders",
        data_product_version="0.1.0-draft",
    )
    plan = ResolvedReadPlan(
        contract=contract,  # type: ignore[arg-type]
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="orders",
        dataset_version="2024-01-01",
        input_binding=binding,
        allowed_data_product_statuses=("active", "draft"),
    )
    executor = BatchReadExecutor(
        spark=spark,
        contract_id=None,
        contract_service=None,
        expected_contract_version=None,
        format=None,
        path=None,
        table=None,
        options=None,
        enforce=True,
        auto_cast=True,
        data_quality_service=None,
        governance_service=None,
        data_product_service=None,
        data_product_input=None,
        dataset_locator=None,
        status_strategy=None,
        pipeline_context=None,
        plan=plan,
    )
    product = OpenDataProductStandard(
        id="Sales.KPIs",
        status="draft",
        version="0.1.0-draft",
    )

    assert executor.status_handler.allowed_data_product_statuses == ("active", "draft")
    executor.status_handler.validate_data_product_status(
        data_product=product,
        enforce=executor.data_product_status_enforce,
        operation="read",
    )


def test_write_executor_applies_plan_data_product_status(spark) -> None:
    contract = _StubContract(id="sales.orders", version="1.0.0")
    binding = DataProductOutputBinding(
        data_product="Sales.KPIs",
        port_name="kpis.simple",
        data_product_version="0.1.0-draft",
    )
    plan = ResolvedWritePlan(
        contract=contract,  # type: ignore[arg-type]
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="sales.kpis",
        dataset_version="2024-01-01",
        output_binding=binding,
        allowed_data_product_statuses=("active", "draft"),
    )
    df = spark.createDataFrame([(1,)], ["value"])
    executor = BatchWriteExecutor(
        df=df,
        contract_id=None,
        contract_service=None,
        expected_contract_version=None,
        path=None,
        table=None,
        format=None,
        options=None,
        mode="append",
        enforce=True,
        auto_cast=True,
        data_quality_service=None,
        governance_service=None,
        data_product_service=None,
        data_product_output=None,
        dataset_locator=None,
        pipeline_context=None,
        violation_strategy=None,
        streaming_intervention_strategy=None,
        streaming_batch_callback=None,
        plan=plan,
    )
    product = OpenDataProductStandard(
        id="Sales.KPIs",
        status="draft",
        version="0.1.0-draft",
    )

    assert executor.strategy.allowed_data_product_statuses == ("active", "draft")
    executor.strategy.validate_data_product_status(
        data_product=product,
        enforce=executor.data_product_status_enforce,
        operation="write",
    )
