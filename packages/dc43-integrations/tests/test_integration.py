from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

import pytest

from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_integrations.spark.io import (
    read_from_data_product,
    read_with_contract,
    write_to_data_product,
    write_with_contract,
    StaticDatasetLocator,
    ContractVersionLocator,
    DatasetResolution,
    DefaultReadStatusStrategy,
)
from dc43_integrations.spark.violation_strategy import (
    SplitWriteViolationStrategy,
    NoOpWriteViolationStrategy,
)
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance import build_local_governance_service
from dc43_service_backends.data_products import DataProductRegistrationResult
from dc43_service_backends.core.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard as DataProductDoc,
)
from .helpers.orders import build_orders_contract, materialise_orders
from datetime import datetime
import logging


def persist_contract(
    tmp_path: Path, contract: OpenDataContractStandard
) -> Tuple[FSContractStore, LocalContractServiceClient, LocalDataQualityServiceClient]:
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return store, LocalContractServiceClient(store), LocalDataQualityServiceClient()


class StubDataProductService:
    def __init__(
        self,
        contract_ref: tuple[str, str] | Mapping[str, tuple[str, str]] | None = None,
        *,
        registration_changed: bool = True,
    ) -> None:
        self.contract_ref = contract_ref
        self.input_calls: list[dict[str, Any]] = []
        self.output_calls: list[dict[str, Any]] = []
        self.registration_changed = registration_changed

    def get(self, data_product_id: str, version: str) -> DataProductDoc:
        raise NotImplementedError

    def latest(self, data_product_id: str) -> Optional[DataProductDoc]:  # pragma: no cover - not used
        return None

    def list_versions(self, data_product_id: str) -> list[str]:  # pragma: no cover - not used
        return []

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
        status = "draft" if self.registration_changed else "active"
        doc = DataProductDoc(id=data_product_id, status=status, version="0.1.0-draft")
        doc.input_ports.append(
            DataProductInputPort(name=port_name, version=contract_version, contract_id=contract_id)
        )
        return DataProductRegistrationResult(product=doc, changed=self.registration_changed)

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
        status = "draft" if self.registration_changed else "active"
        doc = DataProductDoc(id=data_product_id, status=status, version="0.1.0-draft")
        doc.output_ports.append(
            DataProductOutputPort(name=port_name, version=contract_version, contract_id=contract_id)
        )
        return DataProductRegistrationResult(product=doc, changed=self.registration_changed)

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        if isinstance(self.contract_ref, Mapping):
            return self.contract_ref.get(port_name)
        return self.contract_ref


def test_read_blocks_on_draft_contract_status(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path)
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
    data_dir = materialise_orders(spark, tmp_path)
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


def test_read_registers_data_product_input_port(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path)
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
    data_dir = materialise_orders(spark, tmp_path)
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
    data_dir = materialise_orders(spark, tmp_path)
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


def test_write_registers_data_product_output_port(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path)
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


def test_write_skips_registration_when_output_exists(spark, tmp_path: Path) -> None:
    data_dir = materialise_orders(spark, tmp_path)
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


def test_data_product_pipeline_roundtrip(spark, tmp_path: Path) -> None:
    source_dir = materialise_orders(spark, tmp_path)
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
