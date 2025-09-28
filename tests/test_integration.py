from pathlib import Path
from typing import Optional, Tuple

import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
    Server,
)

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_integrations.spark.io import (
    read_with_contract,
    write_with_contract,
    StaticDatasetLocator,
    ContractVersionLocator,
    DatasetResolution,
)
from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance import build_local_governance_service
from datetime import datetime
import logging


def make_contract(base_path: str, fmt: str = "parquet") -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                    SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                    SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                    SchemaProperty(
                        name="amount",
                        physicalType="double",
                        required=True,
                        quality=[DataQuality(mustBeGreaterThan=0.0)],
                    ),
                    SchemaProperty(
                        name="currency",
                        physicalType="string",
                        required=True,
                        quality=[DataQuality(rule="enum", mustBe=["EUR", "USD"])],
                    ),
                ],
            )
        ],
        servers=[Server(server="local", type="filesystem", path=base_path, format=fmt)],
    )


def persist_contract(
    tmp_path: Path, contract: OpenDataContractStandard
) -> Tuple[FSContractStore, LocalContractServiceClient, LocalDataQualityServiceClient]:
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return store, LocalContractServiceClient(store), LocalDataQualityServiceClient()


def test_dq_integration_blocks(spark, tmp_path: Path) -> None:
    data_dir = tmp_path / "parquet"
    contract = make_contract(str(data_dir))
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
    contract = make_contract(str(dest_dir))
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
    contract = make_contract(str(dest_dir))
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
    contract = make_contract(str(expected_dir))
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
    contract = make_contract(str(contract_path))
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
    contract = make_contract(str(data_dir), fmt="parquet")
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
    contract = make_contract(str(dest_dir), fmt="parquet")
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
    contract = make_contract(str(base_dir))
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
    contract = make_contract(str(dest_dir))
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
    contract_v1 = make_contract(str(dest_dir))
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


def test_governance_service_persists_draft_context(spark, tmp_path: Path) -> None:
    dest_dir = tmp_path / "handles"
    contract = make_contract(str(dest_dir))
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
            "dc43.versioning": {
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
            "dc43.versioning": {
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
            "dc43.versioning": {
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
