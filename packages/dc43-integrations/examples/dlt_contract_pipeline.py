"""Standalone DLT-style pipeline runnable on a local Spark session."""

from __future__ import annotations

import argparse
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from open_data_contract_standard.model import (
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_integrations.spark.dlt import contract_table
from dc43_integrations.spark.dlt_local import LocalDLTHarness, ensure_dlt_module
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.governance.storage.memory import InMemoryGovernanceStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance.client.local import LocalGovernanceServiceClient


def _build_contract(base_path: Path) -> OpenDataContractStandard:
    return OpenDataContractStandard(
        id="demo.orders",
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        name="Orders",
        description=Description(usage="Demo DLT pipeline"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                    SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                    SchemaProperty(
                        name="order_ts",
                        physicalType="timestamp",
                        required=True,
                    ),
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
        servers=[
            Server(
                server="local",
                type="filesystem",
                path=str(base_path),
                format="parquet",
            )
        ],
    )


def _create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("dc43-dlt-demo")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path to persist the validated table as Delta/Parquet",
    )
    args = parser.parse_args()

    spark = _create_spark()

    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract = _build_contract(root / "data")
            store = FSContractStore(str(root / "contracts"))
            store.put(contract)

            contract_service = LocalContractServiceClient(store)
            data_quality_service = LocalDataQualityServiceClient()
            governance_backend = LocalGovernanceServiceBackend(
                contract_client=contract_service,
                dq_client=data_quality_service,
                draft_store=store,
                store=InMemoryGovernanceStore(),
            )
            governance_service = LocalGovernanceServiceClient(governance_backend)
            bronze_rows = [
                (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
                (2, 102, datetime(2024, 1, 2, 10, 0, 0), -5.0, "EUR"),
                (3, 103, datetime(2024, 1, 3, 10, 0, 0), 12.5, "GBP"),
            ]
            columns = ["order_id", "customer_id", "order_ts", "amount", "currency"]
            bronze_df = spark.createDataFrame(bronze_rows, columns)

            dlt_module = ensure_dlt_module(allow_stub=True)

            with LocalDLTHarness(spark, module=dlt_module) as harness:

                @contract_table(
                    dlt_module,
                    context={
                        "contract": {
                            "contract_id": contract.id,
                            "contract_version": contract.version,
                        }
                    },
                    governance_service=governance_service,
                    name="orders",
                )
                def orders():
                    return bronze_df

                result = harness.run_asset("orders")

            print("\nValidated rows:")
            result.show(truncate=False)

            print("\nExpectation summary:")
            for report in harness.expectation_reports:
                print(
                    f"- {report.asset}::{report.rule} [{report.action}] "
                    f"status={report.status} failures={report.failed_rows}"
                )

            binding = getattr(orders, "__dc43_contract_binding__")
            print(
                f"\nBound contract: {binding.contract_id}@{binding.contract_version}"
            )

            if args.output:
                result.write.mode("overwrite").format("delta").save(str(args.output))
                print(f"\nWrote validated dataset to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover - manual entry point
    main()
