"""DLT code snippets displayed in the demo scenario guides."""

from __future__ import annotations

from textwrap import dedent


def _orders_enriched_base_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.0.0",
            comment="Join curated orders with customers before publishing.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2025-10-05__pinned"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            return orders.join(customers, "customer_id", "left")


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _dq_failure_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession, functions as F

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Deliberately break the amount expectation to surface a blocking verdict.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            df = orders.join(customers, "customer_id", "left")
            return df.withColumn("amount", F.col("amount") - F.lit(120))


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _schema_and_dq_failure_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession, functions as F

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==2.0.0",
            comment="Drop columns and shrink amounts to trigger schema plus DQ drift.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            df = orders.join(customers, "customer_id", "left")
            df = df.drop("customer_id")
            return df.withColumn("amount", F.col("amount") - F.lit(200))


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _draft_guard_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table


        def ensure_active_contract(contract_id: str, version: str) -> None:
            contract = contract_service.get(contract_id, version)
            status = (getattr(contract, "status", "") or "").lower()
            if status != "active":
                raise RuntimeError(
                    f"{contract_id}:{version} must be promoted to 'active' before enforcement",
                )


        ensure_active_contract("orders_enriched", "3.0.0")


        @contract_table(
            dlt,
            name="orders_enriched_draft",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==3.0.0",
            comment="Execution stops before this table while the contract remains in draft.",
        )
        def orders_enriched_draft():
            raise RuntimeError("unreachable: contract status guard aborts earlier")
        """
    ).strip()


def _draft_override_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession, functions as F

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==3.0.0",
            comment="Test draft contracts with curated inputs while documenting overrides.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(
                    dataset_id="orders::valid",
                    dataset_version="latest__valid",
                ),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            df = orders.join(customers, "customer_id", "left")
            df = df.withColumn(
                "amount",
                F.when(F.col("amount") < 150, F.lit(150)).otherwise(F.col("amount")),
            )
            if "customer_segment" not in df.columns:
                df = df.withColumn("customer_segment", F.lit("loyalty_pilot"))
            return df


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _read_latest_blocked_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            ContractFirstDatasetLocator,
            ContractVersionLocator,
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Force the latest orders slice through validation to surface a block.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=ContractVersionLocator(
                    dataset_version="latest",
                    base=ContractFirstDatasetLocator(),
                ),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            return orders.join(customers, "customer_id", "left")


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _read_valid_subset_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Consume the curated valid orders subset while observing outcomes.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(
                    dataset_id="orders::valid",
                    dataset_version="latest__valid",
                ),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            return orders.join(customers, "customer_id", "left")


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _valid_subset_violation_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession, functions as F

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Start from the curated subset but degrade rows to record violations.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(
                    dataset_id="orders::valid",
                    dataset_version="latest__valid",
                ),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            df = orders.join(customers, "customer_id", "left")
            return df.withColumn(
                "amount",
                F.when(F.col("order_id") == 3, F.col("amount") / 2).otherwise(F.col("amount")),
            )


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _data_product_roundtrip_snippet() -> str:
    return dedent(
        """
        import dlt
        from pyspark.sql import SparkSession, functions as F

        from dc43_demo_app.contracts_api import contract_service, data_product_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
            read_from_data_product,
        )


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Join the upstream data product feed with customers before publishing the governed table.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_data_product(
                spark,
                data_product_service=data_product_service,
                data_product_input={
                    "data_product": "dp.orders",
                    "port_name": "orders-latest",
                    "source_data_product": "dp.orders",
                    "source_output_port": "orders-latest",
                },
                contract_service=contract_service,
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="latest"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            df = orders.join(customers, "customer_id", "left")
            df = df.withColumn(
                "amount",
                F.when(F.col("amount") < 150, F.lit(150)).otherwise(F.col("amount")),
            )
            if "customer_segment" not in df.columns:
                df = df.withColumn("customer_segment", F.lit("loyalty_pilot"))
            return df


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _read_override_full_snippet() -> str:
    return dedent(
        """
        import dlt
        from dataclasses import dataclass
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            ReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
        )
        from dc43_service_clients.data_quality import ValidationResult


        @dataclass
        class DowngradeBlockingReadStrategy(ReadStatusStrategy):
            note: str
            target_status: str = "warn"

            def apply(self, *, dataframe, status, enforce, context):  # type: ignore[override]
                if status and status.status == "block":
                    details = dict(status.details)
                    overrides = list(details.get("overrides", []))
                    overrides.append(self.note)
                    details["overrides"] = overrides
                    details["status_before_override"] = status.status
                    return dataframe, ValidationResult(
                        status=self.target_status,
                        reason=status.reason,
                        details=details,
                    )
                return dataframe, status


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Force a blocked slice through the pipeline while recording the override.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            override = DowngradeBlockingReadStrategy(
                note="Manual override: forced latest slice (→2025-09-28)",
                target_status="warn",
            )
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="latest"),
                status_strategy=override,
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            return orders.join(customers, "customer_id", "left")


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                harness.run_asset("orders_enriched")
        """
    ).strip()


def _split_lenient_snippet() -> str:
    return dedent(
        """
        from datetime import datetime, timezone

        import dlt
        from pyspark.sql import SparkSession

        from dc43_demo_app.contracts_api import contract_service, dq_service
        from dc43_integrations.spark.dlt import contract_table
        from dc43_integrations.spark.dlt_local import LocalDLTHarness
        from dc43_integrations.spark.io import (
            DefaultReadStatusStrategy,
            StaticDatasetLocator,
            read_from_contract,
            write_with_contract_id,
        )
        from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy


        @contract_table(
            dlt,
            name="orders_enriched",
            contract_id="orders_enriched",
            contract_service=contract_service,
            data_quality_service=dq_service,
            expected_contract_version="==1.1.0",
            comment="Prepare the joined dataset; the write step will route rejects via the split strategy.",
        )
        def orders_enriched():
            spark = SparkSession.getActiveSession()
            orders = read_from_contract(
                spark,
                contract_id="orders",
                contract_service=contract_service,
                expected_contract_version="1.1.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            customers = read_from_contract(
                spark,
                contract_id="customers",
                contract_service=contract_service,
                expected_contract_version="1.0.0",
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
                status_strategy=DefaultReadStatusStrategy(),
                return_status=False,
            )
            return orders.join(customers, "customer_id", "left")


        if __name__ == "__main__":
            spark = SparkSession.builder.appName("demo-dlt").getOrCreate()
            with LocalDLTHarness(spark) as harness:
                enriched_df = harness.run_asset("orders_enriched")

            strategy = SplitWriteViolationStrategy(
                include_valid=True,
                include_reject=True,
                write_primary_on_violation=True,
            )
            version = datetime.now(timezone.utc).isoformat()
            result = write_with_contract_id(
                df=enriched_df,
                contract_id="orders_enriched",
                expected_contract_version="==1.1.0",
                contract_service=contract_service,
                data_quality_service=dq_service,
                dataset_locator=StaticDatasetLocator(dataset_version=version),
                violation_strategy=strategy,
                enforce=False,
            )
            print(f"Split strategy finished with status {result.status} for {version}")
        """
    ).strip()


_SNIPPETS = {
    "ok": _orders_enriched_base_snippet(),
    "dq": _dq_failure_snippet(),
    "schema-dq": _schema_and_dq_failure_snippet(),
    "contract-draft-block": _draft_guard_snippet(),
    "contract-draft-override": _draft_override_snippet(),
    "read-invalid-block": _read_latest_blocked_snippet(),
    "read-valid-subset": _read_valid_subset_snippet(),
    "read-valid-subset-violation": _valid_subset_violation_snippet(),
    "data-product-roundtrip": _data_product_roundtrip_snippet(),
    "read-override-full": _read_override_full_snippet(),
    "split-lenient": _split_lenient_snippet(),
}

_ALIASES = {
    "ok-dlt": "ok",
}


def get_dlt_snippet(scenario_key: str) -> str | None:
    """Return the rendered DLT snippet for ``scenario_key`` when available."""

    key = _ALIASES.get(scenario_key, scenario_key)
    return _SNIPPETS.get(key)


__all__ = ["get_dlt_snippet"]
