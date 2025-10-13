"""DLT-powered entry point for the demo pipeline scenarios."""

from __future__ import annotations

from typing import Any, MutableMapping

from pyspark.sql import DataFrame

from dc43_integrations.spark.dlt import contract_table
from dc43_integrations.spark.dlt_local import LocalDLTHarness

from . import pipeline


def _dlt_output_transform(
    df: DataFrame,
    context: MutableMapping[str, Any],
) -> DataFrame:
    """Route the final dataframe through a local DLT harness."""

    try:
        import dlt as databricks_dlt
    except Exception as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "databricks-dlt package is required to run the demo pipeline in DLT mode"
        ) from exc

    spark = context.get("spark")
    if spark is None:
        raise RuntimeError("Spark session missing from pipeline context")

    contract_id = context.get("contract_id")
    if not contract_id:
        raise ValueError("DLT mode requires an output contract id")

    expected_version = context.get("expected_contract_version")
    contract_service = context.get("contract_service")
    data_quality_service = context.get("data_quality_service")
    asset_name = str(context.get("dlt_asset_name") or context.get("dataset_name") or contract_id)

    context["pipeline_engine"] = "dlt"
    context["dlt_asset_name"] = asset_name

    with LocalDLTHarness(spark) as harness:

        @contract_table(
            databricks_dlt,
            name=asset_name,
            contract_id=contract_id,
            contract_service=contract_service,
            data_quality_service=data_quality_service,
            expected_contract_version=expected_version,
        )
        def _dlt_table() -> DataFrame:
            return df

        result_df = harness.run_asset(asset_name)

        context["dlt_expectation_reports"] = [
            {
                "asset": report.asset,
                "rule": report.rule,
                "predicate": report.predicate,
                "action": report.action,
                "failed_rows": report.failed_rows,
                "status": report.status,
            }
            for report in harness.expectation_reports
        ]
        options = harness.table_options.get(asset_name)
        if options:
            context["dlt_table_options"] = dict(options)

    return result_df


def run_dlt_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str | None,
    dataset_version: str | None,
    run_type: str,
    collect_examples: bool = False,
    examples_limit: int = 5,
    violation_strategy: pipeline.StrategySpec = None,
    enforce_contract_status: bool | None = None,
    inputs: dict[str, dict[str, Any]] | None = None,
    output_adjustment: str | None = None,
    data_product_flow: dict[str, Any] | None = None,
    *,
    scenario_key: str | None = None,
) -> tuple[str, str]:
    """Execute the demo pipeline while applying the output through DLT helpers."""

    return pipeline.run_pipeline(
        contract_id,
        contract_version,
        dataset_name,
        dataset_version,
        run_type,
        collect_examples=collect_examples,
        examples_limit=examples_limit,
        violation_strategy=violation_strategy,
        enforce_contract_status=enforce_contract_status,
        inputs=inputs,
        output_adjustment=output_adjustment,
        data_product_flow=data_product_flow,
        scenario_key=scenario_key,
        output_transform=_dlt_output_transform,
    )


__all__ = ["run_dlt_pipeline"]
