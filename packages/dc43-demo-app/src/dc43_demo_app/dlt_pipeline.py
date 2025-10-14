"""DLT-powered entry point for the demo pipeline scenarios."""

from __future__ import annotations

from typing import Any, Mapping, MutableMapping

from pyspark.sql import DataFrame

from .spark_compat import ensure_local_spark_builder

from dc43_integrations.spark.dlt import contract_table
from dc43_integrations.spark.dlt_local import LocalDLTHarness, ensure_dlt_module

from . import pipeline


def _tag_latest_record_as_dlt(
    scenario_key: str | None,
    *,
    module_name: str | None = None,
    module_stub: bool | None = None,
) -> None:
    """Ensure the most recent dataset record reflects a DLT execution."""

    try:
        records = pipeline.load_records()
    except Exception:  # pragma: no cover - defensive guard
        return
    if not records:
        return

    target = None
    if scenario_key:
        for record in reversed(records):
            if record.scenario_key == scenario_key:
                target = record
                break
    if target is None:
        target = records[-1]

    details = target.dq_details
    if not isinstance(details, dict):
        details = dict(details or {}) if isinstance(details, Mapping) else {}
        target.dq_details = details

    output = details.get("output") if isinstance(details, Mapping) else None
    if not isinstance(output, dict):
        output = dict(output or {}) if isinstance(output, Mapping) else {}
        details["output"] = output

    if output.get("pipeline_engine") != "dlt":
        output["pipeline_engine"] = "dlt"
    if module_name and not output.get("dlt_module_name"):
        output["dlt_module_name"] = module_name
    if module_stub is not None and "dlt_module_stub" not in output:
        output["dlt_module_stub"] = bool(module_stub)
    reports = output.setdefault("dlt_expectations", [])
    if not reports:
        contract_id = (target.contract_id or "").strip()
        contract_version = (target.contract_version or "").strip()
        try:
            contract_service = pipeline.contract_service
            dq_service = pipeline.dq_service
        except Exception:  # pragma: no cover - defensive
            contract_service = None
            dq_service = None
        if contract_id and contract_version and contract_service and dq_service:
            try:
                contract = contract_service.get(contract_id, contract_version)
            except Exception:  # pragma: no cover - defensive
                contract = None
            if contract is not None:
                try:
                    plan = dq_service.describe_expectations(contract=contract) or []
                except Exception:  # pragma: no cover - defensive
                    plan = []
                asset_label = target.dataset_name or contract_id or scenario_key or "asset"
                enriched: list[dict[str, Any]] = []
                for descriptor in plan:
                    if not isinstance(descriptor, Mapping):
                        continue
                    rule = descriptor.get("key")
                    predicate = descriptor.get("predicate")
                    if not isinstance(rule, str) or not isinstance(predicate, str):
                        continue
                    action = "drop" if not descriptor.get("optional") else "warn"
                    enriched.append(
                        {
                            "asset": asset_label,
                            "rule": rule,
                            "predicate": predicate,
                            "action": action,
                            "failed_rows": None,
                            "status": "pending",
                        }
                    )
                if enriched:
                    output["dlt_expectations"] = enriched

    pipeline.save_records(records)


def _dlt_output_transform(
    df: DataFrame,
    context: MutableMapping[str, Any],
) -> DataFrame:
    """Route the final dataframe through a local DLT harness."""

    databricks_dlt = ensure_dlt_module(allow_stub=True)
    context["dlt_module_name"] = getattr(databricks_dlt, "__name__", "dlt")
    context["dlt_module_stub"] = bool(getattr(databricks_dlt, "__dc43_is_stub__", False))

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

    ensure_local_spark_builder()
    module = ensure_dlt_module(allow_stub=True)
    module_name = getattr(module, "__name__", "dlt")
    module_stub = bool(getattr(module, "__dc43_is_stub__", False))

    try:
        result = pipeline.run_pipeline(
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
    except Exception:
        _tag_latest_record_as_dlt(
            scenario_key,
            module_name=module_name,
            module_stub=module_stub,
        )
        raise

    _tag_latest_record_as_dlt(
        scenario_key,
        module_name=module_name,
        module_stub=module_stub,
    )
    return result


__all__ = ["run_dlt_pipeline"]
