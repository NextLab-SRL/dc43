from __future__ import annotations

"""Example transformation pipeline using dc43 helpers.

This script demonstrates how a Spark job might read data with contract
validation, perform transformations (omitted) and write the result while
recording the dataset version in the demo app's registry.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

from dc43.demo_app.server import (
    store,
    DATASETS_FILE,
    DATA_DIR,
    DatasetRecord,
    load_records,
    save_records,
    set_active_version,
    register_dataset_version,
)
from dc43.components.data_quality import DataQualityManager
from dc43.components.data_quality.governance import DQStatus
from dc43.components.data_quality.integration import attach_failed_expectations
from dc43.components.data_quality.governance.stubs import StubDQClient
from dc43.components.integration.spark_io import (
    ContractFirstDatasetLocator,
    ContractVersionLocator,
    ReadStatusContext,
    ReadStatusStrategy,
    StaticDatasetLocator,
    read_with_contract,
    write_with_contract,
)
from dc43.components.integration.violation_strategy import (
    NoOpWriteViolationStrategy,
    SplitWriteViolationStrategy,
    StrictWriteViolationStrategy,
    WriteViolationStrategy,
)
from open_data_contract_standard.model import OpenDataContractStandard
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def _next_version(existing: list[str]) -> str:
    """Return a new ISO-8601 timestamp not present in ``existing``."""

    used = set(existing)
    offset = 0
    while True:
        candidate = (datetime.now(timezone.utc) + timedelta(seconds=offset)).isoformat()
        candidate = candidate.replace("+00:00", "Z")
        if candidate not in used:
            return candidate
        offset += 1


def _resolve_output_path(
    contract: OpenDataContractStandard | None,
    dataset_name: str,
    dataset_version: str,
) -> Path:
    """Return output path for dataset relative to contract servers."""
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(DATA_DIR).parent
    server_path = Path(getattr(server, "path", "")) if server else data_root
    if server_path.suffix:
        base_path = server_path.parent / server_path.stem
    else:
        base_path = server_path
    if not base_path.is_absolute():
        base_path = data_root / base_path
    if base_path.name == dataset_name:
        out = base_path / dataset_version
    else:
        out = base_path / dataset_name / dataset_version
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


StrategySpec = WriteViolationStrategy | str | Mapping[str, Any] | None


class _DowngradeBlockingReadStrategy:
    """Interpret blocking read statuses as warnings while annotating details."""

    def __init__(self, *, note: str, target_status: str = "warn") -> None:
        self.note = note
        self.target_status = target_status

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: DQStatus | None,
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, DQStatus | None]:
        if status and status.status == "block":
            details = dict(status.details or {})
            notes = list(details.get("overrides", []))
            notes.append(self.note)
            details["overrides"] = notes
            details.setdefault("status_before_override", status.status)
            return dataframe, DQStatus(
                status=self.target_status,
                reason=status.reason,
                details=details,
            )
        return dataframe, status


ReadStrategySpec = ReadStatusStrategy | str | Mapping[str, Any] | None


def _resolve_violation_strategy(spec: StrategySpec) -> WriteViolationStrategy | None:
    """Return a concrete violation strategy based on ``spec``."""

    if spec is None:
        return None

    if hasattr(spec, "plan"):
        return spec  # type: ignore[return-value]

    name: str
    options: MutableMapping[str, Any]
    if isinstance(spec, str):
        name = spec
        options = {}
    elif isinstance(spec, Mapping):
        opt_map: MutableMapping[str, Any] = dict(spec)
        name = str(
            opt_map.pop("name", None)
            or opt_map.pop("strategy", None)
            or opt_map.pop("type", None)
            or ""
        )
        options = opt_map
    else:  # pragma: no cover - defensive guard for unexpected inputs
        raise TypeError(f"Unsupported violation strategy spec: {spec!r}")

    key = name.lower()
    if key in {"noop", "default", "none"}:
        return NoOpWriteViolationStrategy()
    if key in {"split", "split-datasets", "split_datasets"}:
        allowed: Sequence[str] = (
            "valid_suffix",
            "reject_suffix",
            "include_valid",
            "include_reject",
            "write_primary_on_violation",
            "dataset_suffix_separator",
        )
        filtered = {k: options[k] for k in allowed if k in options}
        return SplitWriteViolationStrategy(**filtered)
    if key in {"split-strict", "strict-split", "split_strict"}:
        allowed: Sequence[str] = (
            "valid_suffix",
            "reject_suffix",
            "include_valid",
            "include_reject",
            "write_primary_on_violation",
            "dataset_suffix_separator",
        )
        failure_message = str(
            options.pop(
                "failure_message",
                StrictWriteViolationStrategy.failure_message,
            )
        )
        fail_on_warnings = bool(options.pop("fail_on_warnings", False))
        base_options = {k: options.pop(k) for k in allowed if k in options}
        base = SplitWriteViolationStrategy(**base_options)
        return StrictWriteViolationStrategy(
            base=base,
            failure_message=failure_message,
            fail_on_warnings=fail_on_warnings,
        )
    if key in {"strict", "fail", "error"}:
        failure_message = str(
            options.pop(
                "failure_message",
                StrictWriteViolationStrategy.failure_message,
            )
        )
        fail_on_warnings = bool(options.pop("fail_on_warnings", False))
        return StrictWriteViolationStrategy(
            failure_message=failure_message,
            fail_on_warnings=fail_on_warnings,
        )

    raise ValueError(f"Unknown violation strategy: {name}")


def _resolve_read_status_strategy(spec: ReadStrategySpec) -> ReadStatusStrategy | None:
    """Return a read status strategy instance for the supplied spec."""

    if spec is None:
        return None

    if hasattr(spec, "apply"):
        return spec  # type: ignore[return-value]

    name: str
    options: MutableMapping[str, Any]
    if isinstance(spec, str):
        name = spec
        options = {}
    elif isinstance(spec, Mapping):
        opt_map: MutableMapping[str, Any] = dict(spec)
        name = str(
            opt_map.pop("name", None)
            or opt_map.pop("strategy", None)
            or opt_map.pop("type", None)
            or ""
        )
        options = opt_map
    else:  # pragma: no cover - defensive guard
        raise TypeError(f"Unsupported read status strategy spec: {spec!r}")

    key = name.lower()
    if key in {"default", "none", "pass", "passthrough"}:
        return None
    if key in {"allow", "allow-block", "downgrade"}:
        note = str(
            options.pop(
                "note",
                "Blocked dataset accepted for downstream processing",
            )
        )
        target = str(options.pop("target_status", "warn"))
        return _DowngradeBlockingReadStrategy(note=note, target_status=target)

    raise ValueError(f"Unknown read status strategy: {name}")


def _apply_locator_overrides(
    default: ContractVersionLocator | StaticDatasetLocator,
    overrides: Mapping[str, Any] | None,
) -> ContractVersionLocator | StaticDatasetLocator:
    """Return a locator with overrides merged onto ``default``."""

    if overrides is None:
        return default

    locator_candidate = overrides.get("dataset_locator") if isinstance(overrides, Mapping) else None
    if locator_candidate is not None and hasattr(locator_candidate, "for_read"):
        return locator_candidate  # type: ignore[return-value]

    dataset_id = overrides.get("dataset_id") if isinstance(overrides, Mapping) else None
    dataset_version = overrides.get("dataset_version") if isinstance(overrides, Mapping) else None
    subpath = overrides.get("subpath") if isinstance(overrides, Mapping) else None

    base_strategy = getattr(default, "base", ContractFirstDatasetLocator())  # type: ignore[arg-type]
    if isinstance(overrides, Mapping) and overrides.get("base") is not None:
        candidate = overrides["base"]
        if hasattr(candidate, "for_read"):
            base_strategy = candidate  # type: ignore[assignment]
        else:  # pragma: no cover - defensive guard for unexpected inputs
            raise TypeError(f"Unsupported base locator: {candidate!r}")

    if isinstance(default, ContractVersionLocator):
        return ContractVersionLocator(
            dataset_version=dataset_version or default.dataset_version,
            dataset_id=dataset_id or default.dataset_id,
            subpath=subpath or default.subpath,
            base=base_strategy,
        )

    params = {
        "dataset_id": dataset_id or default.dataset_id,
        "dataset_version": dataset_version or default.dataset_version,
        "path": overrides.get("path", default.path) if isinstance(overrides, Mapping) else default.path,
        "table": overrides.get("table", default.table) if isinstance(overrides, Mapping) else default.table,
        "format": overrides.get("format", default.format) if isinstance(overrides, Mapping) else default.format,
    }

    return StaticDatasetLocator(base=base_strategy, **params)


def _apply_output_adjustment(
    df: DataFrame,
    adjustment: str | None,
) -> tuple[DataFrame, list[str]]:
    """Apply scenario-specific output adjustments and describe them."""

    if not adjustment:
        return df, []

    key = adjustment.lower()
    notes: list[str] = []

    if key in {"valid-subset-violation", "degrade-valid", "valid_subset_violation"}:
        notes.append("downgraded order 3 amount to illustrate post-join violations")
        df = df.withColumn(
            "amount",
            when(col("order_id") == 3, col("amount") / 2).otherwise(col("amount")),
        )
        return df, notes

    if key in {"amplify-negative", "full-batch-violation", "amplify_negative"}:
        notes.append("preserved negative input amounts to surface contract breach")
        # Ensure the negative row propagates; keep identity transformation.
        return df, notes

    return df, []


def run_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str | None,
    dataset_version: str | None,
    run_type: str,
    collect_examples: bool = False,
    examples_limit: int = 5,
    violation_strategy: StrategySpec = None,
    inputs: Mapping[str, Mapping[str, Any]] | None = None,
    output_adjustment: str | None = None,
) -> tuple[str, str]:
    """Run an example pipeline using the stored contract.

    When an output contract is supplied the dataset name is derived from the
    contract identifier so the recorded runs and filesystem layout match the
    declared server path.  Callers may supply a custom name when no contract is
    available.  The ``inputs`` mapping can override dataset locators, enforce
    flags, and read-status strategies for each source (``"orders"`` and
    ``"customers"``) so demo scenarios can highlight how mixed-validity inputs
    are handled.  ``output_adjustment`` optionally tweaks the joined dataframe
    (for example to deliberately surface violations). Returns the dataset name
    used along with the materialized version.
    """
    existing_session = SparkSession.getActiveSession()
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    dq_client = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))
    dq = DataQualityManager(dq_client, draft_store=store)

    input_overrides: Mapping[str, Mapping[str, Any]] = inputs or {}

    orders_overrides = input_overrides.get("orders")
    orders_locator = _apply_locator_overrides(
        ContractVersionLocator(
            dataset_version="latest",
            base=ContractFirstDatasetLocator(),
        ),
        orders_overrides,
    )
    orders_strategy = _resolve_read_status_strategy(
        orders_overrides.get("status_strategy") if orders_overrides else None
    )
    orders_enforce = bool(
        orders_overrides.get("enforce", True) if orders_overrides else True
    )

    # Read primary orders dataset with its contract
    orders_df, orders_status = read_with_contract(
        spark,
        contract_id="orders",
        contract_store=store,
        expected_contract_version="==1.1.0",
        dq_client=dq,
        dataset_locator=orders_locator,
        status_strategy=orders_strategy,
        enforce=orders_enforce,
    )

    customers_overrides = input_overrides.get("customers")
    customers_locator = _apply_locator_overrides(
        ContractVersionLocator(
            dataset_version="latest",
            base=ContractFirstDatasetLocator(),
        ),
        customers_overrides,
    )
    customers_strategy = _resolve_read_status_strategy(
        customers_overrides.get("status_strategy") if customers_overrides else None
    )
    customers_enforce = bool(
        customers_overrides.get("enforce", True) if customers_overrides else True
    )

    # Join with customers lookup dataset
    customers_df, customers_status = read_with_contract(
        spark,
        contract_id="customers",
        contract_store=store,
        expected_contract_version="==1.0.0",
        dq_client=dq,
        dataset_locator=customers_locator,
        status_strategy=customers_strategy,
        enforce=customers_enforce,
    )

    df = orders_df.join(customers_df, "customer_id")
    # Promote one of the rows above the quality threshold so split strategies
    # demonstrate both valid and reject outputs in the demo.
    df = df.withColumn(
        "amount",
        when(col("order_id") == 1, col("amount") * 20).otherwise(col("amount")),
    )

    df, adjustment_notes = _apply_output_adjustment(df, output_adjustment)

    records = load_records()
    output_contract = (
        store.get(contract_id, contract_version) if contract_id and contract_version else None
    )
    if output_contract and getattr(output_contract, "id", None):
        # Align dataset naming with the contract so recorded versions and paths
        # remain consistent with the declared server definition.
        dataset_name = output_contract.id
    elif not dataset_name:
        dataset_name = contract_id or "result"
    if not dataset_version:
        existing = [r.dataset_version for r in records if r.dataset_name == dataset_name]
        dataset_version = _next_version(existing)

    assert dataset_name
    assert dataset_version
    output_path = _resolve_output_path(output_contract, dataset_name, dataset_version)
    server = (output_contract.servers or [None])[0] if output_contract else None

    strategy = _resolve_violation_strategy(violation_strategy)

    if output_contract:
        locator = ContractVersionLocator(
            dataset_version=dataset_version,
            base=ContractFirstDatasetLocator(),
        )
    else:
        locator = StaticDatasetLocator(
            dataset_id=dataset_name,
            dataset_version=dataset_version,
            path=str(output_path),
        )
    contract_id_ref = getattr(output_contract, "id", None)
    expected_version = f"=={output_contract.version}" if output_contract else None
    result, output_status = write_with_contract(
        df=df,
        contract_id=contract_id_ref,
        contract_store=store if contract_id_ref else None,
        path=None if contract_id_ref else str(output_path),
        format=None if contract_id_ref else getattr(server, "format", "parquet"),
        mode="overwrite",
        enforce=False,
        dq_client=dq,
        dataset_locator=locator,
        expected_contract_version=expected_version,
        return_status=True,
        violation_strategy=strategy,
    )

    if output_status and output_contract:
        output_status = attach_failed_expectations(output_contract, output_status)

    error: ValueError | None = None
    if run_type == "enforce":
        if not output_contract:
            error = ValueError("Contract required for existing mode")
        else:
            issues: list[str] = []
            if output_status and output_status.status != "ok":
                detail_msg: dict[str, Any] = dict(output_status.details or {})
                if output_status.reason:
                    detail_msg["reason"] = output_status.reason
                issues.append(
                    f"DQ violation: {detail_msg or output_status.status}"
                )
            if not result.ok:
                issues.append(
                    f"Schema validation failed: {result.errors}"
                )
            if issues:
                error = ValueError("; ".join(issues))

    draft_version: str | None = None
    output_details = result.details.copy()
    if adjustment_notes:
        extra = output_details.setdefault("transformations", [])
        if isinstance(extra, list):
            extra.extend(adjustment_notes)
        else:
            output_details["transformations"] = adjustment_notes
    if strategy is not None:
        output_details.setdefault("violation_strategy", type(strategy).__name__)
        if isinstance(strategy, SplitWriteViolationStrategy):
            output_details.setdefault(
                "violation_strategy_options",
                {
                    "valid_suffix": strategy.valid_suffix,
                    "reject_suffix": strategy.reject_suffix,
                    "include_valid": strategy.include_valid,
                    "include_reject": strategy.include_reject,
                    "write_primary_on_violation": strategy.write_primary_on_violation,
                    "dataset_suffix_separator": strategy.dataset_suffix_separator,
                },
            )
            aux: list[dict[str, str]] = []
            if dataset_name:
                base_id = dataset_name
                base_path = Path(str(output_path))
                server_path_hint = Path(getattr(server, "path", "")) if server else None
                server_filename = (
                    server_path_hint.name
                    if server_path_hint and server_path_hint.suffix
                    else None
                )
                if strategy.include_valid:
                    valid_dir = base_path / strategy.valid_suffix
                    if server_filename:
                        valid_dir = base_path / server_filename / strategy.valid_suffix
                    aux.append(
                        {
                            "kind": "valid",
                            "dataset": f"{base_id}{strategy.dataset_suffix_separator}{strategy.valid_suffix}",
                            "path": str(valid_dir),
                        }
                    )
                if strategy.include_reject:
                    reject_dir = base_path / strategy.reject_suffix
                    if server_filename:
                        reject_dir = base_path / server_filename / strategy.reject_suffix
                    aux.append(
                        {
                            "kind": "reject",
                            "dataset": f"{base_id}{strategy.dataset_suffix_separator}{strategy.reject_suffix}",
                            "path": str(reject_dir),
                        }
                    )
            if aux:
                output_details.setdefault("auxiliary_datasets", aux)

    if dataset_name and dataset_version:
        try:
            set_active_version(dataset_name, dataset_version)
        except FileNotFoundError:
            pass
        else:
            for aux in output_details.get("auxiliary_datasets", []):
                dataset_ref = aux.get("dataset") if isinstance(aux, Mapping) else None
                path_ref = aux.get("path") if isinstance(aux, Mapping) else None
                if not dataset_ref or not path_ref:
                    continue
                alias = dataset_ref.replace("::", "__")
                try:
                    register_dataset_version(alias, dataset_version, Path(path_ref))
                    set_active_version(alias, dataset_version)
                except FileNotFoundError:
                    continue

    dq_payload: dict[str, Any] = {}
    if output_status:
        dq_payload = dict(output_status.details or {})
        dq_payload.setdefault("status", output_status.status)
        if output_status.reason:
            dq_payload.setdefault("reason", output_status.reason)

        dq_metrics = dq_payload.get("metrics", {})
        if dq_metrics:
            merged_metrics = {**dq_metrics, **output_details.get("metrics", {})}
            output_details["metrics"] = merged_metrics
        if "violations" in dq_payload:
            output_details["violations"] = dq_payload["violations"]
        if "failed_expectations" in dq_payload:
            output_details["failed_expectations"] = dq_payload["failed_expectations"]
        aux_statuses = dq_payload.get("auxiliary_statuses", [])
        if aux_statuses:
            output_details.setdefault("dq_auxiliary_statuses", aux_statuses)

        summary = dict(output_details.get("dq_status", {}))
        summary.setdefault("status", dq_payload.get("status", output_status.status))
        if dq_payload.get("reason"):
            summary.setdefault("reason", dq_payload["reason"])
        extras = {
            k: v
            for k, v in dq_payload.items()
            if k
            not in ("metrics", "violations", "failed_expectations", "status", "reason")
        }
        if extras:
            summary.update(extras)
        if summary:
            output_details["dq_status"] = summary

    draft_version = output_details.get("draft_contract_version")
    if not draft_version and dq_payload:
        draft_version = dq_payload.get("draft_contract_version")
    if not draft_version:
        for aux_status in output_details.get("dq_auxiliary_statuses", []) or []:
            details = aux_status.get("details") if isinstance(aux_status, dict) else None
            if isinstance(details, dict):
                candidate = details.get("draft_contract_version")
                if candidate:
                    draft_version = candidate
                    break
    if draft_version:
        output_details.setdefault("draft_contract_version", draft_version)

    combined_details = {
        "orders": orders_status.details if orders_status else None,
        "customers": customers_status.details if customers_status else None,
        "output": output_details,
    }
    total_violations = 0
    warnings_present = False
    for det in combined_details.values():
        if not det or not isinstance(det, dict):
            continue
        violations_value = det.get("violations")
        if isinstance(violations_value, (int, float)):
            total_violations += int(violations_value)
            if violations_value:
                warnings_present = True
        else:
            metrics_map = det.get("metrics", {})
            if isinstance(metrics_map, Mapping):
                for key, value in metrics_map.items():
                    if key.startswith("violations.") and isinstance(value, (int, float)):
                        total_violations += int(value)
                        if value:
                            warnings_present = True
        errs = det.get("errors")
        if isinstance(errs, list):
            total_violations += len(errs)
            if errs:
                warnings_present = True
        fails = det.get("failed_expectations")
        if isinstance(fails, dict):
            total_violations += sum(int(info.get("count", 0) or 0) for info in fails.values())
            if any((info.get("count") or 0) for info in fails.values()):
                warnings_present = True
        if det.get("warnings"):
            warnings_present = True

    def _status_level(value: str | None, *, treat_block_as_warning: bool = False) -> int:
        if not value:
            return 0
        normalised = value.lower()
        if normalised in {"warn", "warning"}:
            return 1
        if normalised in {"block", "error", "fail", "invalid"}:
            return 1 if treat_block_as_warning else 2
        return 0

    severity = 0
    severity = max(severity, _status_level(getattr(orders_status, "status", None)))
    severity = max(severity, _status_level(getattr(customers_status, "status", None)))
    severity = max(severity, _status_level(getattr(output_status, "status", None)))

    dq_status_summary = output_details.get("dq_status")
    if isinstance(dq_status_summary, Mapping):
        severity = max(severity, _status_level(dq_status_summary.get("status")))
        if dq_status_summary.get("errors"):
            warnings_present = True

    for aux_entry in output_details.get("dq_auxiliary_statuses", []) or []:
        if isinstance(aux_entry, Mapping):
            severity = max(
                severity,
                _status_level(aux_entry.get("status"), treat_block_as_warning=True),
            )
            details = aux_entry.get("details")
            if isinstance(details, Mapping):
                if details.get("warnings") or details.get("errors"):
                    warnings_present = True
                violations = details.get("violations")
                if isinstance(violations, (int, float)) and violations:
                    warnings_present = True

    if result.errors or error is not None:
        severity = 2
    elif warnings_present:
        severity = max(severity, 1)

    status_value = "ok"
    if severity == 1:
        status_value = "warning"
    elif severity >= 2:
        status_value = "error"
    records.append(
        DatasetRecord(
            contract_id or "",
            contract_version or "",
            dataset_name,
            dataset_version,
            status_value,
            combined_details,
            run_type,
            total_violations,
            draft_contract_version=draft_version,
        )
    )
    save_records(records)
    if not existing_session:
        spark.stop()
    if error:
        raise error
    return dataset_name, dataset_version
