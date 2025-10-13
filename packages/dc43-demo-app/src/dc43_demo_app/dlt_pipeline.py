"""Utilities for running the demo pipeline as a Delta Live Tables job."""

from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
import inspect
from pathlib import Path
from typing import Any, Mapping

from . import contracts_api as contracts_server
from . import pipeline as batch_pipeline
from .contracts_records import DatasetRecord


def _timestamp_slug(moment: datetime) -> str:
    """Delegate to the batch pipeline timestamp helper."""

    return batch_pipeline._timestamp_slug(moment)  # type: ignore[attr-defined]


def _next_version(existing: list[str]) -> str:
    """Return a fresh dataset version identifier."""

    return batch_pipeline._next_version(existing)  # type: ignore[attr-defined]


def _resolve_output_path(contract: Any | None, dataset: str, version: str) -> Path:
    """Resolve the filesystem destination for ``dataset`` and ``version``."""

    return batch_pipeline._resolve_output_path(  # type: ignore[attr-defined]
        contract,
        dataset,
        version,
    )


def _load_contract_table_module() -> Any:
    """Import the integration module providing the contract table DLT helper."""

    try:
        return import_module("dc43_integrations.spark.dlt.contract_table")
    except ModuleNotFoundError as exc:  # pragma: no cover - surfaced to callers
        raise RuntimeError(
            "The Delta Live Tables helpers are not installed. "
            "Install dc43-integrations with DLT support to run this scenario."
        ) from exc


def _build_contract_table_pipeline(module: Any, config: Mapping[str, Any] | None) -> Any:
    """Return the callable used to populate the DLT harness."""

    pipeline_override = (config or {}).get("pipeline") if isinstance(config, Mapping) else None
    pipeline_args = (config or {}).get("pipeline_args", {}) if isinstance(config, Mapping) else {}

    if callable(pipeline_override):
        return pipeline_override
    if isinstance(pipeline_override, str):
        candidate = getattr(module, pipeline_override, None)
        if callable(candidate):
            return candidate(**pipeline_args)
        if candidate is not None:
            return candidate

    builder = getattr(module, "build_pipeline", None)
    if callable(builder):
        return builder(**pipeline_args)

    pipeline_attr = getattr(module, "CONTRACT_TABLE_PIPELINE", None)
    if pipeline_attr is not None:
        return pipeline_attr

    pipeline_callable = getattr(module, "pipeline", None)
    if callable(pipeline_callable):
        return pipeline_callable

    raise RuntimeError("Contract table pipeline entry point not found in integration module")


def _call_with_supported_kwargs(func: Any, *, kwargs: Mapping[str, Any]) -> Any:
    """Invoke ``func`` filtering keyword arguments the callable understands."""

    signature = inspect.signature(func)
    accepts_var_kwargs = any(
        param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()
    )
    if accepts_var_kwargs:
        return func(**kwargs)

    supported = {
        name
        for name, param in signature.parameters.items()
        if param.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    filtered = {key: value for key, value in kwargs.items() if key in supported}
    return func(**filtered)


def _summarise_violations(details: Mapping[str, Any]) -> int:
    """Return a best-effort violation count from ``details``."""

    total = 0
    if not isinstance(details, Mapping):
        return 0

    direct = details.get("violations")
    if isinstance(direct, (int, float)):
        total += int(direct)

    metrics = details.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if not isinstance(value, (int, float)):
                continue
            if key.startswith("violations"):
                total += int(value)

    failed = details.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if isinstance(info, Mapping):
                count = info.get("count")
                if isinstance(count, (int, float)):
                    total += int(count)

    errors = details.get("errors")
    if isinstance(errors, list):
        total += len(errors)

    return total


def _status_severity(value: str | None) -> int:
    """Normalise a textual status to a severity bucket."""

    if not value:
        return 0
    text = value.lower()
    if text in {"ok", "success", "passed"}:
        return 0
    if text in {"warn", "warning"}:
        return 1
    if text in {"block", "error", "fail", "invalid"}:
        return 2
    return 1


def _ensure_registered(
    contract: Any | None,
    dataset: str,
    version: str,
    output_details: Mapping[str, Any],
) -> None:
    """Register the dataset version along with any auxiliary outputs."""

    output_path = output_details.get("path") if isinstance(output_details, Mapping) else None
    if isinstance(output_path, str):
        target = Path(output_path)
    else:
        target = _resolve_output_path(contract, dataset, version)
    target.mkdir(parents=True, exist_ok=True)
    contracts_server.register_dataset_version(dataset, version, target)
    try:
        contracts_server.set_active_version(dataset, version)
    except FileNotFoundError:
        pass

    auxiliaries = output_details.get("auxiliary_datasets") if isinstance(output_details, Mapping) else None
    if isinstance(auxiliaries, list):
        for aux in auxiliaries:
            if not isinstance(aux, Mapping):
                continue
            aux_dataset = aux.get("dataset")
            aux_path = aux.get("path")
            if not isinstance(aux_dataset, str) or not isinstance(aux_path, str):
                continue
            aux_target = Path(aux_path)
            aux_target.mkdir(parents=True, exist_ok=True)
            try:
                contracts_server.register_dataset_version(aux_dataset, version, aux_target)
                contracts_server.set_active_version(aux_dataset, version)
            except FileNotFoundError:
                continue

    contracts_server.refresh_dataset_aliases(dataset)


def run_dlt_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str | None,
    dataset_version: str | None,
    run_type: str,
    *,
    scenario_key: str | None = None,
    dlt_config: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    """Execute the contract-table DLT demo pipeline via the local harness."""

    module = _load_contract_table_module()
    harness_cls = getattr(module, "LocalDLTHarness", None)
    if harness_cls is None:
        raise RuntimeError("LocalDLTHarness is not available in the DLT integration module")

    pipeline_callable = _build_contract_table_pipeline(module, dlt_config)

    run_timestamp = _timestamp_slug(datetime.now(timezone.utc))
    base_context: dict[str, Any] = {
        "pipeline": "dc43_demo_app.dlt_pipeline.run_dlt_pipeline",
        "run_id": run_timestamp,
        "run_type": run_type,
    }
    if scenario_key:
        base_context["scenario_key"] = scenario_key
    if contract_id:
        base_context["target_contract_id"] = contract_id
    if contract_version:
        base_context["target_contract_version"] = contract_version

    records = contracts_server.load_records()
    output_contract = (
        contracts_server.store.get(contract_id, contract_version)
        if contract_id and contract_version
        else None
    )
    if output_contract and getattr(output_contract, "id", None):
        dataset_name = output_contract.id
    elif not dataset_name:
        dataset_name = contract_id or "result"

    existing_versions = [
        record.dataset_version for record in records if record.dataset_name == dataset_name
    ]
    if not dataset_version:
        dataset_version = _next_version(existing_versions)

    base_context["output_dataset"] = dataset_name
    base_context["output_dataset_version"] = dataset_version

    harness_config = {}
    run_overrides = {}
    if isinstance(dlt_config, Mapping):
        harness_config = dict(dlt_config.get("harness", {}))
        run_overrides = dict(dlt_config.get("run", {}))

    try:
        harness = harness_cls(pipeline_callable, **harness_config)
    except TypeError:
        harness = harness_cls(**{**harness_config, "pipeline": pipeline_callable})

    run_callable: Any
    context_manager = getattr(harness, "__enter__", None)
    if callable(context_manager):
        with harness as active:
            run_callable = getattr(active, "run", None)
            if run_callable is None:
                raise RuntimeError("DLT harness does not provide a run method")
            result = _call_with_supported_kwargs(
                run_callable,
                kwargs={
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_name": dataset_name,
                    "dataset_version": dataset_version,
                    "run_type": run_type,
                    "pipeline_context": base_context,
                    **run_overrides,
                },
            )
    else:
        run_callable = getattr(harness, "run", None)
        if run_callable is None:
            raise RuntimeError("DLT harness does not provide a run method")
        result = _call_with_supported_kwargs(
            run_callable,
            kwargs={
                "contract_id": contract_id,
                "contract_version": contract_version,
                "dataset_name": dataset_name,
                "dataset_version": dataset_version,
                "run_type": run_type,
                "pipeline_context": base_context,
                **run_overrides,
            },
        )

    dq_details: Mapping[str, Any]
    if isinstance(result, Mapping):
        dq_details = dict(result)
    else:
        dq_details = {"output": {"result": result}}

    reported_dataset = None
    reported_version = None
    if isinstance(result, Mapping):
        reported_dataset = result.get("dataset") or result.get("dataset_name")
        reported_version = result.get("version") or result.get("dataset_version")

    if isinstance(reported_dataset, str) and reported_dataset:
        dataset_name = reported_dataset
    if isinstance(reported_version, str) and reported_version:
        dataset_version = reported_version

    if not dataset_name or not dataset_version:
        raise RuntimeError("DLT run did not yield a dataset identifier")

    output_details: Mapping[str, Any]
    if isinstance(dq_details.get("output"), Mapping):
        output_details = dq_details["output"]  # type: ignore[index]
    else:
        output_details = dq_details
        dq_details = {"output": output_details}

    output_details = dict(output_details)
    output_details.setdefault("pipeline_context", base_context)

    _ensure_registered(output_contract, dataset_name, dataset_version, output_details)

    dq_status = output_details.get("dq_status")
    status_text: str | None = None
    if isinstance(dq_status, Mapping):
        raw_status = dq_status.get("status")
        if isinstance(raw_status, str):
            status_text = raw_status
    if isinstance(dq_details.get("status"), str):
        status_text = str(dq_details["status"])

    severity = _status_severity(status_text)
    if output_details.get("errors"):
        severity = max(severity, 2)
    elif output_details.get("warnings"):
        severity = max(severity, 1)

    violations = _summarise_violations(output_details)
    if violations and severity < 2:
        severity = max(severity, 1)

    status_value = "ok"
    if severity == 1:
        status_value = "warning"
    elif severity >= 2:
        status_value = "error"

    draft_version = None
    if isinstance(output_details.get("draft_contract_version"), str):
        draft_version = output_details.get("draft_contract_version")  # type: ignore[assignment]
    elif isinstance(dq_details.get("draft_contract_version"), str):
        draft_version = dq_details.get("draft_contract_version")  # type: ignore[assignment]

    record = DatasetRecord(
        contract_id or "",
        contract_version or "",
        dataset_name,
        dataset_version,
        status_value,
        dict(dq_details),
        run_type,
        violations,
        draft_contract_version=draft_version,
        scenario_key=scenario_key,
    )

    records.append(record)
    contracts_server.save_records(records)

    if run_type == "enforce" and severity >= 2:
        raise ValueError(
            f"DLT pipeline reported blocking status: {status_text or 'error'}"
        )

    return dataset_name, dataset_version


__all__ = ["run_dlt_pipeline"]

