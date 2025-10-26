from __future__ import annotations

"""Example transformation pipeline using dc43 helpers.

This script demonstrates how a Spark job might read data with contract
validation, perform transformations (omitted) and write the result while
recording the dataset version in the demo app's registry.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Sequence

from . import contracts_api as contracts_server
from .contracts_records import DatasetRecord, _version_sort_key
from .spark_compat import ensure_local_spark_builder
from dc43_service_backends.data_quality.backend.engine import (
    ExpectationSpec,
    expectation_specs,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance import GovernanceReadContext, GovernanceWriteContext
from dc43_integrations.spark.data_quality import attach_failed_expectations
from dc43_integrations.spark.io import (
    ContractFirstDatasetLocator,
    ContractVersionLocator,
    DefaultReadStatusStrategy,
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    ReadStatusContext,
    ReadStatusStrategy,
    StaticDatasetLocator,
    read_with_governance,
    write_with_governance,
)
from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    SplitWriteViolationStrategy,
    StrictWriteViolationStrategy,
    WriteViolationStrategy,
)
from open_data_contract_standard.model import OpenDataContractStandard
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when


def _timestamp_slug(moment: datetime) -> str:
    """Return a filesystem-safe timestamp representation."""

    return moment.strftime("%Y%m%dT%H%M%S%fZ")


def _next_version(existing: list[str]) -> str:
    """Return a new timestamp identifier not present in ``existing``."""

    used = set(existing)
    offset = 0
    while True:
        candidate = _timestamp_slug(datetime.now(timezone.utc) + timedelta(seconds=offset))
        if candidate not in used:
            return candidate
        offset += 1


def _safe_version_segment(value: str) -> str:
    """Return a filesystem-safe folder name for ``value``."""

    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in value)
    return safe or "version"


def _write_version_marker(directory: Path, version: str) -> None:
    """Persist a marker with the canonical version inside ``directory``."""

    marker = directory / ".dc43_version"
    try:
        marker.write_text(version)
    except OSError:
        pass


def _resolve_output_path(
    contract: OpenDataContractStandard | None,
    dataset_name: str,
    dataset_version: str,
) -> Path:
    """Return output path for dataset relative to contract servers."""
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(contracts_server.DATA_DIR).parent
    server_path = Path(getattr(server, "path", "")) if server else data_root
    if server_path.suffix:
        base_path = server_path.parent / server_path.stem
    else:
        base_path = server_path
    if not base_path.is_absolute():
        base_path = data_root / base_path
    segment = _safe_version_segment(dataset_version)
    if base_path.name == dataset_name:
        out = base_path / segment
    else:
        out = base_path / dataset_name / segment
    out.mkdir(parents=True, exist_ok=True)
    if dataset_version and not (out / ".dc43_version").exists():
        _write_version_marker(out, dataset_version)
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
        status: ValidationResult | None,
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, ValidationResult | None]:
        if status and status.status == "block":
            details = dict(status.details)
            notes = list(details.get("overrides", []))
            notes.append(self.note)
            details["overrides"] = notes
            details.setdefault("status_before_override", status.status)
            return dataframe, ValidationResult(
                status=self.target_status,
                reason=status.reason,
                details=details,
        )
        return dataframe, status


ReadStrategySpec = ReadStatusStrategy | str | Mapping[str, Any] | None
ContractStatusSpec = Mapping[str, Any] | None


def _coerce_bool(value: Any, default: bool) -> bool:
    """Return a normalised boolean for ``value``."""

    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return bool(value)


def _coerce_statuses(value: Any) -> tuple[str, ...]:
    """Return a tuple of contract status strings extracted from ``value``."""

    if value is None:
        return tuple()
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        try:
            items = list(value)  # type: ignore[arg-type]
        except TypeError:
            items = [value]
    statuses: list[str] = []
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            statuses.append(text)
    return tuple(statuses)


def _extract_contract_status_config(options: MutableMapping[str, Any]) -> dict[str, Any]:
    """Return canonical contract-status options popped from ``options``."""

    if not isinstance(options, MutableMapping):
        return {}

    config: dict[str, Any] = {}
    raw = options.pop("contract_status", None)
    if isinstance(raw, Mapping):
        config.update(raw)

    alias_map = {
        "allowed_contract_statuses": "allowed_contract_statuses",
        "allowed_statuses": "allowed_contract_statuses",
        "allowed": "allowed_contract_statuses",
        "allow_missing_contract_status": "allow_missing_contract_status",
        "allow_missing": "allow_missing_contract_status",
        "contract_status_case_insensitive": "contract_status_case_insensitive",
        "case_insensitive": "contract_status_case_insensitive",
        "contract_status_failure_message": "contract_status_failure_message",
    }

    for key, target in alias_map.items():
        if key in options:
            config[target] = options.pop(key)

    return config


def _canonical_contract_status_config(spec: ContractStatusSpec) -> dict[str, Any]:
    """Return canonicalised contract-status options and validate keys."""

    if not spec:
        return {}
    raw: MutableMapping[str, Any] = dict(spec)
    config = _extract_contract_status_config(raw)
    if raw:
        unknown = ", ".join(sorted(map(str, raw.keys())))
        raise ValueError(f"Unknown contract status option(s): {unknown}")
    return config


def _normalise_contract_status_config(
    config: Mapping[str, Any] | None,
    *,
    defaults: Any,
) -> dict[str, Any]:
    """Return normalised contract-status kwargs merged with ``defaults``."""

    allowed_default = tuple(
        getattr(defaults, "allowed_contract_statuses", ("active",))
    )
    allow_missing_default = bool(
        getattr(defaults, "allow_missing_contract_status", True)
    )
    case_insensitive_default = bool(
        getattr(defaults, "contract_status_case_insensitive", True)
    )
    failure_default = getattr(defaults, "contract_status_failure_message", None)

    allowed_source: Any = None
    if config:
        allowed_source = (
            config.get("allowed_contract_statuses")
            or config.get("allowed_statuses")
            or config.get("allowed")
        )
    allowed = _coerce_statuses(allowed_source)
    if not allowed:
        allowed = tuple(str(status) for status in allowed_default)

    allow_missing = allow_missing_default
    if config:
        allow_missing = _coerce_bool(
            config.get("allow_missing_contract_status", config.get("allow_missing")),
            allow_missing_default,
        )

    case_insensitive = case_insensitive_default
    if config:
        case_insensitive = _coerce_bool(
            config.get(
                "contract_status_case_insensitive",
                config.get("case_insensitive"),
            ),
            case_insensitive_default,
        )

    failure_value = failure_default
    if config and "contract_status_failure_message" in config:
        failure_value = config["contract_status_failure_message"]
    elif config and "failure_message" in config:
        failure_value = config["failure_message"]

    failure_message = None
    if failure_value is not None:
        failure_message = str(failure_value)

    return {
        "allowed_contract_statuses": tuple(str(status) for status in allowed),
        "allow_missing_contract_status": allow_missing,
        "contract_status_case_insensitive": case_insensitive,
        "contract_status_failure_message": failure_message,
    }


class _ContractStatusReadWrapper:
    """Attach contract-status validation to an existing read strategy."""

    def __init__(
        self,
        base: ReadStatusStrategy | None,
        *,
        allowed_contract_statuses: tuple[str, ...],
        allow_missing_contract_status: bool,
        contract_status_case_insensitive: bool,
        contract_status_failure_message: str | None,
    ) -> None:
        self.base = base
        self.allowed_contract_statuses = tuple(allowed_contract_statuses)
        self.allow_missing_contract_status = allow_missing_contract_status
        self.contract_status_case_insensitive = contract_status_case_insensitive
        self.contract_status_failure_message = contract_status_failure_message
        self._default = DefaultReadStatusStrategy(
            allowed_contract_statuses=self.allowed_contract_statuses,
            allow_missing_contract_status=self.allow_missing_contract_status,
            contract_status_case_insensitive=self.contract_status_case_insensitive,
            contract_status_failure_message=self.contract_status_failure_message,
        )

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: ValidationResult | None,
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, ValidationResult | None]:
        if self.base is not None:
            return self.base.apply(
                dataframe=dataframe,
                status=status,
                enforce=enforce,
                context=context,
            )
        return self._default.apply(
            dataframe=dataframe,
            status=status,
            enforce=enforce,
            context=context,
        )

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        self._default.validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
        )


def _apply_contract_status_to_read_strategy(
    strategy: ReadStatusStrategy | None,
    config: Mapping[str, Any] | None,
) -> ReadStatusStrategy | None:
    """Return ``strategy`` updated with the provided contract-status config."""

    if not config:
        return strategy

    defaults = DefaultReadStatusStrategy()
    options = _normalise_contract_status_config(config, defaults=defaults)

    if strategy is None:
        return DefaultReadStatusStrategy(**options)

    if isinstance(strategy, DefaultReadStatusStrategy):
        strategy.allowed_contract_statuses = options["allowed_contract_statuses"]
        strategy.allow_missing_contract_status = options["allow_missing_contract_status"]
        strategy.contract_status_case_insensitive = options[
            "contract_status_case_insensitive"
        ]
        strategy.contract_status_failure_message = options[
            "contract_status_failure_message"
        ]
        return strategy

    return _ContractStatusReadWrapper(strategy, **options)


def _apply_contract_status_to_write_strategy(
    strategy: WriteViolationStrategy | None,
    config: Mapping[str, Any] | None,
) -> WriteViolationStrategy | None:
    """Attach contract-status overrides to ``strategy`` when supported."""

    if strategy is None or not config:
        return strategy

    defaults = strategy
    if not hasattr(strategy, "allowed_contract_statuses"):
        defaults = NoOpWriteViolationStrategy()

    options = _normalise_contract_status_config(config, defaults=defaults)

    if hasattr(strategy, "allowed_contract_statuses"):
        strategy.allowed_contract_statuses = options["allowed_contract_statuses"]  # type: ignore[attr-defined]
        strategy.allow_missing_contract_status = options["allow_missing_contract_status"]  # type: ignore[attr-defined]
        strategy.contract_status_case_insensitive = options[
            "contract_status_case_insensitive"
        ]  # type: ignore[attr-defined]
        strategy.contract_status_failure_message = options[
            "contract_status_failure_message"
        ]  # type: ignore[attr-defined]

    base = getattr(strategy, "base", None)
    if base is not None:
        setattr(
            strategy,
            "base",
            _apply_contract_status_to_write_strategy(base, config),
        )

    return strategy


def _describe_contract_status_policy(handler: object) -> dict[str, Any] | None:
    """Return a serialisable summary of a contract-status policy."""

    allowed = getattr(handler, "allowed_contract_statuses", None)
    allow_missing = getattr(handler, "allow_missing_contract_status", None)
    case_insensitive = getattr(handler, "contract_status_case_insensitive", None)
    failure_message = getattr(handler, "contract_status_failure_message", None)
    if allowed is None or allow_missing is None or case_insensitive is None:
        return None
    return {
        "allowed": list(allowed),
        "allow_missing": bool(allow_missing),
        "case_insensitive": bool(case_insensitive),
        "failure_message": failure_message,
    }


def _resolve_violation_strategy(
    spec: StrategySpec,
    *,
    contract_status: ContractStatusSpec = None,
) -> WriteViolationStrategy | None:
    """Return a concrete violation strategy based on ``spec``."""

    status_config = _canonical_contract_status_config(contract_status)

    if spec is None:
        if status_config:
            return _apply_contract_status_to_write_strategy(
                NoOpWriteViolationStrategy(),
                status_config,
            )
        return None

    if hasattr(spec, "plan"):
        return _apply_contract_status_to_write_strategy(
            spec,  # type: ignore[arg-type]
            status_config,
        )

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

    spec_status_config = _extract_contract_status_config(options)
    merged_status_config = dict(status_config)
    merged_status_config.update(spec_status_config)

    key = name.lower()
    if key in {"noop", "default", "none"}:
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown violation strategy option(s): {unknown}"
            )
        strategy = NoOpWriteViolationStrategy()
        return _apply_contract_status_to_write_strategy(
            strategy,
            merged_status_config,
        )
    if key in {"split", "split-datasets", "split_datasets"}:
        allowed: Sequence[str] = (
            "valid_suffix",
            "reject_suffix",
            "include_valid",
            "include_reject",
            "write_primary_on_violation",
            "dataset_suffix_separator",
        )
        filtered = {k: options.pop(k) for k in allowed if k in options}
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown split strategy option(s): {unknown}"
            )
        strategy = SplitWriteViolationStrategy(**filtered)
        return _apply_contract_status_to_write_strategy(
            strategy,
            merged_status_config,
        )
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
        fail_on_warnings = _coerce_bool(
            options.pop("fail_on_warnings", False),
            False,
        )
        base_options = {k: options.pop(k) for k in allowed if k in options}
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown split strategy option(s): {unknown}"
            )
        base_strategy = SplitWriteViolationStrategy(**base_options)
        base_strategy = _apply_contract_status_to_write_strategy(
            base_strategy,
            merged_status_config,
        )
        return StrictWriteViolationStrategy(
            base=base_strategy,
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
        fail_on_warnings = _coerce_bool(
            options.pop("fail_on_warnings", False),
            False,
        )
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown strict strategy option(s): {unknown}"
            )
        base_strategy = _apply_contract_status_to_write_strategy(
            NoOpWriteViolationStrategy(),
            merged_status_config,
        )
        return StrictWriteViolationStrategy(
            base=base_strategy,
            failure_message=failure_message,
            fail_on_warnings=fail_on_warnings,
        )

    raise ValueError(f"Unknown violation strategy: {name}")


def _resolve_read_status_strategy(
    spec: ReadStrategySpec,
    contract_status: ContractStatusSpec = None,
) -> ReadStatusStrategy | None:
    """Return a read status strategy instance for the supplied spec."""

    status_config = _canonical_contract_status_config(contract_status)

    if spec is None:
        return _apply_contract_status_to_read_strategy(None, status_config)

    if hasattr(spec, "apply"):
        return _apply_contract_status_to_read_strategy(
            spec,  # type: ignore[arg-type]
            status_config,
        )

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

    spec_status_config = _extract_contract_status_config(options)
    merged_status_config = dict(status_config)
    merged_status_config.update(spec_status_config)

    key = name.lower()
    if key in {"default", "none", "pass", "passthrough"}:
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown read status option(s): {unknown}"
            )
        return _apply_contract_status_to_read_strategy(
            None,
            merged_status_config,
        )
    if key in {"allow", "allow-block", "downgrade"}:
        note = str(
            options.pop(
                "note",
                "Blocked dataset accepted for downstream processing",
            )
        )
        target = str(
            options.pop(
                "target_status",
                options.pop("target", "warn"),
            )
        )
        if options:
            unknown = ", ".join(sorted(map(str, options.keys())))
            raise ValueError(
                f"Unknown read status option(s): {unknown}"
            )
        strategy = _DowngradeBlockingReadStrategy(
            note=note,
            target_status=target,
        )
        return _apply_contract_status_to_read_strategy(
            strategy,
            merged_status_config,
        )

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

    if key in {"boost-amounts", "raise_amounts", "ensure_high_amounts"}:
        notes.append(
            "raised low order amounts and populated customer segments to satisfy the draft contract expectations"
        )
        df = df.withColumn(
            "amount",
            when(col("amount") < 150, lit(150)).otherwise(col("amount")),
        )
        if "customer_segment" not in df.columns:
            df = df.withColumn("customer_segment", lit("loyalty_pilot"))
        return df, notes

    return df, []


def _format_expectation_violation_message(spec: ExpectationSpec, count: int) -> str:
    """Return the engine-style message for a failed expectation."""

    column = spec.column or "field"
    if spec.rule in {"not_null", "required"}:
        return f"column {column} contains {count} null value(s) but is required in the contract"
    if spec.rule == "unique":
        return f"column {column} has {count} duplicate value(s)"
    if spec.rule == "enum":
        allowed = spec.params.get("values")
        if isinstance(allowed, Iterable):
            allowed_str = ", ".join(map(str, allowed))
        else:
            allowed_str = str(allowed)
        return f"column {column} contains {count} value(s) outside enum [{allowed_str}]"
    if spec.rule == "regex":
        pattern = spec.params.get("pattern")
        return f"column {column} contains {count} value(s) not matching regex {pattern}"
    if spec.rule == "gt":
        return f"column {column} contains {count} value(s) not greater than {spec.params.get('threshold')}"
    if spec.rule == "ge":
        return f"column {column} contains {count} value(s) below {spec.params.get('threshold')}"
    if spec.rule == "lt":
        return f"column {column} contains {count} value(s) not less than {spec.params.get('threshold')}"
    if spec.rule == "le":
        return f"column {column} contains {count} value(s) above {spec.params.get('threshold')}"
    return f"expectation {spec.key} failed {count} time(s)"


def _expectation_error_messages(
    contract: OpenDataContractStandard,
    metrics: Mapping[str, Any] | None,
) -> set[str]:
    """Return messages describing expectation failures found in ``metrics``."""

    metric_map = dict(metrics or {})
    messages: set[str] = set()
    for spec in expectation_specs(contract):
        if spec.rule == "query":
            continue
        key = f"violations.{spec.key}"
        count = metric_map.get(key)
        if isinstance(count, (int, float)) and count > 0:
            messages.add(_format_expectation_violation_message(spec, int(count)))
    return messages


def _status_payload(status: ValidationResult | None) -> dict[str, Any] | None:
    """Return a JSON-serialisable payload summarising ``status``."""

    if status is None:
        return None
    payload: dict[str, Any] = {}
    details = status.details
    if isinstance(details, Mapping):
        payload.update(details)
    elif details is not None:
        payload["details"] = details
    payload.setdefault("status", status.status)
    if status.reason:
        payload.setdefault("reason", status.reason)
    return payload


def _resolve_dataset_name_hint(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str | None,
) -> str:
    """Return the most appropriate dataset identifier for logging failures."""

    if dataset_name:
        return dataset_name
    if contract_id and contract_version:
        try:
            contract = contracts_server.contract_service.get(contract_id, contract_version)
        except FileNotFoundError:
            return contract_id
        dataset_id = getattr(contract, "id", None)
        if dataset_id:
            return dataset_id
        return contract_id
    return dataset_name or contract_id or "result"


def _record_blocked_read_failure(
    *,
    error_message: str,
    contract_id: str | None,
    contract_version: str | None,
    dataset_name_hint: str,
    run_type: str,
    scenario_key: str | None,
    orders_status: ValidationResult | None,
    customers_status: ValidationResult | None,
) -> None:
    """Persist a dataset record describing a blocked input read."""

    dq_details: dict[str, Any] = {}
    orders_payload = _status_payload(orders_status)
    customers_payload = _status_payload(customers_status)
    if orders_payload:
        dq_details["orders"] = orders_payload
    if customers_payload:
        dq_details["customers"] = customers_payload
    dq_details["output"] = {
        "errors": [error_message],
        "dq_status": {"status": "error", "reason": error_message},
    }

    violations_total = 0
    for payload in (orders_payload, customers_payload):
        if isinstance(payload, Mapping):
            violations_value = payload.get("violations")
            if isinstance(violations_value, (int, float)):
                violations_total += int(violations_value)

    records = contracts_server.load_records()
    record = contracts_server.DatasetRecord(
        contract_id or "",
        contract_version or "",
        dataset_name_hint,
        "",
        "error",
        dq_details,
        run_type,
        violations_total,
        scenario_key=scenario_key,
    )
    record.reason = error_message
    records.append(record)
    contracts_server.save_records(records)


def _normalise_record_status(value: str | None) -> str:
    """Map validation status strings onto registry-friendly labels."""

    if not value:
        return "ok"
    text = value.lower()
    if text in {"warn", "warning"}:
        return "warning"
    if text in {"block", "error", "fail", "invalid"}:
        return "error"
    return "ok"


def _extract_violation_count(section: Mapping[str, Any] | None) -> int:
    """Return the maximum violation count found within ``section``."""

    if not isinstance(section, Mapping):
        return 0
    total = 0
    candidate = section.get("violations")
    if isinstance(candidate, (int, float)):
        total = max(total, int(candidate))
    metrics = section.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if str(key).startswith("violations") and isinstance(value, (int, float)):
                total = max(total, int(value))
    failed = section.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if not isinstance(info, Mapping):
                continue
            count = info.get("count")
            if isinstance(count, (int, float)):
                total = max(total, int(count))
    errors = section.get("errors")
    if isinstance(errors, list):
        total = max(total, len(errors))
    details = section.get("details")
    if isinstance(details, Mapping):
        total = max(total, _extract_violation_count(details))
    dq_status = section.get("dq_status")
    if isinstance(dq_status, Mapping):
        total = max(total, _extract_violation_count(dq_status))
    return total


def _aggregate_violation_counts(*sections: Mapping[str, Any] | None) -> int:
    """Return the highest violation count across the supplied sections."""

    total = 0
    for section in sections:
        total = max(total, _extract_violation_count(section))
    return total


def _preferred_dataset_version(
    config: Mapping[str, Any],
    records: Sequence[DatasetRecord] | None,
) -> str | None:
    """Return the latest approved dataset version for the requested input."""

    if not records:
        return None

    requested_version = str(config.get("dataset_version") or "").strip()
    if requested_version and requested_version.lower() != "latest":
        return None

    candidate_names = {
        str(config.get("dataset_name") or "").strip(),
        str(config.get("dataset_id") or "").strip(),
        str(config.get("contract_id") or "").strip(),
    }
    candidate_names.discard("")
    if not candidate_names:
        return None

    statuses: Iterable[str] | str | None = config.get("preferred_statuses") or config.get("dataset_status")
    if statuses is None:
        allowed = {"ok"}
    elif isinstance(statuses, str):
        allowed = {statuses.lower()}
    else:
        allowed = {str(status).lower() for status in statuses if status}
        if not allowed:
            allowed = {"ok"}

    approved_versions: list[str] = []
    for record in records:
        if not isinstance(record, DatasetRecord):
            continue
        status = (record.status or "").lower()
        if status not in allowed:
            continue
        if record.dataset_name not in candidate_names and record.contract_id not in candidate_names:
            continue
        if not record.dataset_version:
            continue
        approved_versions.append(record.dataset_version)

    if not approved_versions:
        return None

    approved_versions.sort(key=_version_sort_key)
    return approved_versions[-1]


def _data_product_input_locator(
    config: Mapping[str, Any],
    *,
    records: Sequence[DatasetRecord] | None = None,
) -> ContractVersionLocator | StaticDatasetLocator:
    """Return the dataset locator used for data product reads."""

    dataset_id = config.get("dataset_id")
    preferred_version = _preferred_dataset_version(config, records)
    dataset_version = preferred_version or config.get("dataset_version") or "latest"
    default = ContractVersionLocator(
        dataset_version=str(dataset_version),
        dataset_id=str(dataset_id) if dataset_id not in (None, "") else None,
        base=ContractFirstDatasetLocator(),
    )

    locator_spec = config.get("dataset_locator") if isinstance(config, Mapping) else None
    if locator_spec is not None and hasattr(locator_spec, "for_read"):
        return locator_spec  # type: ignore[return-value]

    overrides: Mapping[str, Any] | None = None
    if isinstance(locator_spec, Mapping):
        overrides = locator_spec
    else:
        keys = ("dataset_id", "dataset_version", "path", "table", "format", "subpath", "base")
        extracted = {
            key: config[key]
            for key in keys
            if key in config
            and not (key == "dataset_version" and preferred_version is not None)
        }
        if extracted:
            overrides = extracted

    return _apply_locator_overrides(default, overrides)


def _run_data_product_flow(
    *,
    spark: SparkSession,
    base_context: Mapping[str, Any],
    run_timestamp: str,
    run_type: str,
    data_product_flow: Mapping[str, Any],
    collect_examples: bool,
    examples_limit: int,
    scenario_key: str | None,
) -> tuple[str, str]:
    """Execute the data-product centric pipeline scenario."""

    governance = contracts_server.governance_service

    def _context(step: str, extra: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        payload = dict(base_context)
        payload["step"] = step
        if extra:
            payload.update(extra)
        return payload

    records = contracts_server.load_records()

    input_cfg = data_product_flow.get("input") if isinstance(data_product_flow, Mapping) else {}
    input_binding = dict(input_cfg.get("binding") or {})
    if not input_binding:
        raise ValueError("data_product_flow requires an input binding")

    input_locator = _data_product_input_locator(input_cfg, records=records)
    input_dataset_id = getattr(input_locator, "dataset_id", None)
    input_dataset_version = getattr(input_locator, "dataset_version", None)
    input_expected_version = (
        input_cfg.get("expected_contract_version")
        if isinstance(input_cfg, Mapping)
        else None
    )
    input_contract_version = (
        input_cfg.get("contract_version") if isinstance(input_cfg, Mapping) else None
    )
    source_dp = input_binding.get("source_data_product")
    source_port = input_binding.get("source_output_port")
    orders_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            input_binding=input_binding,
            dataset_id=input_dataset_id,
            dataset_version=input_dataset_version,
        ),
        dataset_locator=input_locator,
        pipeline_context=_context(
            "data-product-input",
            {
                "data_product": input_binding.get("data_product"),
                "port_name": input_binding.get("port_name"),
                "source_data_product": source_dp,
                "source_output_port": source_port,
                **(
                    {"dataset": input_dataset_id}
                    if input_dataset_id
                    else {}
                ),
                **(
                    {"dataset_version": input_dataset_version}
                    if input_dataset_version
                    else {}
                ),
                **(
                    {"expected_contract_version": input_expected_version}
                    if input_expected_version
                    else {}
                ),
                **(
                    {"contract_version": input_contract_version}
                    if input_contract_version
                    else {}
                ),
                "collect_examples": bool(collect_examples),
                "examples_limit": examples_limit,
            },
        ),
    )
    orders_df, orders_status = read_with_governance(
        spark,
        orders_request,
        governance_service=governance,
        return_status=True,
    )

    orders_payload = _status_payload(orders_status)
    if isinstance(orders_payload, Mapping):
        orders_payload = dict(orders_payload)
    elif input_dataset_id or input_dataset_version or input_binding.get("data_product"):
        orders_payload = {}
    if isinstance(orders_payload, dict):
        if input_dataset_id:
            orders_payload.setdefault("dataset_id", input_dataset_id)
        if input_dataset_version:
            orders_payload.setdefault("dataset_version", input_dataset_version)
        port_info = input_binding.get("port_name") or source_port
        if port_info:
            orders_payload.setdefault("data_product_port", port_info)
        dp_identifier = input_binding.get("data_product") or source_dp
        if dp_identifier:
            orders_payload.setdefault("data_product", dp_identifier)
    else:
        orders_payload = None

    customers_cfg = data_product_flow.get("customers") if isinstance(data_product_flow, Mapping) else {}
    customers_contract_id = customers_cfg.get("contract_id") or "customers"
    customers_expected = customers_cfg.get("expected_contract_version")
    customers_version = customers_cfg.get("contract_version") or None
    customers_locator = _apply_locator_overrides(
        ContractVersionLocator(
            dataset_version=str(customers_cfg.get("dataset_version") or "latest"),
            dataset_id=str(customers_cfg.get("dataset_id") or "customers"),
            base=ContractFirstDatasetLocator(),
        ),
        customers_cfg if isinstance(customers_cfg, Mapping) else None,
    )
    customers_contract = {"contract_id": customers_contract_id}
    if customers_version:
        customers_contract["contract_version"] = customers_version
    if customers_expected:
        customers_contract.setdefault("version_selector", customers_expected)

    customers_df, customers_status = read_with_governance(
        spark,
        GovernanceSparkReadRequest(
            context={"contract": customers_contract},
            dataset_locator=customers_locator,
            pipeline_context=_context(
                "customers-read",
                {
                    "contract_id": customers_contract_id,
                    "contract_version": customers_version,
                    "collect_examples": bool(collect_examples),
                    "examples_limit": examples_limit,
                },
            ),
        ),
        governance_service=governance,
        return_status=True,
    )

    df = orders_df.join(customers_df, "customer_id", "left")

    adjustment = data_product_flow.get("output_adjustment") if isinstance(data_product_flow, Mapping) else None
    df, adjustment_notes = _apply_output_adjustment(df, adjustment)

    stage_cfg = data_product_flow.get("intermediate_contract") if isinstance(data_product_flow, Mapping) else {}
    stage_contract_id = stage_cfg.get("contract_id")
    if not stage_contract_id:
        raise ValueError("data_product_flow requires an intermediate_contract contract_id")
    stage_contract_version = stage_cfg.get("contract_version")
    stage_expected_version = stage_cfg.get("expected_contract_version")
    stage_dataset_name = stage_cfg.get("dataset_name") or stage_contract_id
    stage_contract = contracts_server.contract_service.get(stage_contract_id, stage_contract_version)

    existing_stage_versions = [
        rec.dataset_version for rec in records if rec.dataset_name == stage_dataset_name and rec.dataset_version
    ]
    stage_dataset_version = _next_version(existing_stage_versions)
    stage_output_path = _resolve_output_path(stage_contract, stage_dataset_name, stage_dataset_version)
    stage_locator = ContractVersionLocator(
        dataset_version=stage_dataset_version,
        base=ContractFirstDatasetLocator(),
    )

    stage_contract_spec = {"contract_id": stage_contract_id}
    if stage_contract_version:
        stage_contract_spec["contract_version"] = stage_contract_version
    if stage_expected_version:
        stage_contract_spec.setdefault("version_selector", stage_expected_version)

    stage_result, stage_status = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context={
                "contract": stage_contract_spec,
                "dataset_id": stage_dataset_name,
                "dataset_version": stage_dataset_version,
            },
            dataset_locator=stage_locator,
            mode="overwrite",
            pipeline_context=_context(
                "stage-write",
                {
                    "dataset": stage_dataset_name,
                    "dataset_version": stage_dataset_version,
                    "storage_path": str(stage_output_path),
                    "collect_examples": bool(collect_examples),
                    "examples_limit": examples_limit,
                },
            ),
        ),
        governance_service=governance,
        enforce=False,
        return_status=True,
    )

    try:
        contracts_server.register_dataset_version(stage_dataset_name, stage_dataset_version, stage_output_path)
        contracts_server.set_active_version(stage_dataset_name, stage_dataset_version)
    except FileNotFoundError:
        pass

    stage_output_details = dict(stage_result.details or {})
    if adjustment_notes:
        stage_output_details.setdefault("transformation_notes", adjustment_notes)
    stage_payload = _status_payload(stage_status)
    if stage_payload:
        stage_output_details.setdefault("dq_status", stage_payload)
    stage_activity = governance.get_pipeline_activity(
        dataset_id=stage_dataset_name,
        dataset_version=stage_dataset_version,
    )
    if stage_activity:
        stage_output_details.setdefault("pipeline_activity", stage_activity)
    stage_output_details.setdefault(
        "data_product",
        {
            "id": data_product_flow.get("output", {}).get("data_product"),
            "port": stage_cfg.get("contract_id"),
            "role": "intermediate",
        },
    )
    stage_output_details.setdefault("storage_path", str(stage_output_path))

    stage_combined_details: Dict[str, Any] = {
        "orders": orders_payload,
        "customers": _status_payload(customers_status),
        "output": stage_output_details,
    }
    stage_violations = _aggregate_violation_counts(*stage_combined_details.values())
    stage_status_value = _normalise_record_status(getattr(stage_status, "status", None))
    if not stage_result.ok:
        stage_status_value = "error"
    if stage_status_value == "ok" and stage_output_details.get("warnings"):
        stage_status_value = "warning"

    stage_draft_version = stage_output_details.get("draft_contract_version")
    if not stage_draft_version and stage_payload and isinstance(stage_payload, Mapping):
        stage_draft_version = stage_payload.get("draft_contract_version")

    stage_record = contracts_server.DatasetRecord(
        stage_contract_id or "",
        stage_contract_version or "",
        stage_dataset_name,
        stage_dataset_version,
        stage_status_value,
        stage_combined_details,
        run_type,
        stage_violations,
        draft_contract_version=stage_draft_version if isinstance(stage_draft_version, str) else None,
        scenario_key=scenario_key,
    )
    stage_record.reason = getattr(stage_status, "reason", "") or ""
    stage_record.data_product_id = data_product_flow.get("output", {}).get("data_product", "")
    stage_record.data_product_port = stage_cfg.get("contract_id", "")
    stage_record.data_product_role = "intermediate"
    records.append(stage_record)

    stage_read_contract = {"contract_id": stage_contract_id}
    if stage_contract_version:
        stage_read_contract["contract_version"] = stage_contract_version
    if stage_expected_version:
        stage_read_contract.setdefault("version_selector", stage_expected_version)

    stage_read_df, stage_read_status = read_with_governance(
        spark,
        GovernanceSparkReadRequest(
            context=GovernanceReadContext(contract=stage_read_contract),
            dataset_locator=ContractVersionLocator(
                dataset_version=stage_dataset_version,
                base=ContractFirstDatasetLocator(),
            ),
            pipeline_context=_context(
                "stage-read",
                {
                    "dataset": stage_dataset_name,
                    "dataset_version": stage_dataset_version,
                    "collect_examples": bool(collect_examples),
                    "examples_limit": examples_limit,
                },
            ),
        ),
        governance_service=governance,
        return_status=True,
    )

    output_cfg = data_product_flow.get("output") if isinstance(data_product_flow, Mapping) else {}
    output_dataset_name = output_cfg.get("dataset_name") or output_cfg.get("contract_id") or stage_contract_id
    output_dataset_version = run_timestamp
    output_contract_id = output_cfg.get("contract_id") or stage_contract_id
    output_contract_version = output_cfg.get("contract_version")
    expected_output_version = output_cfg.get("expected_contract_version")
    output_contract = None
    if output_contract_id and output_contract_version:
        output_contract = contracts_server.contract_service.get(output_contract_id, output_contract_version)

    output_path = _resolve_output_path(output_contract, output_dataset_name, output_dataset_version)
    output_locator = ContractVersionLocator(
        dataset_version=output_dataset_version,
        base=ContractFirstDatasetLocator(),
    )

    dp_binding = {
        "data_product": output_cfg.get("data_product"),
        "port_name": output_cfg.get("port_name"),
    }

    final_result, final_status = write_to_data_product(
        df=stage_read_df,
        data_product_service=contracts_server.data_product_service,
        data_product_output=dp_binding,
        contract_id=output_contract_id,
        contract_service=contracts_server.contract_service,
        expected_contract_version=expected_output_version,
        data_quality_service=contracts_server.dq_service,
        governance_service=governance,
        dataset_locator=output_locator,
        mode="overwrite",
        enforce=False,
        return_status=True,
        pipeline_context=_context(
            "data-product-write",
            {
                "dataset": output_dataset_name,
                "dataset_version": output_dataset_version,
                "storage_path": str(output_path),
                "collect_examples": bool(collect_examples),
                "examples_limit": examples_limit,
            },
        ),
    )

    try:
        contracts_server.register_dataset_version(output_dataset_name, output_dataset_version, output_path)
        contracts_server.set_active_version(output_dataset_name, output_dataset_version)
    except FileNotFoundError:
        pass

    final_output_details = dict(final_result.details or {})
    if adjustment_notes:
        existing_notes = list(final_output_details.get("transformation_notes", []) or [])
        for note in adjustment_notes:
            if note not in existing_notes:
                existing_notes.append(note)
        if existing_notes:
            final_output_details["transformation_notes"] = existing_notes
    final_payload = _status_payload(final_status)
    if final_payload:
        final_output_details.setdefault("dq_status", final_payload)
    final_activity = governance.get_pipeline_activity(
        dataset_id=output_dataset_name,
        dataset_version=output_dataset_version,
    )
    if final_activity:
        final_output_details.setdefault("pipeline_activity", final_activity)
    final_output_details.setdefault(
        "data_product",
        {
            "id": output_cfg.get("data_product"),
            "port": output_cfg.get("port_name"),
            "role": "output",
        },
    )
    final_output_details.setdefault("storage_path", str(output_path))

    final_combined_details: Dict[str, Any] = {
        "orders": orders_payload,
        "customers": _status_payload(customers_status),
        "stage": _status_payload(stage_read_status),
        "output": final_output_details,
    }
    final_violations = _aggregate_violation_counts(*final_combined_details.values())
    final_status_value = _normalise_record_status(getattr(final_status, "status", None))
    if not final_result.ok:
        final_status_value = "error"
    if final_status_value == "ok" and final_output_details.get("warnings"):
        final_status_value = "warning"

    final_draft_version = final_output_details.get("draft_contract_version")
    if not final_draft_version and final_payload and isinstance(final_payload, Mapping):
        final_draft_version = final_payload.get("draft_contract_version")

    final_record = contracts_server.DatasetRecord(
        output_contract_id or "",
        output_contract_version or "",
        output_dataset_name,
        output_dataset_version,
        final_status_value,
        final_combined_details,
        run_type,
        final_violations,
        draft_contract_version=final_draft_version if isinstance(final_draft_version, str) else None,
        scenario_key=scenario_key,
    )
    final_record.reason = getattr(final_status, "reason", "") or ""
    final_record.data_product_id = output_cfg.get("data_product", "")
    final_record.data_product_port = output_cfg.get("port_name", "")
    final_record.data_product_role = "output"
    records.append(final_record)

    contracts_server.save_records(records)

    return output_dataset_name or stage_dataset_name, output_dataset_version


def run_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str | None,
    dataset_version: str | None,
    run_type: str,
    collect_examples: bool = False,
    examples_limit: int = 5,
    violation_strategy: StrategySpec = None,
    enforce_contract_status: bool | None = None,
    inputs: Mapping[str, Mapping[str, Any]] | None = None,
    output_adjustment: str | None = None,
    data_product_flow: Mapping[str, Any] | None = None,
    *,
    scenario_key: str | None = None,
    output_transform: Callable[[DataFrame, MutableMapping[str, Any]], DataFrame] | None = None,
) -> tuple[str, str]:
    """Run an example pipeline using the configured contract store.

    When an output contract is supplied the dataset name is derived from the
    contract identifier so the recorded runs and filesystem layout match the
    declared server path.  Callers may supply a custom name when no contract is
    available.  The ``inputs`` mapping can override dataset locators, enforce
    flags, and read-status strategies for each source (``"orders"`` and
    ``"customers"``) so demo scenarios can highlight how mixed-validity inputs
    are handled.  ``output_adjustment`` optionally tweaks the joined dataframe
    (for example to deliberately surface violations). ``enforce_contract_status``
    toggles whether non-active contracts raise immediately when writing; by
    default enforcement matches ``run_type == "enforce"``. ``scenario_key`` tags
    the recorded run so the UI can distinguish scenarios that share the same
    output dataset.  ``output_transform`` allows callers to wrap the final
    dataframe before publishing (for example to execute it through a local DLT
    harness) while keeping the record-keeping logic shared with the default
    Spark implementation. Returns the dataset name used along with the
    materialized version.
    """
    ensure_local_spark_builder()

    existing_session = SparkSession.getActiveSession()
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    governance = contracts_server.governance_service

    run_timestamp = _timestamp_slug(datetime.now(timezone.utc))
    base_pipeline_context: dict[str, Any] = {
        "pipeline": "dc43_demo_app.pipeline.run_pipeline",
        "run_id": run_timestamp,
        "run_type": run_type,
    }
    contract_status_enforce = (
        enforce_contract_status
        if enforce_contract_status is not None
        else run_type == "enforce"
    )
    base_pipeline_context["contract_status_enforced"] = bool(contract_status_enforce)
    if scenario_key:
        base_pipeline_context["scenario_key"] = scenario_key
    if contract_id:
        base_pipeline_context["target_contract_id"] = contract_id
    if contract_version:
        base_pipeline_context["target_contract_version"] = contract_version
    if dataset_name:
        base_pipeline_context["output_dataset_hint"] = dataset_name

    if data_product_flow:
        result_dataset, result_version = _run_data_product_flow(
            spark=spark,
            base_context=base_pipeline_context,
            run_timestamp=run_timestamp,
            run_type=run_type,
            data_product_flow=data_product_flow,
            collect_examples=collect_examples,
            examples_limit=examples_limit,
            scenario_key=scenario_key,
        )
        contracts_server.refresh_dataset_aliases()
        if not existing_session:
            spark.stop()
        return result_dataset, result_version

    def _context_for(step: str, extra: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        payload = dict(base_pipeline_context)
        payload["step"] = step
        if extra:
            payload.update(extra)
        return payload

    input_overrides: Mapping[str, Mapping[str, Any]] = inputs or {}
    dataset_name_hint = _resolve_dataset_name_hint(
        contract_id,
        contract_version,
        dataset_name,
    )

    orders_overrides = input_overrides.get("orders")
    orders_locator = _apply_locator_overrides(
        ContractVersionLocator(
            dataset_version="latest",
            base=ContractFirstDatasetLocator(),
        ),
        orders_overrides,
    )
    orders_strategy = _resolve_read_status_strategy(
        orders_overrides.get("status_strategy") if isinstance(orders_overrides, Mapping) else None,
        contract_status=
        orders_overrides.get("contract_status") if isinstance(orders_overrides, Mapping) else None,
    )
    orders_default_enforce = False
    treat_orders_blocking = False
    if orders_overrides and orders_overrides.get("dataset_version") == "latest":
        if run_type == "enforce":
            treat_orders_blocking = True
    orders_enforce = bool(
        orders_overrides.get("enforce", orders_default_enforce)
        if orders_overrides
        else orders_default_enforce
    )
    if orders_strategy is None and not orders_enforce and not treat_orders_blocking:
        orders_strategy = _DowngradeBlockingReadStrategy(
            note="Blocked dataset accepted for downstream processing",
            target_status="warn",
        )

    orders_policy = _describe_contract_status_policy(
        orders_strategy or DefaultReadStatusStrategy()
    )

    # Read primary orders dataset with its contract
    orders_read_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": "orders",
                "version_selector": "==1.1.0",
            }
        ),
        dataset_locator=orders_locator,
        status_strategy=orders_strategy,
        pipeline_context=_context_for(
            "orders-read",
            {
                "dataset_role": "orders",
                "contract_status_enforced": bool(orders_enforce),
                **(
                    {"contract_status_policy": orders_policy}
                    if orders_policy
                    else {}
                ),
            },
        ),
    )
    orders_df, orders_status = read_with_governance(
        spark,
        orders_read_request,
        governance_service=governance,
        enforce=orders_enforce,
        return_status=True,
    )
    if treat_orders_blocking and orders_status and orders_status.status == "block":
        details = orders_status.reason or orders_status.details
        message = f"DQ status is blocking: {details}"
        _record_blocked_read_failure(
            error_message=message,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_name_hint=dataset_name_hint,
            run_type=run_type,
            scenario_key=scenario_key,
            orders_status=orders_status,
            customers_status=None,
        )
        raise ValueError(message)

    customers_overrides = input_overrides.get("customers")
    customers_locator = _apply_locator_overrides(
        ContractVersionLocator(
            dataset_version="latest",
            base=ContractFirstDatasetLocator(),
        ),
        customers_overrides,
    )
    customers_strategy = _resolve_read_status_strategy(
        customers_overrides.get("status_strategy") if isinstance(customers_overrides, Mapping) else None,
        contract_status=
        customers_overrides.get("contract_status") if isinstance(customers_overrides, Mapping) else None,
    )
    customers_default_enforce = False
    treat_customers_blocking = False
    if customers_overrides and customers_overrides.get("dataset_version") == "latest":
        if run_type == "enforce":
            treat_customers_blocking = True
    customers_enforce = bool(
        customers_overrides.get("enforce", customers_default_enforce)
        if customers_overrides
        else customers_default_enforce
    )
    if (
        customers_strategy is None
        and not customers_enforce
        and not treat_customers_blocking
    ):
        customers_strategy = _DowngradeBlockingReadStrategy(
            note="Blocked dataset accepted for downstream processing",
            target_status="warn",
        )

    customers_policy = _describe_contract_status_policy(
        customers_strategy or DefaultReadStatusStrategy()
    )

    # Join with customers lookup dataset
    customers_read_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": "customers",
                "version_selector": "==1.0.0",
            }
        ),
        dataset_locator=customers_locator,
        status_strategy=customers_strategy,
        pipeline_context=_context_for(
            "customers-read",
            {
                "dataset_role": "customers",
                "contract_status_enforced": bool(customers_enforce),
                **(
                    {"contract_status_policy": customers_policy}
                    if customers_policy
                    else {}
                ),
            },
        ),
    )
    customers_df, customers_status = read_with_governance(
        spark,
        customers_read_request,
        governance_service=governance,
        enforce=customers_enforce,
        return_status=True,
    )
    if treat_customers_blocking and customers_status and customers_status.status == "block":
        details = customers_status.reason or customers_status.details
        message = f"DQ status is blocking: {details}"
        _record_blocked_read_failure(
            error_message=message,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_name_hint=dataset_name_hint,
            run_type=run_type,
            scenario_key=scenario_key,
            orders_status=orders_status,
            customers_status=customers_status,
        )
        raise ValueError(message)

    df = orders_df.join(customers_df, "customer_id")
    # Promote one of the rows above the quality threshold so split strategies
    # demonstrate both valid and reject outputs in the demo.
    df = df.withColumn(
        "amount",
        when(col("order_id") == 1, col("amount") * 20).otherwise(col("amount")),
    )

    df, adjustment_notes = _apply_output_adjustment(df, output_adjustment)

    records = contracts_server.load_records()
    output_contract = (
        contracts_server.store.get(contract_id, contract_version) if contract_id and contract_version else None
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

    base_pipeline_context["output_dataset"] = dataset_name
    base_pipeline_context["output_dataset_version"] = dataset_version

    strategy = _resolve_violation_strategy(violation_strategy)
    status_handler = strategy or NoOpWriteViolationStrategy()
    strategy_policy = _describe_contract_status_policy(status_handler)

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
    write_context_extra: dict[str, Any] = {
        "dataset": dataset_name,
        "dataset_version": dataset_version,
        "storage_path": str(output_path),
        "contract_status_enforced": bool(contract_status_enforce),
    }
    if strategy_policy:
        write_context_extra["contract_status_policy"] = strategy_policy

    transform_context: MutableMapping[str, Any] = {}
    if output_transform:
        transform_context = {
            "spark": spark,
            "contract_id": contract_id_ref,
            "expected_contract_version": expected_version,
            "dataset_name": dataset_name,
            "dataset_version": dataset_version,
            "governance_service": contracts_server.governance_service,
            "run_type": run_type,
        }
        df = output_transform(df, transform_context)
        engine_label = transform_context.get("pipeline_engine")
        if engine_label:
            base_pipeline_context["engine"] = engine_label
            write_context_extra.setdefault("engine", engine_label)
        asset_name = transform_context.get("dlt_asset_name")
        if asset_name:
            write_context_extra.setdefault("dlt_asset", asset_name)

    output_pipeline_context = _context_for("output-write", write_context_extra)
    governance_write_request: GovernanceSparkWriteRequest | None = None
    if contract_id_ref:
        contract_spec: dict[str, Any] = {"contract_id": contract_id_ref}
        resolved_version = (
            getattr(output_contract, "version", None)
            or contract_version
        )
        if resolved_version:
            contract_spec["contract_version"] = resolved_version
        if expected_version:
            contract_spec.setdefault("version_selector", expected_version)
        governance_write_request = GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract=contract_spec,
                dataset_id=dataset_name,
                dataset_version=dataset_version,
            ),
            dataset_locator=locator,
            mode="overwrite",
            pipeline_context=output_pipeline_context,
        )

    write_error: ValueError | None = None
    if output_contract and contract_status_enforce:
        try:
            status_handler.validate_contract_status(
                contract=output_contract,
                enforce=True,
                operation="write",
            )
        except ValueError as exc:
            write_error = exc
            error_message = str(exc)
            failure_details: dict[str, Any] = {
                "errors": [error_message],
                "contract_status_error": error_message,
            }
            if strategy_policy:
                failure_details.setdefault(
                    "contract_status_policy",
                    strategy_policy,
                )
            result = ValidationResult(
                ok=False,
                errors=[error_message],
                warnings=[],
                metrics={},
                status="error",
                reason=error_message,
                details=failure_details,
            )
            output_status = None
        else:
            if not governance_write_request:
                raise ValueError("Output contract must be configured before publishing data.")
            result, output_status = write_with_governance(
                df=df,
                request=governance_write_request,
                governance_service=governance,
                enforce=False,
                return_status=True,
                violation_strategy=strategy,
            )
    else:
        if governance_write_request is None:
            message = "Output contract must be configured before publishing data."
            result = ValidationResult(
                ok=False,
                errors=[message],
                warnings=[],
                metrics={},
                status="error",
                reason=message,
                details={"errors": [message]},
            )
            output_status = None
            write_error = ValueError(message)
        else:
            result, output_status = write_with_governance(
                df=df,
                request=governance_write_request,
                governance_service=governance,
                enforce=False,
                return_status=True,
                violation_strategy=strategy,
            )

    if output_status and output_contract:
        output_status = attach_failed_expectations(
            output_contract,
            output_status,
            metrics=result.metrics,
        )

    expectation_messages: set[str] = set()
    if output_contract:
        expectation_messages = _expectation_error_messages(
            output_contract,
            result.metrics,
        )

    schema_errors: list[str] = []
    seen_schema_errors: set[str] = set()
    original_errors = list(result.errors)
    if result.errors:
        filtered_errors: list[str] = []
        for message in result.errors:
            if message in expectation_messages:
                continue
            if message in seen_schema_errors:
                continue
            seen_schema_errors.add(message)
            filtered_errors.append(message)
        if filtered_errors != result.errors:
            result.errors[:] = filtered_errors
        schema_errors.extend(filtered_errors)
    if not schema_errors:
        residual = [msg for msg in original_errors if msg not in expectation_messages]
        schema_errors.extend(residual)
    if output_contract:
        expected_columns = {
            prop.name
            for obj in output_contract.schema_ or []
            for prop in obj.properties or []
            if prop.name
        }
        missing = sorted(name for name in expected_columns if name not in set(df.columns))
        for name in missing:
            message = f"missing required column: {name}"
            if message not in schema_errors:
                schema_errors.append(message)

    handled_split_override = False
    if isinstance(strategy, SplitWriteViolationStrategy) and output_status:
        details = output_status.details or {}
        if isinstance(details, Mapping) and details.get("status_before_override"):
            handled_split_override = True

    if handled_split_override and result.errors:
        migrated = list(result.errors)
        result.errors.clear()
        for message in migrated:
            if message not in result.warnings:
                result.warnings.append(message)
        if not result.errors:
            result.ok = True

    error: ValueError | None = write_error
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
            if schema_errors:
                issues.append(
                    f"Schema validation failed: {schema_errors}"
                )
            if issues:
                error = ValueError("; ".join(issues))

    draft_version: str | None = None
    output_details = result.details.copy()
    if result.warnings:
        warning_list = list(output_details.get("warnings", []) or [])
        for message in result.warnings:
            if message not in warning_list:
                warning_list.append(message)
        if warning_list:
            output_details["warnings"] = warning_list
    if schema_errors:
        output_details["errors"] = schema_errors
    else:
        output_details.pop("errors", None)
    if adjustment_notes:
        extra = output_details.setdefault("transformations", [])
        if isinstance(extra, list):
            extra.extend(adjustment_notes)
        else:
            output_details["transformations"] = adjustment_notes
    if strategy_policy and "contract_status_policy" not in output_details:
        output_details["contract_status_policy"] = strategy_policy
    output_details.setdefault(
        "contract_status_enforced",
        bool(contract_status_enforce),
    )
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
                warning_messages = list(output_details.get("warnings", []) or [])
                for entry in aux:
                    if not isinstance(entry, Mapping):
                        continue
                    kind = entry.get("kind")
                    if kind == "valid":
                        message = (
                            f"Valid subset written to dataset suffix '{strategy.valid_suffix}'"
                        )
                    elif kind == "reject":
                        message = (
                            f"Rejected subset written to dataset suffix '{strategy.reject_suffix}'"
                        )
                    else:
                        continue
                    if message not in warning_messages:
                        warning_messages.append(message)
            if warning_messages:
                output_details["warnings"] = warning_messages

    if transform_context:
        engine_label = transform_context.get("pipeline_engine")
        if engine_label and "pipeline_engine" not in output_details:
            output_details["pipeline_engine"] = engine_label
        module_name = transform_context.get("dlt_module_name")
        if module_name and "dlt_module_name" not in output_details:
            output_details["dlt_module_name"] = module_name
        if "dlt_module_stub" not in output_details and "dlt_module_stub" in transform_context:
            output_details["dlt_module_stub"] = bool(transform_context["dlt_module_stub"])
        asset_name = transform_context.get("dlt_asset_name")
        if asset_name and "dlt_asset" not in output_details:
            output_details["dlt_asset"] = asset_name
        reports = transform_context.get("dlt_expectation_reports")
        if reports and "dlt_expectations" not in output_details:
            output_details["dlt_expectations"] = reports
        table_options = transform_context.get("dlt_table_options")
        if table_options and "dlt_table_options" not in output_details:
            output_details["dlt_table_options"] = table_options

    if dataset_name and dataset_version:
        try:
            contracts_server.set_active_version(dataset_name, dataset_version)
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
                    contracts_server.register_dataset_version(alias, dataset_version, Path(path_ref))
                    contracts_server.set_active_version(alias, dataset_version)
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

    output_activity = governance.get_pipeline_activity(
        dataset_id=dataset_name,
        dataset_version=dataset_version,
    )
    if output_activity:
        output_details.setdefault("pipeline_activity", output_activity)

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

    if schema_errors or result.errors or error is not None:
        severity = 2
    elif warnings_present:
        severity = max(severity, 1)

    if (
        handled_split_override
        and severity > 1
        and not schema_errors
        and not result.errors
        and error is None
    ):
        severity = 1

    status_value = "ok"
    if severity == 1:
        status_value = "warning"
    elif severity >= 2:
        status_value = "error"
    records.append(
        contracts_server.DatasetRecord(
            contract_id or "",
            contract_version or "",
            dataset_name,
            dataset_version,
            status_value,
            combined_details,
            run_type,
            total_violations,
            draft_contract_version=draft_version,
            scenario_key=scenario_key,
        )
    )
    contracts_server.save_records(records)
    if not existing_session:
        spark.stop()
    if error:
        raise error
    return dataset_name, dataset_version


def __getattr__(name: str) -> Any:
    delegated = {
        "DATASETS_FILE",
        "DATA_DIR",
        "DatasetRecord",
        "load_records",
        "save_records",
        "register_dataset_version",
        "set_active_version",
        "store",
        "contract_service",
        "dq_service",
        "governance_service",
    }
    if name in delegated:
        return getattr(contracts_server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
