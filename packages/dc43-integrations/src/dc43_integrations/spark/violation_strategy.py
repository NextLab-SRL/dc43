"""Strategies that control how contract violations are handled during writes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional, Protocol, Sequence

try:  # pragma: no cover - optional dependency at runtime
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover
    DataFrame = object  # type: ignore[misc,assignment]

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore
from dc43_service_clients.odps import OpenDataProductStandard

from dc43_service_clients.data_quality import ValidationResult


if TYPE_CHECKING:  # pragma: no cover
    from .io import StreamingObservationWriter


def _merge_pipeline_context(
    base: Optional[Mapping[str, Any]],
    extra: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return a merged pipeline context mapping."""

    combined: Dict[str, Any] = {}
    if base:
        combined.update(base)
    if extra:
        combined.update(extra)
    return combined or None


def _clone_validation_result(result: ValidationResult) -> ValidationResult:
    """Return a shallow copy of ``result`` suitable for reuse."""

    return ValidationResult(
        ok=result.ok,
        errors=list(result.errors),
        warnings=list(result.warnings),
        metrics=dict(result.metrics),
        schema=dict(result.schema),
        status=result.status,
        reason=result.reason,
        details=result.details,
    )


@dataclass
class WriteRequest:
    """Description of a single write operation produced by a strategy."""

    df: DataFrame
    path: Optional[str]
    table: Optional[str]
    format: Optional[str]
    options: Dict[str, str]
    mode: str
    contract: Optional[OpenDataContractStandard]
    dataset_id: Optional[str]
    dataset_version: Optional[str]
    streaming: bool = False
    validation_factory: Optional[Callable[[], ValidationResult]] = None
    warnings: tuple[str, ...] = field(default_factory=tuple)
    pipeline_context: Optional[Mapping[str, Any]] = None
    streaming_observation_writer: Optional["StreamingObservationWriter"] = None


@dataclass
class WritePlan:
    """Collection of write requests prepared by a strategy."""

    primary: Optional[WriteRequest]
    additional: Sequence[WriteRequest] = field(default_factory=tuple)
    result_factory: Optional[Callable[[], ValidationResult]] = None


@dataclass
class WriteStrategyContext:
    """Information exposed to strategies when planning writes."""

    df: DataFrame
    aligned_df: DataFrame
    contract: Optional[OpenDataContractStandard]
    path: Optional[str]
    table: Optional[str]
    format: Optional[str]
    options: Dict[str, str]
    mode: str
    validation: ValidationResult
    dataset_id: Optional[str]
    dataset_version: Optional[str]
    revalidate: Callable[[DataFrame], ValidationResult]
    expectation_predicates: Mapping[str, str]
    pipeline_context: Optional[Mapping[str, Any]] = None
    streaming: bool = False
    streaming_observation_writer: Optional["StreamingObservationWriter"] = None

    def base_request(
        self,
        *,
        validation_factory: Optional[Callable[[], ValidationResult]] = None,
        warnings: Optional[Sequence[str]] = None,
        pipeline_context: Optional[Mapping[str, Any]] = None,
    ) -> WriteRequest:
        """Return the default write request for the aligned dataframe."""

        factory = validation_factory
        if factory is None:
            if self.streaming:
                if self.streaming_observation_writer is not None:
                    factory = lambda: self.validation
                else:
                    factory = lambda: _clone_validation_result(self.validation)
            else:
                factory = lambda: self.revalidate(self.aligned_df)

        return WriteRequest(
            df=self.aligned_df,
            path=self.path,
            table=self.table,
            format=self.format,
            options=dict(self.options),
            mode=self.mode,
            contract=self.contract,
            dataset_id=self.dataset_id,
            dataset_version=self.dataset_version,
            streaming=self.streaming,
            validation_factory=factory,
            warnings=tuple(warnings) if warnings is not None else tuple(self.validation.warnings),
            pipeline_context=_merge_pipeline_context(
                self.pipeline_context,
                pipeline_context,
            ),
            streaming_observation_writer=self.streaming_observation_writer,
        )


class WriteViolationStrategy(Protocol):
    """Plan how a write should proceed when validation discovers violations."""

    def plan(self, context: WriteStrategyContext) -> WritePlan:
        """Return the write plan to execute for the provided context."""


@dataclass
class NoOpWriteViolationStrategy:
    """Default strategy that keeps the original behaviour intact."""

    allowed_contract_statuses: tuple[str, ...] = ("active",)
    allow_missing_contract_status: bool = True
    contract_status_case_insensitive: bool = True
    contract_status_failure_message: str | None = None
    allowed_data_product_statuses: tuple[str, ...] = ("active",)
    allow_missing_data_product_status: bool = True
    data_product_status_case_insensitive: bool = True
    data_product_status_failure_message: str | None = None

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        from .io import _validate_contract_status  # local import to avoid cycles

        _validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_contract_statuses,
            allow_missing=self.allow_missing_contract_status,
            case_insensitive=self.contract_status_case_insensitive,
            failure_message=self.contract_status_failure_message,
        )

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        from .io import _validate_data_product_status  # local import to avoid cycles

        _validate_data_product_status(
            data_product=data_product,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_data_product_statuses,
            allow_missing=self.allow_missing_data_product_status,
            case_insensitive=self.data_product_status_case_insensitive,
            failure_message=self.data_product_status_failure_message,
        )

    def plan(self, context: WriteStrategyContext) -> WritePlan:  # noqa: D401 - short docstring
        return WritePlan(primary=context.base_request())


@dataclass
class SplitWriteViolationStrategy:
    """Split invalid rows into dedicated datasets when a violation occurs."""

    valid_suffix: str = "valid"
    reject_suffix: str = "reject"
    include_valid: bool = True
    include_reject: bool = True
    write_primary_on_violation: bool = False
    dataset_suffix_separator: str = "::"
    allowed_contract_statuses: tuple[str, ...] = ("active",)
    allow_missing_contract_status: bool = True
    contract_status_case_insensitive: bool = True
    contract_status_failure_message: str | None = None
    allowed_data_product_statuses: tuple[str, ...] = ("active",)
    allow_missing_data_product_status: bool = True
    data_product_status_case_insensitive: bool = True
    data_product_status_failure_message: str | None = None

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        from .io import _validate_contract_status  # local import to avoid cycles

        _validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_contract_statuses,
            allow_missing=self.allow_missing_contract_status,
            case_insensitive=self.contract_status_case_insensitive,
            failure_message=self.contract_status_failure_message,
        )

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        from .io import _validate_data_product_status  # local import to avoid cycles

        _validate_data_product_status(
            data_product=data_product,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_data_product_statuses,
            allow_missing=self.allow_missing_data_product_status,
            case_insensitive=self.data_product_status_case_insensitive,
            failure_message=self.data_product_status_failure_message,
        )

    def plan(self, context: WriteStrategyContext) -> WritePlan:  # noqa: D401 - short docstring
        result = context.validation
        has_violations = self._has_violations(result)
        if not has_violations:
            return WritePlan(primary=context.base_request())

        if context.streaming:
            # Structured streaming pipelines do not support the synchronous
            # dataframe actions required to materialise split subsets.  Fallback
            # to the default behaviour so the write can proceed.
            return WritePlan(primary=context.base_request())

        predicates = list(context.expectation_predicates.values())
        if not predicates:
            # Nothing to split on – fall back to the default behaviour.
            return WritePlan(primary=context.base_request())

        composite_predicate = " AND ".join(f"({p})" for p in predicates)

        valid_request: Optional[WriteRequest] = None
        reject_request: Optional[WriteRequest] = None
        warnings: list[str] = []

        def _extend_dataset_id(base: Optional[str], suffix: str) -> Optional[str]:
            if not base:
                return None
            return f"{base}{self.dataset_suffix_separator}{suffix}"

        if self.include_valid:
            valid_df = context.aligned_df.filter(composite_predicate)
            has_valid = valid_df.limit(1).count() > 0
            if has_valid:
                valid_warning = (
                    f"Valid subset written to dataset suffix '{self.valid_suffix}'"
                )
                warnings.append(valid_warning)
                valid_request = WriteRequest(
                    df=valid_df,
                    path=self._extend_path(context.path, self.valid_suffix),
                    table=self._extend_table(context.table, self.valid_suffix),
                    format=context.format,
                    options=dict(context.options),
                    mode=context.mode,
                    contract=context.contract,
                    dataset_id=_extend_dataset_id(context.dataset_id, self.valid_suffix),
                    dataset_version=context.dataset_version,
                    validation_factory=lambda df=valid_df: context.revalidate(df),
                    warnings=(valid_warning,),
                    pipeline_context=_merge_pipeline_context(
                        context.pipeline_context,
                        {"subset": self.valid_suffix},
                    ),
                    streaming_observation_writer=context.streaming_observation_writer,
                )

        if self.include_reject:
            reject_df = context.aligned_df.filter(f"NOT ({composite_predicate})")
            has_reject = reject_df.limit(1).count() > 0
            if has_reject:
                reject_warning = (
                    f"Rejected subset written to dataset suffix '{self.reject_suffix}'"
                )
                warnings.append(reject_warning)
                reject_request = WriteRequest(
                    df=reject_df,
                    path=self._extend_path(context.path, self.reject_suffix),
                    table=self._extend_table(context.table, self.reject_suffix),
                    format=context.format,
                    options=dict(context.options),
                    mode=context.mode,
                    contract=context.contract,
                    dataset_id=_extend_dataset_id(context.dataset_id, self.reject_suffix),
                    dataset_version=context.dataset_version,
                    validation_factory=lambda df=reject_df: context.revalidate(df),
                    warnings=(reject_warning,),
                    pipeline_context=_merge_pipeline_context(
                        context.pipeline_context,
                        {"subset": self.reject_suffix},
                    ),
                    streaming_observation_writer=context.streaming_observation_writer,
                )

        for message in warnings:
            if message not in result.warnings:
                result.warnings.append(message)

        if valid_request is None and reject_request is None:
            return WritePlan(primary=context.base_request())

        primary = (
            context.base_request()
            if (not has_violations or self.write_primary_on_violation)
            else None
        )
        requests: list[WriteRequest] = []
        if valid_request is not None:
            requests.append(valid_request)
        if reject_request is not None:
            requests.append(reject_request)

        if primary is None and requests:
            # Keep the validation result describing the overall dataframe but
            # prefer the status coming from the first split write.
            warnings_snapshot = tuple(result.warnings)

            def _final_result() -> ValidationResult:
                validation = context.revalidate(context.aligned_df)
                for message in warnings_snapshot:
                    if message not in validation.warnings:
                        validation.warnings.append(message)
                return validation

            return WritePlan(
                primary=None,
                additional=tuple(requests),
                result_factory=_final_result,
            )

        return WritePlan(
            primary=primary,
            additional=tuple(requests),
        )

    @staticmethod
    def _extend_path(path: Optional[str], suffix: str) -> Optional[str]:
        if not path:
            return None
        stripped = path.rstrip("/")
        return f"{stripped}/{suffix}"

    @staticmethod
    def _extend_table(table: Optional[str], suffix: str) -> Optional[str]:
        if not table:
            return None
        return f"{table}_{suffix}"

    @staticmethod
    def _has_violations(result: ValidationResult) -> bool:
        if not result.metrics:
            return bool(result.errors)
        for key, value in result.metrics.items():
            if not key.startswith("violations."):
                continue
            if isinstance(value, (int, float)) and value > 0:
                return True
        return bool(result.errors)


@dataclass
class StrictWriteViolationStrategy:
    """Decorate another strategy and fail the run when violations persist."""

    base: WriteViolationStrategy = field(default_factory=NoOpWriteViolationStrategy)
    failure_message: str = "Validation recorded contract violations"
    fail_on_warnings: bool = False
    allowed_contract_statuses: tuple[str, ...] | None = None
    allow_missing_contract_status: bool | None = None
    contract_status_case_insensitive: bool | None = None
    contract_status_failure_message: str | None = None

    def _contract_status_option(self, name: str, default: Any) -> Any:
        value = getattr(self, name, None)
        if value is not None:
            return value

        base = getattr(self, "base", None)
        if base is None:
            return default

        inherited = getattr(base, name, None)
        if inherited is not None:
            return inherited

        return default

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        validator = getattr(self.base, "validate_contract_status", None)
        if validator is not None:
            validator(contract=contract, enforce=enforce, operation=operation)
            return

        from .io import _validate_contract_status  # local import to avoid cycles

        _validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self._contract_status_option(
                "allowed_contract_statuses", ("active",)
            ),
            allow_missing=self._contract_status_option(
                "allow_missing_contract_status", True
            ),
            case_insensitive=self._contract_status_option(
                "contract_status_case_insensitive", True
            ),
            failure_message=self._contract_status_option(
                "contract_status_failure_message", None
            ),
        )

    def plan(self, context: WriteStrategyContext) -> WritePlan:  # noqa: D401 - short docstring
        base_plan = self.base.plan(context)

        has_violations = SplitWriteViolationStrategy._has_violations(context.validation)
        has_warnings = bool(context.validation.warnings)
        if not has_violations and not (self.fail_on_warnings and has_warnings):
            return base_plan

        original_factory = base_plan.result_factory

        def _strict_result() -> ValidationResult:
            base_result = (
                original_factory()
                if original_factory is not None
                else context.revalidate(context.aligned_df)
            )
            strict_result = ValidationResult(
                ok=False,
                errors=list(base_result.errors),
                warnings=list(base_result.warnings),
                metrics=dict(base_result.metrics),
                schema=dict(base_result.schema),
            )

            for warning in context.validation.warnings:
                if warning not in strict_result.warnings:
                    strict_result.warnings.append(warning)

            message = self.failure_message.strip()
            if message and message not in strict_result.errors:
                strict_result.errors.append(message)

            return strict_result

        return WritePlan(
            primary=base_plan.primary,
            additional=base_plan.additional,
            result_factory=_strict_result,
        )


__all__ = [
    "NoOpWriteViolationStrategy",
    "StrictWriteViolationStrategy",
    "SplitWriteViolationStrategy",
    "WritePlan",
    "WriteRequest",
    "WriteStrategyContext",
    "WriteViolationStrategy",
]

