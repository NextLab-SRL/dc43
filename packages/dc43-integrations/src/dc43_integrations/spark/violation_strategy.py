"""Strategies that control how contract violations are handled during writes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Sequence

try:  # pragma: no cover - optional dependency at runtime
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover
    DataFrame = object  # type: ignore[misc,assignment]

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ValidationResult


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
    validation_factory: Optional[Callable[[], ValidationResult]] = None
    warnings: tuple[str, ...] = field(default_factory=tuple)
    pipeline_context: Optional[Mapping[str, Any]] = None


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
            validation_factory=factory,
            warnings=tuple(warnings) if warnings is not None else tuple(self.validation.warnings),
            pipeline_context=_merge_pipeline_context(
                self.pipeline_context,
                pipeline_context,
            ),
        )


class WriteViolationStrategy(Protocol):
    """Plan how a write should proceed when validation discovers violations."""

    def plan(self, context: WriteStrategyContext) -> WritePlan:
        """Return the write plan to execute for the provided context."""


@dataclass
class NoOpWriteViolationStrategy:
    """Default strategy that keeps the original behaviour intact."""

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

    def plan(self, context: WriteStrategyContext) -> WritePlan:  # noqa: D401 - short docstring
        result = context.validation
        has_violations = self._has_violations(result)
        if not has_violations:
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
                warnings.append(
                    f"Valid subset written to dataset suffix '{self.valid_suffix}'",
                )
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
                    pipeline_context=_merge_pipeline_context(
                        context.pipeline_context,
                        {"subset": self.valid_suffix},
                    ),
                )

        if self.include_reject:
            reject_df = context.aligned_df.filter(f"NOT ({composite_predicate})")
            has_reject = reject_df.limit(1).count() > 0
            if has_reject:
                warnings.append(
                    f"Rejected subset written to dataset suffix '{self.reject_suffix}'",
                )
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
                    pipeline_context=_merge_pipeline_context(
                        context.pipeline_context,
                        {"subset": self.reject_suffix},
                    ),
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

