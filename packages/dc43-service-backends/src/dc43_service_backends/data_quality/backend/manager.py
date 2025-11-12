"""Lightweight data-quality manager backed by pluggable execution engines."""

from __future__ import annotations

from typing import Dict, Mapping

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload

from .engine import ValidationResult
from .engines import (
    DataQualityExecutionEngine,
    GreatExpectationsEngine,
    NativeDataQualityEngine,
    SodaEngine,
)
from .predicates import expectation_predicates_from_plan


class DataQualityManager:
    """Evaluate observation payloads using registered execution engines."""

    def __init__(
        self,
        *,
        default_engine: str = "native",
        engines: Mapping[str, DataQualityExecutionEngine] | None = None,
        strict_types: bool = True,
        allow_extra_columns: bool = True,
        expectation_severity: str = "error",
    ) -> None:
        native = NativeDataQualityEngine(
            strict_types=strict_types,
            allow_extra_columns=allow_extra_columns,
            expectation_severity=expectation_severity,
        )
        registry: Dict[str, DataQualityExecutionEngine] = {
            "native": native,
            "builtin": native,
            "great_expectations": GreatExpectationsEngine(),
            "soda": SodaEngine(),
        }
        if engines:
            registry.update({name.lower(): engine for name, engine in engines.items()})
        self._engines = registry
        self._default_engine = (default_engine or "native").lower()

    def _resolve_engine_name(self, contract: OpenDataContractStandard) -> str:
        try:
            metadata = contract.metadata  # type: ignore[attr-defined]
        except AttributeError:
            metadata = None
        if isinstance(metadata, Mapping):
            for key in ("quality_engine", "qualityEngine", "dq_engine", "dqEngine"):
                value = metadata.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip().lower()
        try:
            schema_objects = contract.schema_  # type: ignore[attr-defined]
        except AttributeError:
            schema_objects = None
        for obj in schema_objects or []:
            try:
                qualities = obj.quality  # type: ignore[attr-defined]
            except AttributeError:
                qualities = None
            for dq in qualities or []:
                try:
                    engine_name = dq.engine  # type: ignore[attr-defined]
                except AttributeError:
                    engine_name = None
                if isinstance(engine_name, str) and engine_name.strip():
                    return engine_name.strip().lower()
        return self._default_engine or "native"

    def _engine_for_name(self, name: str) -> DataQualityExecutionEngine:
        engine = self._engines.get(name)
        if engine is None:
            raise RuntimeError(f"No data-quality engine registered for '{name}'")
        return engine

    def _engine_for(
        self, contract: OpenDataContractStandard
    ) -> tuple[str, DataQualityExecutionEngine]:
        name = self._resolve_engine_name(contract)
        engine = self._engine_for_name(name)
        return name, engine

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        engine_name, engine = self._engine_for(contract)
        result = engine.evaluate(contract, payload)
        details = result.details
        need_plan = not isinstance(details.get("expectation_plan"), list)
        need_predicates = not isinstance(details.get("expectation_predicates"), Mapping)
        if need_plan or need_predicates:
            descriptors = list(engine.describe_expectations(contract))
            extras: Dict[str, object] = {}
            if need_plan and descriptors:
                extras["expectation_plan"] = descriptors
            if need_predicates:
                predicates = expectation_predicates_from_plan(descriptors)
                if predicates:
                    extras["expectation_predicates"] = predicates
            if extras:
                result.merge_details(extras)
        if (
            result.status == "unknown"
            and not result.reason
            and not result.errors
            and details.get("engine") == engine_name
        ):
            result.status = engine_name
        return result

    def describe_expectations(
        self,
        contract: OpenDataContractStandard,
    ) -> list[dict[str, object]]:
        _, engine = self._engine_for(contract)
        descriptors = engine.describe_expectations(contract)
        return [dict(item) for item in descriptors]


__all__ = ["DataQualityManager", "ObservationPayload", "ValidationResult"]
