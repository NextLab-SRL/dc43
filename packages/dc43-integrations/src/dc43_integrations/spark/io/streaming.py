from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

from pyspark.sql import DataFrame

from dc43_service_clients.data_quality import ValidationResult, ObservationPayload
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_integrations.spark.data_quality import collect_observations
from dc43_integrations.spark.io.common import (
    _safe_fs_name,
    resolve_dataset_version,
)
from dc43_service_backends.core.odcs import contract_identity

logger = logging.getLogger(__name__)


def _derive_metrics_checkpoint(
    base: Optional[str],
    dataset_id: Optional[str],
    dataset_version: Optional[str],
) -> str:
    """Return a checkpoint path for streaming metric collectors."""

    if isinstance(base, str) and base:
        trimmed = base.rstrip("/")
        if trimmed.endswith("_dq"):
            return trimmed
        return f"{trimmed}_dq"

    safe_id = _safe_fs_name(dataset_id or "stream")
    safe_version = _safe_fs_name(dataset_version or "unknown")  # Use "unknown" if None to match original logic approx
    # Original used _timestamp() if None:
    # safe_version = _safe_fs_name(dataset_version or _timestamp())
    # I need _timestamp. I can import it from strategies? No, circular.
    # Duplicate strict timestamp logic or utility?
    # Or just use datetime.utcnow() directly here.
    if not dataset_version:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        safe_version = _safe_fs_name(now.isoformat().replace("+00:00", "Z"))
    
    root = Path(tempfile.gettempdir()) / "dc43_stream_metrics" / safe_id / safe_version
    try:
        root.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover
        pass
    return str(root)


class StreamingInterventionError(RuntimeError):
    """Raised when a streaming intervention strategy blocks the pipeline."""


@dataclass(frozen=True)
class StreamingInterventionContext:
    """Information provided to intervention strategies for each micro-batch."""

    batch_id: int
    validation: ValidationResult
    dataset_id: str
    dataset_version: str


class StreamingInterventionStrategy(Protocol):
    """Decide whether a streaming pipeline should be interrupted."""

    def decide(self, context: StreamingInterventionContext) -> Optional[str]:
        """Return a reason to block the stream or ``None`` to continue."""


class NoOpStreamingInterventionStrategy:
    """Default strategy that never blocks the streaming pipeline."""

    def decide(self, context: StreamingInterventionContext) -> Optional[str]:
        return None


class StreamingObservationWriter:
    """Send streaming micro-batch observations to the data-quality service."""

    def __init__(
        self,
        *,
        contract: OpenDataContractStandard,
        expectation_plan: Sequence[Mapping[str, Any]],
        governance_service: GovernanceServiceClient,
        dataset_id: Optional[str],
        dataset_version: Optional[str],
        enforce: bool,
        checkpoint_location: Optional[str] = None,
        intervention: Optional[StreamingInterventionStrategy] = None,
        progress_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
        pipeline_context: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.contract = contract
        self.expectation_plan = list(expectation_plan)
        self.governance_service = governance_service
        self.dataset_id = dataset_id or "unknown"
        self.dataset_version = dataset_version or "unknown"
        self.pipeline_context = pipeline_context
        self.enforce = enforce
        self._validation: Optional[ValidationResult] = None
        self._latest_batch_id: Optional[int] = None
        self._active = False
        self._checkpoint_location = _derive_metrics_checkpoint(
            checkpoint_location,
            self.dataset_id,
            self.dataset_version,
        )
        default_name = f"dc43_metrics_{_safe_fs_name(self.dataset_id)}"
        self.query_name = f"{default_name}_{_safe_fs_name(self.dataset_version)}"
        self._intervention = intervention or NoOpStreamingInterventionStrategy()
        self._batches: List[Dict[str, Any]] = []
        self._progress_callback = progress_callback
        self._sink_queries: List[Any] = []

    @property
    def checkpoint_location(self) -> str:
        """Location used to checkpoint the metrics query."""
        return self._checkpoint_location

    @property
    def active(self) -> bool:
        """Whether the observation writer has already started its query."""
        return self._active

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        # Remove objects that cannot be pickled (e.g. Spark queries, callbacks)
        state['_sink_queries'] = []
        state['_progress_callback'] = None
        
        # Try to serialize RemoteGovernanceServiceClient configuration to reconstruct on worker
        gov = state.get('governance_service')
        if gov is not None and type(gov).__name__ == "RemoteGovernanceServiceClient":
            state["_gov_base_url"] = getattr(gov, "_base_url", None)
            client = getattr(gov, "_client", None)
            if client is not None and hasattr(client, "headers"):
                state["_gov_headers"] = dict(client.headers)
        elif gov is not None and type(gov).__name__ == "LocalGovernanceServiceBackend":
            import os
            # Capture relevant environment variables to re-bootstrap the local backend on the worker
            state["_gov_env"] = {
                k: v for k, v in os.environ.items() 
                if k.startswith("DC43_") or k.startswith("DATABRICKS_")
            }
                
        # Drop governance_service to prevent httpx weakref PicklingError on Spark Connect.
        # Fallback evaluation will be used in the worker if reconstruction fails.
        state['governance_service'] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        base_url = state.get("_gov_base_url")
        if base_url and self.governance_service is None:
            try:
                from dc43_service_clients.governance import RemoteGovernanceServiceClient
                headers = state.get("_gov_headers") or {}
                self.governance_service = RemoteGovernanceServiceClient(
                    base_url=base_url,
                    headers=headers,
                )
            except Exception:
                pass

        gov_env = state.get("_gov_env")
        if gov_env is not None and self.governance_service is None:
            try:
                import os
                # Temporarily inject environment variables needed for configuration
                original_env_values = {}
                for k, v in gov_env.items():
                    if k in os.environ:
                        original_env_values[k] = os.environ[k]
                    os.environ[k] = v
                
                from dc43_service_backends.config import load_config
                from dc43_service_backends.bootstrap import build_backends
                
                config = load_config()
                suite = build_backends(config)
                self.governance_service = suite.governance
                
                # Restore original environment
                for k in gov_env:
                    if k in original_env_values:
                        os.environ[k] = original_env_values[k]
                    else:
                        os.environ.pop(k, None)
            except Exception:
                pass

    def attach_validation(self, validation: ValidationResult) -> None:
        """Attach the validation object that should receive streaming metrics."""
        if self._validation is not None and self._validation is not validation:
            raise RuntimeError("StreamingObservationWriter already bound to a validation")

        self._validation = validation
        validation.merge_details(
            {
                "dataset_id": self.dataset_id,
                "dataset_version": self.dataset_version,
            }
        )

    def latest_validation(self) -> Optional[ValidationResult]:
        """Return the most recent validation produced by the observer."""
        return self._validation

    def streaming_batches(self) -> List[Mapping[str, Any]]:
        """Return the recorded micro-batch timeline."""
        return [dict(item) for item in self._batches]

    def _record_batch(
        self,
        *,
        batch_id: int,
        metrics: Mapping[str, Any] | None,
        row_count: int,
        status: str,
        timestamp: datetime,
        errors: Optional[Sequence[str]] = None,
        warnings: Optional[Sequence[str]] = None,
        intervention: Optional[str] = None,
    ) -> None:
        metrics_map = dict(metrics or {})
        violation_total = sum(
            int(value)
            for key, value in metrics_map.items()
            if key.startswith("violations.") and isinstance(value, (int, float))
        )
        entry: Dict[str, Any] = {
            "batch_id": batch_id,
            "timestamp": timestamp.isoformat(),
            "row_count": row_count,
            "violations": violation_total,
            "status": status,
        }
        if metrics_map:
            entry["metrics"] = metrics_map
        if errors:
            entry["errors"] = list(errors)
        if warnings:
            entry["warnings"] = list(warnings)
        if intervention:
            entry["intervention"] = intervention
        self._batches.append(entry)
        self._notify_progress({"type": "batch", **entry})

    def _notify_progress(self, event: Mapping[str, Any]) -> None:
        if self._progress_callback is None:
            return
        try:
            self._progress_callback(dict(event))
        except Exception:  # pragma: no cover - best effort progress hook
            logger.exception("Streaming progress callback failed")

    def watch_sink_query(self, query: Any) -> None:
        """Track a sink query so it can be stopped on enforcement failure."""
        if query not in self._sink_queries:
            self._sink_queries.append(query)

    def _stop_sink_queries(self) -> None:
        for query in list(self._sink_queries):
            try:
                stop = query.stop  # type: ignore[attr-defined]
            except AttributeError:
                stop = None
            try:
                if callable(stop):
                    stop()
            except Exception:  # pragma: no cover - best effort cleanup
                logger.exception("Failed to stop streaming sink query")

    def _merge_batch_details(
        self,
        result: ValidationResult,
        *,
        batch_id: int,
        effective_dataset_version: str,
    ) -> None:
        details = {
            "dataset_id": self.dataset_id,
            "dataset_version": effective_dataset_version,
            "streaming_batch_id": batch_id,
        }
        if result.metrics:
            details["streaming_metrics"] = dict(result.metrics)
        if self._batches:
            details["streaming_batches"] = [dict(item) for item in self._batches]
        result.merge_details(details)
        if self._validation is not None:
            validation = self._validation
            validation.ok = result.ok
            validation.errors = list(result.errors)
            validation.warnings = list(result.warnings)
            validation.metrics = dict(result.metrics)
            validation.schema = dict(result.schema)
            validation.status = result.status
            validation.reason = result.reason
            validation.merge_details(details)
            self._validation = validation
        else:
            self._validation = result

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> ValidationResult:
        """Validate a micro-batch and update the attached validation."""
        timestamp = datetime.now(timezone.utc)
        effective_version = resolve_dataset_version(self.dataset_version, batch_id, timestamp)
        
        debug_log_path = os.environ.get("DC43_STREAMING_DEBUG_LOG")
        if debug_log_path:
            try:
                import json
                with open(debug_log_path, "a") as f:
                    f.write(json.dumps({"event": "start_batch", "batch_id": batch_id, "dataset": f"{self.dataset_id}@{effective_version}", "timestamp": str(timestamp)}) + "\n")
            except Exception:
                pass

        logger.info(f"DC43: Starting to process streaming batch {batch_id} for dataset {self.dataset_id}@{effective_version}")
        
        try:
            schema, metrics = collect_observations(
                batch_df,
                self.contract,
                expectations=self.expectation_plan,
                collect_metrics=True,
            )
            logger.info(f"DC43: Extracted schema and metrics for batch {batch_id}: {metrics}")
            if debug_log_path:
                try:
                    import json
                    with open(debug_log_path, "a") as f:
                        f.write(json.dumps({"event": "metrics_collected", "batch_id": batch_id, "metrics": metrics}) + "\n")
                except Exception:
                    pass
        except Exception as e:
            logger.exception(f"DC43: Failed to collect observations for batch {batch_id}: {e}")
            if debug_log_path:
                try:
                    import json
                    with open(debug_log_path, "a") as f:
                        f.write(json.dumps({"event": "metrics_error", "batch_id": batch_id, "error": str(e)}) + "\n")
                except Exception:
                    pass
            schema, metrics = None, {}
        
        row_count = metrics.get("row_count")
        if isinstance(row_count, (int, float)) and row_count <= 0:
            logger.info(
                "Skipping empty streaming batch %s for %s@%s. (Check your Spark stream checkpoint, 0 rows were read in this micro-batch)",
                batch_id,
                self.dataset_id,
                effective_version,
            )
            self._latest_batch_id = batch_id
            validation = self._validation
            if validation is None:
                validation = ValidationResult(ok=True, errors=[], warnings=[])
            validation.merge_details(
                {
                    "dataset_id": self.dataset_id,
                    "dataset_version": effective_version,
                    "streaming_batch_id": batch_id,
                }
            )
            self._validation = validation
            self._record_batch(
                batch_id=batch_id,
                metrics={},
                row_count=0,
                status="idle",
                timestamp=timestamp,
            )
            return validation

        cid, cver = contract_identity(self.contract)
        def _obs(): return ObservationPayload(metrics=dict(metrics or {}), schema=dict(schema or {}), reused=False)
        
        result = None
        if self.governance_service is not None:
            try:
                logger.info(f"DC43: Using governance service '{type(self.governance_service).__name__}' to evaluate batch {batch_id}")
                assessment = self.governance_service.evaluate_dataset(
                    contract_id=cid,
                    contract_version=cver,
                    dataset_id=self.dataset_id,
                    dataset_version=effective_version,
                    validation=None,
                    observations=_obs,
                    pipeline_context=self.pipeline_context,
                    operation="write",
                )
                result = assessment.validation or assessment.status
                logger.info(
                    "DC43: Successfully evaluated streaming batch %s (effective_version=%s) on governance service. Got %d errors, %d metrics",
                    batch_id, effective_version, len(result.errors if result else []), len(result.metrics if result else {})
                )
                if debug_log_path:
                    try:
                        import json
                        with open(debug_log_path, "a") as f:
                            f.write(json.dumps({
                                "event": "governance_evaluation", 
                                "batch_id": batch_id, 
                                "backend": type(self.governance_service).__name__,
                                "ok": result.ok if result else None,
                                "violations": len(result.errors) if result and result.errors else 0
                            }) + "\n")
                    except Exception:
                        pass
            except Exception as e:
                logger.exception(f"DC43: Streaming observation service evaluation failed for batch {batch_id}, falling back to offline: {e}")
                if debug_log_path:
                    try:
                        import json
                        with open(debug_log_path, "a") as f:
                            f.write(json.dumps({"event": "governance_error", "batch_id": batch_id, "error": str(e)}) + "\n")
                    except Exception:
                        pass

        if result is None:
            logger.info(f"DC43: Falling back to offline evaluation for batch {batch_id}")
            result = self._evaluate_without_service(schema=schema, metrics=metrics)
            logger.info(f"DC43: Offline fallback evaluation generated {len(result.errors if result else [])} errors, {len(result.metrics if result else {})} metrics.")
        self._latest_batch_id = batch_id
        status = "ok"
        if result.errors:
            status = "error"
        elif result.warnings:
            status = "warning"
        self._record_batch(
            batch_id=batch_id,
            metrics=result.metrics or metrics,
            row_count=int(metrics.get("row_count", 0) or 0),
            status=status,
            timestamp=timestamp,
            errors=result.errors if result.errors else None,
            warnings=result.warnings if result.warnings else None,
        )
        self._merge_batch_details(result, batch_id=batch_id, effective_dataset_version=effective_version)

        if self.enforce and not result.ok:
            self._stop_sink_queries()
            raise ValueError(
                "Streaming batch %s failed data-quality validation: %s"
                % (batch_id, result.errors)
            )

        decision = self._intervention.decide(
            StreamingInterventionContext(
                batch_id=batch_id,
                validation=result,
                dataset_id=self.dataset_id,
                dataset_version=effective_version,
            )
        )
        if decision:
            if self._batches:
                self._batches[-1]["intervention"] = decision
            batches_payload = [dict(item) for item in self._batches]
            if self._validation is not None:
                self._validation.merge_details({"streaming_batches": batches_payload})
            result.merge_details({"streaming_batches": batches_payload})
            reason_details = {"streaming_intervention_reason": decision}
            if self._validation is not None:
                self._validation.merge_details(reason_details)
            result.merge_details(reason_details)
            self._notify_progress(
                {
                    "type": "intervention",
                    "batch_id": batch_id,
                    "reason": decision,
                }
            )
            self._stop_sink_queries()
            raise StreamingInterventionError(decision)

        return result

    def start(self, df: DataFrame, *, output_mode: str, modifier: Optional[Callable[[Any], Any]] = None) -> "StreamingQuery":
        """Start the observation writer for ``df`` and return its query handle."""
        if self._active:
            raise RuntimeError("StreamingObservationWriter can only be started once")
        self._active = True

        def _run(batch_df: DataFrame, batch_id: int) -> None:
            self.process_batch(batch_df, batch_id)

        writer = df.writeStream.foreachBatch(_run).outputMode(output_mode)
        writer = writer.option("checkpointLocation", self.checkpoint_location)
        if self.query_name:
            writer = writer.queryName(self.query_name)
        if modifier:
            writer = modifier(writer)
        query = writer.start()
        try:
            query_name = query.name  # type: ignore[attr-defined]
        except AttributeError:
            query_name = self.query_name
        try:
            query_id = query.id  # type: ignore[attr-defined]
        except AttributeError:
            query_id = ""
        self._notify_progress(
            {
                "type": "observer-started",
                "query_name": query_name,
                "id": query_id,
            }
        )
        return query

    def _evaluate_without_service(
        self,
        *,
        schema: Mapping[str, Mapping[str, Any]],
        metrics: Mapping[str, Any],
    ) -> ValidationResult:
        """Return a validation outcome without a dedicated DQ service."""
        violation_counts: Dict[str, float] = {}
        for key, value in metrics.items():
            if not isinstance(key, str) or not key.startswith("violations."):
                continue
            suffix = key.partition(".")[2]
            try:
                violation_counts[suffix] = float(value)
            except (TypeError, ValueError):
                continue

        errors: list[str] = []
        warnings: list[str] = []
        for descriptor in self.expectation_plan:
            if not isinstance(descriptor, Mapping):
                continue
            key = descriptor.get("key")
            if not isinstance(key, str):
                continue
            count = violation_counts.get(key, 0.0)
            if count <= 0:
                continue
            message = f"Expectation {key} reported {int(count)} violation(s)"
            if bool(descriptor.get("optional")):
                warnings.append(message)
            else:
                errors.append(message)

        ok = not errors
        status = "ok"
        if errors:
            status = "block"
        elif warnings:
            status = "warn"

        return ValidationResult(
            ok=ok,
            errors=errors,
            warnings=warnings,
            metrics=dict(metrics),
            schema=dict(schema),
            status=status,
        )
