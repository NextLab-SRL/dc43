from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
)

from pyspark.sql import DataFrame

from dc43_integrations.spark.io.common import (
    GovernanceSparkWriteRequest,
)
from dc43_integrations.spark.io.base import (
    BaseWriteExecutor,
    WriteExecutionResult,
    _ensure_sink_table_exists,
)
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance.models import GovernanceWriteContext, GovernancePolicy
from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteViolationStrategy,
)

logger = logging.getLogger(__name__)


class BaseMergeExecutor(BaseWriteExecutor):
    def __init__(
        self,
        *,
        df: DataFrame,
        condition: str,
        merge_builder_modifier: Callable[[Any], Any],
        **kwargs: Any,
    ):
        super().__init__(df=df, **kwargs)
        self.condition = condition
        self.merge_builder_modifier = merge_builder_modifier

    def _perform_writes(self, requests, governance_client, enforce, result, assessment):
        from delta.tables import DeltaTable  # type: ignore

        streaming_queries = []
        primary_status = None
        for i, req in enumerate(requests):
            if req.warnings:
                for w in req.warnings:
                    if w not in result.warnings:
                        result.warnings.append(w)
            
            df = req.df
            spark = getattr(df, "sparkSession", getattr(getattr(df, "sql_ctx", None), "sparkSession", None))

            # Pre-create sink table using ContractDDLBuilder to ensure contract DDL conformity
            _ensure_sink_table_exists(req, spark=spark, df=df, default_format="delta", is_merge=True)

            # Setup Governance dataset linking before execution
            if governance_client and req.contract and req.dataset_id:
                try:
                    governance_client.link_dataset_contract(
                        dataset_id=req.dataset_id,
                        dataset_version=req.dataset_version or "",
                        contract_id=req.contract.id,
                        contract_version=req.contract.version,
                    )
                except Exception:
                    logger.exception(
                        "Failed to link dataset %s to contract %s",
                        req.dataset_id, req.contract.id
                    )

            validation = req.validation_factory() if getattr(req, "validation_factory", None) else None
            observation_writer = getattr(req, "streaming_observation_writer", None)
            if observation_writer is not None and validation is not None:
                try:
                    observation_writer.attach_validation(validation)
                except RuntimeError:
                    pass

            is_streaming = bool(getattr(req, "streaming", False) or getattr(df, "isStreaming", False))

            if is_streaming:
                condition = self.condition
                builder_modifier = self.merge_builder_modifier
                table = req.table
                path = req.path

                def _merge_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
                    from delta.tables import DeltaTable  # type: ignore

                    try:
                        if hasattr(batch_df, "isEmpty") and batch_df.isEmpty():
                            return
                    except Exception:
                        pass

                    batch_spark = getattr(
                        batch_df,
                        "sparkSession",
                        getattr(getattr(batch_df, "sql_ctx", None), "sparkSession", None),
                    )
                    if table:
                        dt = DeltaTable.forName(batch_spark, table)
                    else:
                        dt = DeltaTable.forPath(batch_spark, path)

                    builder = dt.alias("target").merge(batch_df.alias("source"), condition)
                    if builder_modifier is not None:
                        builder = builder_modifier(builder)
                    builder.execute()

                writer = df.writeStream.foreachBatch(_merge_micro_batch)
                if req.mode:
                    writer = writer.outputMode(req.mode)
                if req.options:
                    writer = writer.options(**req.options)
                if req.writer_modifier:
                    writer = req.writer_modifier(writer)
                query = writer.start()
                streaming_queries.append(query)

                if observation_writer is not None and not observation_writer.active:
                    metrics_mode = req.mode or "append"
                    metrics_query = observation_writer.start(
                        df,
                        output_mode=metrics_mode,
                        modifier=req.observation_writer_modifier,
                    )
                    streaming_queries.append(metrics_query)
                    observation_writer.watch_sink_query(query)
            else:
                if req.table:
                    dt = DeltaTable.forName(df.sparkSession, req.table)
                else:
                    dt = DeltaTable.forPath(df.sparkSession, req.path)
                
                builder = dt.alias("target").merge(df.alias("source"), self.condition)
                builder = self.merge_builder_modifier(builder)
                builder.execute()
            
            # No primary status for native merges since it's synchronous logic or async stream
            if i == 0: primary_status = None
            
        return primary_status, streaming_queries


def merge_with_governance(
    source_df: DataFrame,
    condition: str,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    merge_builder_modifier: Callable[[Any], Any],
    enforce: bool = True,
    auto_cast: bool = True,
    streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> WriteExecutionResult:
    """Merge a DataFrame into a Delta Lake table allowing governance service to resolve policy."""
    if not isinstance(request, GovernanceSparkWriteRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkWriteRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkWriteRequest or mapping")

    violation_strategy = request.violation_strategy or NoOpWriteViolationStrategy()
    context = request.context
    if not isinstance(context, GovernanceWriteContext):
        context = GovernanceWriteContext(**dict(context))
    request.context = context
    
    if context.policy is None:
        context.policy = GovernancePolicy()

    # Fill defaults from old strategies if needed inside write_with_governance too
    if context.policy.enforce_data_product_status is None:
        context.policy.enforce_data_product_status = bool(enforce)

    plan = governance_service.resolve_write_context(context=context)

    executor = BaseMergeExecutor(
        df=source_df,
        condition=condition,
        merge_builder_modifier=merge_builder_modifier,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        streaming_batch_callback=streaming_batch_callback,
        plan=plan,
    )
    return executor.execute()


merge_stream_with_governance = merge_with_governance
