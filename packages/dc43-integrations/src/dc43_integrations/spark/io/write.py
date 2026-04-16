from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Type,
    Union,
    overload,
    Literal,
)
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession

from dc43_integrations.spark.io.common import (
    GovernanceSparkWriteRequest,
)
from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteViolationStrategy,
)
from dc43_integrations.spark.io.base import (
    BaseWriteExecutor,
    WriteExecutionResult,
)
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance.models import GovernanceWriteContext, GovernancePolicy

logger = logging.getLogger(__name__)


def write_with_governance(
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> WriteExecutionResult:
    """Write a DataFrame allowing governance service to resolve policy."""

    if not isinstance(request, GovernanceSparkWriteRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkWriteRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkWriteRequest or mapping")

    violation_strategy = request.violation_strategy or NoOpWriteViolationStrategy()
    context = request.context
    if not isinstance(context, GovernanceWriteContext):
        context = GovernanceWriteContext(**dict(context))
    
    if context.policy is None:
        context.policy = GovernancePolicy()

    if (
        context.policy.allowed_data_product_statuses is None
        and isinstance(violation_strategy, WriteViolationStrategy)
        and hasattr(violation_strategy, 'allowed_data_product_statuses')
    ):
        allowed = violation_strategy.allowed_data_product_statuses
        if isinstance(allowed, str):
            context.policy.allowed_data_product_statuses = (allowed,)
        else:
            context.policy.allowed_data_product_statuses = tuple(allowed)
    if (
        context.policy.allow_missing_data_product_status is None
        and isinstance(violation_strategy, WriteViolationStrategy)
        and hasattr(violation_strategy, 'allow_missing_data_product_status')
    ):
        context.policy.allow_missing_data_product_status = bool(
            violation_strategy.allow_missing_data_product_status
        )
    if (
        context.policy.data_product_status_case_insensitive is None
        and isinstance(violation_strategy, WriteViolationStrategy)
        and hasattr(violation_strategy, 'data_product_status_case_insensitive')
    ):
        context.policy.data_product_status_case_insensitive = bool(
            violation_strategy.data_product_status_case_insensitive
        )
    if (
        context.policy.data_product_status_failure_message is None
        and isinstance(violation_strategy, WriteViolationStrategy)
        and hasattr(violation_strategy, 'data_product_status_failure_message')
    ):
        failure_message = violation_strategy.data_product_status_failure_message
        if failure_message is not None:
            context.policy.data_product_status_failure_message = str(failure_message)
    if context.policy.enforce_data_product_status is None:
        context.policy.enforce_data_product_status = bool(enforce)

    plan = governance_service.resolve_write_context(context=context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    executor = BaseWriteExecutor(
        df=df,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        streaming_batch_callback=streaming_batch_callback,
        plan=plan,
    )
    return executor.execute()

