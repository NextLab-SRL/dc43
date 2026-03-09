from __future__ import annotations

import logging
from typing import (
    Any,
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

from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.data_products import (
    DataProductServiceClient,
    DataProductInputBinding,
)
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance.models import (
    ResolvedReadPlan,
    GovernanceReadContext,
    GovernancePolicy,
)

from dc43_integrations.spark.io.common import (
    GovernanceSparkReadRequest,
)

from dc43_integrations.spark.io.base import (
    BatchReadExecutor,
    StreamingReadExecutor,
    _execute_read,
)

logger = logging.getLogger(__name__)




@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Read a DataFrame relying solely on governance context resolution."""

    if not isinstance(request, GovernanceSparkReadRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkReadRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkReadRequest or mapping")

    strategy = getattr(request, 'status_strategy', None)
    context = request.context
    if not isinstance(context, GovernanceReadContext):
        context = GovernanceReadContext(**dict(context))
    
    if context.policy is None:
        context.policy = GovernancePolicy()

    if (
        context.policy.allowed_data_product_statuses is None
        and strategy is not None
        and hasattr(strategy, 'allowed_data_product_statuses')
    ):
        allowed = strategy.allowed_data_product_statuses
        if isinstance(allowed, str):
            context.policy.allowed_data_product_statuses = (allowed,)
        else:
            context.policy.allowed_data_product_statuses = tuple(allowed)
    if (
        context.policy.allow_missing_data_product_status is None
        and strategy is not None
        and hasattr(strategy, 'allow_missing_data_product_status')
    ):
        context.policy.allow_missing_data_product_status = bool(
            strategy.allow_missing_data_product_status
        )
    if (
        context.policy.data_product_status_case_insensitive is None
        and strategy is not None
        and hasattr(strategy, 'data_product_status_case_insensitive')
    ):
        context.policy.data_product_status_case_insensitive = bool(
            strategy.data_product_status_case_insensitive
        )
    if (
        context.policy.data_product_status_failure_message is None
        and strategy is not None
        and hasattr(strategy, 'data_product_status_failure_message')
    ):
        failure_message = strategy.data_product_status_failure_message
        if failure_message is not None:
            context.policy.data_product_status_failure_message = str(failure_message)
    if context.policy.enforce_data_product_status is None:
        context.policy.enforce_data_product_status = bool(enforce)

    plan = governance_service.resolve_read_context(context=context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    return _execute_read(
        BatchReadExecutor,
        spark=spark,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        return_status=return_status,
        plan=plan,
    )





@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Streaming variant of :func:`read_with_governance`."""

    if not isinstance(request, GovernanceSparkReadRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkReadRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkReadRequest or mapping")

    context = request.context
    if not isinstance(context, GovernanceReadContext):
        context = GovernanceReadContext(**dict(context))
    
    plan = governance_service.resolve_read_context(context=context)
    pipeline_ctx = context.pipeline_context or plan.pipeline_context

    return _execute_read(
        StreamingReadExecutor,
        spark=spark,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        return_status=return_status,
        plan=plan,
    )



