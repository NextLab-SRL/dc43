from __future__ import annotations

import logging
from typing import (
    Any,
    Mapping,
    Union,
)

from pyspark.sql import DataFrame, SparkSession

from dc43_integrations.spark.io.common import (
    GovernanceSparkDeclareRequest,
    GovernanceSparkReadRequest,
)
from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteViolationStrategy,
)
from dc43_integrations.spark.io.base import (
    BaseDeclareExecutor,
    WriteExecutionResult,
)
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance.models import GovernanceDeclareContext, GovernancePolicy

logger = logging.getLogger(__name__)


def declare_with_governance(
    spark: SparkSession,
    sql_template: str,
    inputs: Mapping[str, Union[GovernanceSparkReadRequest, Mapping[str, object]]],
    request: GovernanceSparkDeclareRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
) -> WriteExecutionResult:
    """Declare a Databricks View enforcing input validations and output governance policies.

    This function introduces declarative permanent view deployment to the governance framework. 
    Rather than writing DataFrames sequentially, it interprets a templated SQL query (the `sql_template`), 
    discovers and evaluates all inputs (applying data quality rules, dataset locators, and time travel versions) 
    in a pre-flight sequence, and creates a Databricks catalog view safely.

    Parameters
    ----------
    spark : SparkSession
        Active Spark Session.
    sql_template : str
        The pure SQL string which defines the view. Use bracket syntax `{key}` for tables/paths you wish
        to govern. Example: `SELECT a.id, b.value FROM {input_a} a JOIN {input_b} b ON a.tag = b.tag`.
    inputs : Mapping[str, Union[GovernanceSparkReadRequest, Mapping[str, object]]]
        A dictionary mapping the templated keys (e.g. "input_a") to GovernanceSparkReadRequest objects. 
        Each dataset locator within these requests will be evaluated to dynamically inject the correct 
        Spark SQL string (with aliases or `VERSION AS OF` options if localized) into the template.
    request : GovernanceSparkDeclareRequest | Mapping[str, object]
        The output request representing the View itself. Defines the output contract, metadata policies, 
        and Data Product Status validations. The output contract's Server definition *must* define a 
        `table` (i.e. `catalog.schema.table` identifier), as Databricks does not allow views to be purely path-based.
    governance_service : GovernanceServiceClient
        The active governance service instance.
    enforce : bool, default True
        If True, non-compliant input or output configurations interrupt execution.
    auto_cast : bool, default True
        Used contextually during governance context resolution.

    Returns
    -------
    WriteExecutionResult
        Details on execution output, validation status, and warnings.

    Example
    -------
    ```python
    from dc43_integrations.spark.io.common import (
        GovernanceSparkReadRequest,
        GovernanceSparkDeclareRequest,
        GovernanceDeclareContext
    )
    from dc43_integrations.spark.io.locators import ContractVersionLocator

    # 1. Define the declarative logic. 
    # Keys like {client_table} and {dim_table} will be fully resolved and injected.
    sql_template = \"\"\"
        SELECT 
            c.client_id, 
            c.revenue, 
            d.segment_name
        FROM {client_table} c
        LEFT JOIN {dim_table} d ON c.segment_id = d.segment_id
    \"\"\"

    # 2. Execute the Declaration
    # The framework will evaluate input Data Quality before creating the target View.
    result = declare_with_governance(
        spark=spark,
        sql_template=sql_template,
        inputs={
            "client_table": GovernanceSparkReadRequest(
                dataset_locator=ContractVersionLocator(
                    dataset_id="contract-clients", 
                    dataset_version="latest"
                )
            ),
            "dim_table": GovernanceSparkReadRequest(
                dataset_id="contract-dimensions"
            )
        },
        request=GovernanceSparkDeclareRequest(
            context=GovernanceDeclareContext(),
            dataset_id="contract-output-client-view"
        ),
        governance_service=governance_service
    )
    ```
    """

    if not isinstance(request, GovernanceSparkDeclareRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkDeclareRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkDeclareRequest or mapping")

    violation_strategy = request.violation_strategy or NoOpWriteViolationStrategy()
    context = request.context
    if not isinstance(context, GovernanceDeclareContext):
        context = GovernanceDeclareContext(**dict(context))
    
    if context.policy is None:
        context.policy = GovernancePolicy()

    # Pre-configure policy from strategy rules
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

    executor = BaseDeclareExecutor(
        spark=spark,
        sql_template=sql_template,
        inputs=inputs,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        plan=plan,
    )
    return executor.execute()
