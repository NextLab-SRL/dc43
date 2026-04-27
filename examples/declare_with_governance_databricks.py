# Databricks notebook / Python Script Example
# How to declare a governed permanent View in Databricks using dc43's `declare_with_governance`

from pyspark.sql import SparkSession

from dc43_integrations.spark.io.common import (
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    GovernanceDeclareContext,
)
from dc43_integrations.spark.io.common import GovernanceSparkDeclareRequest
from dc43_integrations.spark.io.declare import declare_with_governance
from dc43_integrations.spark.io.locators import ContractVersionLocator

from dc43_service_clients.governance.client.interface import GovernanceServiceClient

# This dummy client represents your initialized governance service.
# Replace with the actual ServiceClient instantiation in your environment.
governance_service: GovernanceServiceClient = None  # TODO: Initialize your Governance Service Client

spark = SparkSession.builder.getOrCreate()


def deploy_my_view():
    """
    This script demonstrates how to deploy a View that transforms our CRM data 
    and joins it with a lookup table, all while remaining under the control of 
    Data Governance (Data Quality validations, metrics mapping, and publication status checks).

    All `{placeholders}` are going to be natively resolved to proper Databricks catalog objects
    (including Time Travel `VERSION AS OF` syntaxes if specified in the Contract locators).
    """
    
    # 1. Define the declarative SQL template for the view.
    # Notice that instead of putting the concrete Databricks catalog identifiers here
    # (e.g., `FROM main.prod.users`), we use bracketed `{placeholders}` which correspond 
    # to the mapped inputs.
    sql_template = """
        SELECT 
            u.user_id,
            u.account_tier,
            u.created_at,
            t.tier_multiplier
        FROM {users_table} u
        LEFT JOIN {tiers_lookup} t ON u.account_tier = t.tier_name
        WHERE u.status = 'ACTIVE'
    """
    
    # 2. Execute the Declaration
    print("Starting Governed View Declaration...")
    
    result = declare_with_governance(
        spark=spark,
        sql_template=sql_template,
        
        # 3. Inputs definition
        # The `declare_with_governance` method evaluates all inputs as standard DataFrame reads internally.
        # This triggers Input Data Quality expectations BEFORE creating the view.
        # If the input data violates its target Status policy, the view declaration will abort.
        inputs={
            "users_table": GovernanceSparkReadRequest(
                dataset_id="contract-crm-users",
                # Using a version locator allows the engine to parse Time Travel correctly!
                # If 'latest' resolves to a prior commit version on the contract, the engine will
                # seamlessly inject `VERSION AS OF X` in the background when translating the {users_table} SQL.
                dataset_locator=ContractVersionLocator(
                    dataset_id="contract-crm-users",
                    dataset_version="latest"
                )
            ),
            "tiers_lookup": GovernanceSparkReadRequest(
                dataset_id="contract-lookup-tiers"
            )
        },
        
        # 4. Output definition (The View itself)
        # The resulting view will be pushed to the Unity Catalog/Hive Metastore based on the
        # output contract's Server definition. The target contract MUST provide a standard `table` 
        # definition (e.g. `catalog.schema.table`), otherwise view creation will raise an exception.
        request=GovernanceSparkDeclareRequest(
            context=GovernanceDeclareContext(),
            dataset_id="contract-output-crm-view"
        ),
        
        governance_service=governance_service,
        enforce=True  # Ensure Data Product Status and Quality bounds are strictly respected
    )
    
    # Check if the execution returned validation failures
    if result.status and not result.status.success:
        print("View deployment flagged as non-compliant according to governance policies.")
    else:
        print("Governed View deployed successfully!")


if __name__ == "__main__":
    deploy_my_view()
