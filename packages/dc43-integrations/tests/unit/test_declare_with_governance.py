from unittest.mock import MagicMock
import pytest
from types import SimpleNamespace

from dc43_integrations.spark.io import (
    declare_with_governance,
    BaseDeclareExecutor,
    GovernanceSparkReadRequest,
    GovernanceSparkDeclareRequest,
    GovernanceDeclareContext,
    build_spark_sql_ref,
)
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_integrations.spark.io.resolution import DatasetResolution
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)


def _make_contract(contract_id: str, table: str = "catalog.schema.tbl"):
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id=contract_id,
        name=f"Contract {contract_id}",
        status="active",
        servers=[Server(server="local", type="delta", path=table, format="delta")],
        schema=[
            SchemaObject(
                name="table",
                properties=[
                    SchemaProperty(name="CTYPARTICL", physicalType="string"),
                ],
            )
        ],
    )


def test_build_spark_sql_ref_table():
    res = DatasetResolution(
        path=None,
        table="my_catalog.my_schema.site_acores",
        format="delta",
        dataset_id="contract-1",
        dataset_version="1.0.0",
    )
    assert build_spark_sql_ref(res) == "my_catalog.my_schema.site_acores"


def test_build_spark_sql_ref_path():
    res = DatasetResolution(
        path="abfss://container@account.dfs.core.windows.net/data/site_acores",
        table=None,
        format="delta",
        dataset_id="contract-1",
        dataset_version="1.0.0",
    )
    assert build_spark_sql_ref(res) == "delta.`abfss://container@account.dfs.core.windows.net/data/site_acores`"


def test_build_spark_sql_ref_time_travel():
    res = DatasetResolution(
        path=None,
        table="catalog.schema.tbl",
        format="delta",
        dataset_id="contract-1",
        dataset_version="1.0.0",
        read_options={"versionAsOf": "5"},
    )
    assert build_spark_sql_ref(res) == "catalog.schema.tbl VERSION AS OF 5"


def test_declare_with_governance_executes_successfully(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df
    mock_spark.read = MagicMock()
    mock_reader = MagicMock()
    mock_spark.read.format.return_value = mock_reader
    mock_reader.table.return_value = mock_df
    mock_reader.load.return_value = mock_df
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["CTYPARTICL"]
    mock_df.select.return_value = mock_df

    input_contract = _make_contract("019fc75f-0ffb-771e-aa06-9e33a91f9c77", table="raw.site_acores")
    output_contract = _make_contract("testview", table="views.site_acores_view")

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None,
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.side_effect = lambda id, **kw: input_contract if id == input_contract.id else output_contract
    mock_gov.resolve_read_context.return_value = ResolvedReadPlan(
        contract=input_contract,
        contract_id=input_contract.id,
        contract_version=input_contract.version,
        dataset_id=input_contract.id,
        dataset_version=input_contract.version,
    )
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=output_contract,
        contract_id=output_contract.id,
        contract_version=output_contract.version,
        dataset_id=output_contract.id,
        dataset_version=output_contract.version,
    )

    sql_template = """
        SELECT 
            c.CTYPARTICL
        FROM {site_acores} c
    """

    validation = declare_with_governance(
        spark=mock_spark,
        sql_template=sql_template,
        inputs={
            "site_acores": GovernanceSparkReadRequest(
                context=GovernanceReadContext.from_contract(
                    id="019fc75f-0ffb-771e-aa06-9e33a91f9c77",
                    version="P1_Dev 0.0.2",
                )
            )
        },
        request=GovernanceSparkDeclareRequest(
            context=GovernanceWriteContext.from_contract(id="testview", version="v0.1")
        ),
        governance_service=mock_gov,
        enforce=False,
        auto_cast=False,
    )

    assert validation is not None
    # Check that spark.sql was invoked with CREATE OR REPLACE VIEW
    calls = [str(call) for call in mock_spark.sql.call_args_list]
    assert any("CREATE OR REPLACE VIEW views.site_acores_view AS" in call for call in calls)


def test_declare_with_governance_multi_input_join(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df
    mock_spark.read = MagicMock()
    mock_reader = MagicMock()
    mock_spark.read.format.return_value = mock_reader
    mock_reader.table.return_value = mock_df
    mock_reader.load.return_value = mock_df
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "val"]
    mock_df.select.return_value = mock_df

    contract_a = _make_contract("contract_a", table="raw.table_a")
    contract_b = _make_contract("contract_b", table="raw.table_b")
    output_contract = _make_contract("output_view", table="views.joined_view")

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None,
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    contracts = {
        "contract_a": contract_a,
        "contract_b": contract_b,
        "output_view": output_contract,
    }
    mock_gov.resolve_contract.side_effect = lambda id, **kw: contracts[id]
    mock_gov.resolve_read_context.side_effect = lambda context: ResolvedReadPlan(
        contract=contracts[context.contract_id],
        contract_id=context.contract_id,
        contract_version="1.0.0",
        dataset_id=context.contract_id,
        dataset_version="1.0.0",
    )
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=output_contract,
        contract_id=output_contract.id,
        contract_version=output_contract.version,
        dataset_id=output_contract.id,
        dataset_version=output_contract.version,
    )

    sql_template = """
        SELECT a.id, b.val
        FROM {input_a} a
        JOIN {input_b} b ON a.id = b.id
    """

    validation = declare_with_governance(
        spark=mock_spark,
        sql_template=sql_template,
        inputs={
            "input_a": GovernanceSparkReadRequest(
                context=GovernanceReadContext.from_contract(id="contract_a")
            ),
            "input_b": {
                "context": GovernanceReadContext.from_contract(id="contract_b")
            },
        },
        request={
            "context": GovernanceWriteContext.from_contract(id="output_view")
        },
        governance_service=mock_gov,
        enforce=False,
    )

    assert validation is not None
    calls = [str(call) for call in mock_spark.sql.call_args_list]
    assert any("CREATE OR REPLACE VIEW views.joined_view AS" in call for call in calls)

