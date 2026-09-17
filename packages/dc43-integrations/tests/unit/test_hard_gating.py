from unittest.mock import MagicMock
import pytest
from types import SimpleNamespace

from dc43_integrations.spark.io.common import GovernanceSparkWriteRequest
from dc43_integrations.spark.io.write import write_with_governance
from dc43_integrations.spark.io.merge import merge_with_governance
from dc43_integrations.spark.io.interceptors import BaseGovernanceInterceptor, InterceptorContext
from dc43_service_clients.governance.models import GovernanceWriteContext, ResolvedWritePlan
from dc43_service_clients.data_quality import ValidationResult
from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject, SchemaProperty


class FailingPostWriteInterceptor(BaseGovernanceInterceptor):
    def post_write(self, context: InterceptorContext, result: object) -> None:
        raise RuntimeError("Unity Catalog tagging service unavailable")


def make_contract():
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.contract",
        name="Contract",
        status="active",
        schema=[
            SchemaObject(
                name="table",
                properties=[
                    SchemaProperty(name="id", physicalType="bigint", required=True, primaryKey=True),
                    SchemaProperty(name="name", physicalType="string", required=True),
                    SchemaProperty(name="event_date", physicalType="date", partitioned=True),
                ]
            )
        ]
    )


def test_post_write_interceptor_error_captured_when_enforce_false(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        contract_interceptors=[FailingPostWriteInterceptor()],
    )

    result = write_with_governance(
        df=mock_df,
        request=request,
        governance_service=mock_gov,
        enforce=False,
        auto_cast=False,
    )

    assert result.validation is not None
    assert result.validation.ok is False
    assert any("FailingPostWriteInterceptor" in err for err in result.validation.errors)


def test_post_write_interceptor_error_raises_when_enforce_true(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        contract_interceptors=[FailingPostWriteInterceptor()],
    )

    with pytest.raises(RuntimeError, match="Unity Catalog tagging service unavailable"):
        write_with_governance(
            df=mock_df,
            request=request,
            governance_service=mock_gov,
            enforce=True,
            auto_cast=False,
        )


def test_ddl_modifier_and_table_properties_applied_on_write(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df
    mock_spark.catalog.tableExists.return_value = False

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        ddl_modifier=lambda sql: sql + "\n-- custom comment",
        table_properties={"delta.autoOptimize.optimizeWrite": "true"},
    )

    result = write_with_governance(
        df=mock_df,
        request=request,
        governance_service=mock_gov,
        enforce=False,
        auto_cast=True,
    )

    assert result.validation.ok is True
    # Verify spark.sql was called with the generated DDL including modifier and table properties
    mock_spark.sql.assert_called()
    executed_sql = mock_spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS `orders`" in executed_sql
    assert "PARTITIONED BY (`event_date`)" in executed_sql
    assert "CONSTRAINT pk_orders PRIMARY KEY (`id`)" in executed_sql
    assert "'delta.autoOptimize.optimizeWrite' = 'true'" in executed_sql
    assert "-- custom comment" in executed_sql


def test_ddl_modifier_and_table_properties_applied_on_merge(monkeypatch):
    import sys
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df
    mock_spark.catalog.tableExists.return_value = False

    # Mock delta.tables
    mock_dt_module = MagicMock()
    mock_dt = MagicMock()
    mock_dt_module.DeltaTable.forName.return_value = mock_dt
    mock_alias_target = MagicMock()
    mock_dt.alias.return_value = mock_alias_target
    mock_merge_builder = MagicMock()
    mock_alias_target.merge.return_value = mock_merge_builder

    monkeypatch.setitem(sys.modules, "delta", MagicMock())
    monkeypatch.setitem(sys.modules, "delta.tables", mock_dt_module)

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        ddl_modifier=lambda sql: sql + "\n-- custom merge comment",
        table_properties={"delta.autoOptimize.optimizeWrite": "true"},
    )

    result = merge_with_governance(
        source_df=mock_df,
        condition="target.id = source.id",
        request=request,
        governance_service=mock_gov,
        merge_builder_modifier=lambda b: b,
        enforce=False,
        auto_cast=True,
    )

    assert result.validation.ok is True
    # Verify spark.sql was called with the generated DDL including modifier and table properties
    mock_spark.sql.assert_called()
    executed_sql = mock_spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS `orders`" in executed_sql
    assert "PARTITIONED BY (`event_date`)" in executed_sql
    assert "CONSTRAINT pk_orders PRIMARY KEY (`id`)" in executed_sql
    assert "'delta.autoOptimize.optimizeWrite' = 'true'" in executed_sql
    assert "-- custom merge comment" in executed_sql

    # Verify merge was executed
    mock_dt_module.DeltaTable.forName.assert_called_with(mock_spark, "orders")
    mock_merge_builder.execute.assert_called_once()


def test_ddl_builder_applied_on_merge_with_path(monkeypatch):
    import sys
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df

    # Mock delta.tables
    mock_dt_module = MagicMock()
    mock_dt_module.DeltaTable.isDeltaTable.return_value = False
    mock_dt = MagicMock()
    mock_dt_module.DeltaTable.forPath.return_value = mock_dt
    mock_alias_target = MagicMock()
    mock_dt.alias.return_value = mock_alias_target
    mock_merge_builder = MagicMock()
    mock_alias_target.merge.return_value = mock_merge_builder

    monkeypatch.setitem(sys.modules, "delta", MagicMock())
    monkeypatch.setitem(sys.modules, "delta.tables", mock_dt_module)

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        dataset_path="/data/orders_delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        path="/data/orders_delta",
    )

    result = merge_with_governance(
        source_df=mock_df,
        condition="target.id = source.id",
        request=request,
        governance_service=mock_gov,
        merge_builder_modifier=lambda b: b,
        enforce=False,
        auto_cast=True,
    )

    assert result.validation.ok is True
    # Verify spark.sql was called with delta.`path` DDL
    mock_spark.sql.assert_called()
    executed_sql = mock_spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS delta.`/data/orders_delta`" in executed_sql
    mock_dt_module.DeltaTable.forPath.assert_called_with(mock_spark, "/data/orders_delta")
    mock_merge_builder.execute.assert_called_once()


def test_ddl_builder_merge_fallback_to_empty_df_when_sql_fails(monkeypatch):
    import sys
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df
    mock_spark.catalog.tableExists.return_value = False
    mock_spark.sql.side_effect = RuntimeError("DDL execution failed")

    mock_empty_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_empty_df

    mock_dt_module = MagicMock()
    mock_dt = MagicMock()
    mock_dt_module.DeltaTable.forName.return_value = mock_dt
    mock_alias_target = MagicMock()
    mock_dt.alias.return_value = mock_alias_target
    mock_merge_builder = MagicMock()
    mock_alias_target.merge.return_value = mock_merge_builder

    monkeypatch.setitem(sys.modules, "delta", MagicMock())
    monkeypatch.setitem(sys.modules, "delta.tables", mock_dt_module)

    contract = make_contract()

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
    )

    result = merge_with_governance(
        source_df=mock_df,
        condition="target.id = source.id",
        request=request,
        governance_service=mock_gov,
        merge_builder_modifier=lambda b: b,
        enforce=False,
        auto_cast=True,
    )

    assert result.validation.ok is True
    # Fallback to createDataFrame and saveAsTable
    mock_spark.createDataFrame.assert_called()
    mock_empty_df.write.format.assert_called_with("delta")
    mock_empty_df.write.format().mode.assert_called_with("append")
    mock_empty_df.write.format().mode().saveAsTable.assert_called_with("orders")
