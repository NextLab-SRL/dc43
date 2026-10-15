from pathlib import Path
from datetime import datetime
import sys
import pytest
from unittest.mock import MagicMock

from dc43_integrations.spark.io.merge import merge_with_governance, merge_stream_with_governance
from dc43_integrations.spark.io.common import GovernanceSparkWriteRequest
from dc43_service_clients.governance.models import GovernanceWriteContext, ContractReference
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.governance import build_local_governance_service
from tests.helpers.orders import build_orders_contract

def persist_contract(tmp_path: Path, contract):
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return store, LocalContractServiceClient(store), LocalDataQualityServiceClient()

@pytest.fixture(autouse=True)
def mock_delta_tables_module():
    mock_module = MagicMock()
    sys.modules['delta'] = MagicMock()
    sys.modules['delta.tables'] = mock_module
    yield mock_module
    del sys.modules['delta.tables']
    del sys.modules['delta']

def test_merge_with_governance_delta(mock_delta_tables_module, spark, tmp_path: Path):
    dest_dir = tmp_path / "target_table"
    
    # Setup mocks
    mock_delta_table = mock_delta_tables_module.DeltaTable
    mock_dt = MagicMock()
    mock_delta_table.forPath.return_value = mock_dt
    mock_alias_target = MagicMock()
    mock_dt.alias.return_value = mock_alias_target
    mock_merge_builder = MagicMock()
    mock_alias_target.merge.return_value = mock_merge_builder
    
    contract = build_orders_contract(str(dest_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)
    
    # Mock source dataframe
    source_df = spark.createDataFrame([
        (1, 101, datetime(2024, 1, 1, 11, 0, 0), 20.0, "EUR")
    ], ["order_id", "customer_id", "order_ts", "amount", "currency"])

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract=ContractReference(
                contract_id=contract.id,
                contract_version=contract.version,
            )
        ),
        path=str(dest_dir),
        format="delta",
    )

    def merge_builder_modifier(builder):
        builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll()
        return builder

    exec_result = merge_with_governance(
        source_df=source_df,
        condition="target.order_id = source.order_id",
        request=request,
        governance_service=governance,
        merge_builder_modifier=merge_builder_modifier
    )
    
    assert getattr(exec_result, 'result', getattr(exec_result, 'validation', None) if not hasattr(exec_result, 'result') else exec_result).ok
    
    # Verify DeltaTable was invoked properly
    mock_delta_table.forPath.assert_called_once()
    mock_dt.alias.assert_called_with("target")
    mock_alias_target.merge.assert_called_once()
    
    # Verify that the builder was modified and executed
    mock_merge_builder.whenMatchedUpdateAll.assert_called_once()
    mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()
    mock_merge_builder.execute.assert_called_once()


def test_merge_with_governance_streaming(mock_delta_tables_module, spark, tmp_path: Path):
    dest_dir = tmp_path / "target_streaming_table"

    mock_delta_table = mock_delta_tables_module.DeltaTable
    mock_dt = MagicMock()
    mock_delta_table.forPath.return_value = mock_dt
    mock_alias_target = MagicMock()
    mock_dt.alias.return_value = mock_alias_target
    mock_merge_builder = MagicMock()
    mock_alias_target.merge.return_value = mock_merge_builder

    contract = build_orders_contract(str(dest_dir))
    store, _, _ = persist_contract(tmp_path, contract)
    governance = build_local_governance_service(store)

    sample_df = spark.createDataFrame([
        (1, 101, datetime(2024, 1, 1, 11, 0, 0), 20.0, "EUR")
    ], ["order_id", "customer_id", "order_ts", "amount", "currency"])

    # Setup mock streaming DataFrame
    mock_df = MagicMock()
    mock_df.sparkSession = spark
    mock_df.sql_ctx.sparkSession = spark
    mock_df.isStreaming = True
    mock_df.schema = sample_df.schema
    mock_df.columns = sample_df.columns
    mock_df.select.return_value = mock_df

    mock_writer = MagicMock()
    mock_df.writeStream.foreachBatch.return_value = mock_writer
    mock_writer.outputMode.return_value = mock_writer
    mock_writer.options.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.queryName.return_value = mock_writer
    mock_query = MagicMock()
    mock_writer.start.return_value = mock_query

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract=ContractReference(
                contract_id=contract.id,
                contract_version=contract.version,
            )
        ),
        path=str(dest_dir),
        format="delta",
        options={"checkpointLocation": "/tmp/chk"},
    )

    def merge_builder_modifier(builder):
        builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll()
        return builder

    exec_result = merge_stream_with_governance(
        source_df=mock_df,
        condition="target.order_id = source.order_id",
        request=request,
        governance_service=governance,
        merge_builder_modifier=merge_builder_modifier,
        enforce=False,
    )

    assert exec_result.result.ok
    assert mock_query in exec_result.streaming_queries
    assert mock_df.writeStream.foreachBatch.call_count == 2
    mock_writer.options.assert_called_with(checkpointLocation="/tmp/chk")
    assert mock_writer.start.call_count == 2

    # Extract the micro-batch callback passed to foreachBatch (first call is the merge sink)
    micro_batch_fn = mock_df.writeStream.foreachBatch.call_args_list[0][0][0]

    # Verify that the callback does not capture unpicklable SparkSession/DataFrame in its closure
    import cloudpickle
    assert cloudpickle.dumps(micro_batch_fn) is not None

    # Test executing a normal micro-batch
    batch_df = spark.createDataFrame([
        (1, 101, datetime(2024, 1, 1, 11, 0, 0), 20.0, "EUR")
    ], ["order_id", "customer_id", "order_ts", "amount", "currency"])

    micro_batch_fn(batch_df, 1)

    mock_delta_table.forPath.assert_called()
    mock_alias_target.merge.assert_called()
    mock_merge_builder.whenMatchedUpdateAll.assert_called()
    mock_merge_builder.whenNotMatchedInsertAll.assert_called()
    mock_merge_builder.execute.assert_called()

    # Test empty batch is skipped
    mock_merge_builder.execute.reset_mock()
    empty_df = MagicMock()
    empty_df.isEmpty.return_value = True
    micro_batch_fn(empty_df, 2)
    mock_merge_builder.execute.assert_not_called()
