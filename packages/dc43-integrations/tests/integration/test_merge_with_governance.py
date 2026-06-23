from pathlib import Path
from datetime import datetime
import sys
import pytest
from unittest.mock import MagicMock

from dc43_integrations.spark.io.merge import merge_with_governance
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
