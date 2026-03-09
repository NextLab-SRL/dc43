"""Tests for integration-helper Spark stub generation."""

from __future__ import annotations

from typing import Any, Dict, Tuple

from dc43_contracts_app.server import (
    IntegrationContractContext,
    _spark_stub_for_selection,
)


def _context_summary(contract_id: str, version: str) -> Dict[str, Any]:
    return {
        "id": contract_id,
        "version": version,
        "datasetId": f"{contract_id}_dataset",
        "server": {
            "path": f"/data/{contract_id}",
            "format": "parquet",
        },
        "expectations": {},
        "schemaEntries": [],
    }


def _context_map(contract_id: str, version: str) -> Dict[Tuple[str, str], IntegrationContractContext]:
    return {
        (contract_id, version): IntegrationContractContext(
            contract=object(),
            summary=_context_summary(contract_id, version),
        )
    }


def test_spark_stub_prefers_product_bindings_when_present() -> None:
    """Product-bound selections should emit binding-aware contexts without contracts."""

    contract_id = "orders"
    version = "1.0.0"
    binding = {
        "product_id": "orders_product",
        "product_version": "2.0.0",
        "port_name": "validated_orders",
    }

    stub = _spark_stub_for_selection(
        inputs=[{"contract_id": contract_id, "version": version, "data_product": binding}],
        outputs=[{"contract_id": contract_id, "version": version, "data_product": binding}],
        context_map=_context_map(contract_id, version),
        read_strategy={},
        write_strategy={},
    )

    assert "input_binding=DataProductInputBinding(" in stub
    assert "output_binding=DataProductOutputBinding(" in stub
    assert "contract=ContractReference" not in stub
    assert "context={" not in stub
    assert "dataset_format" not in stub
    assert "table=" not in stub and "path=" not in stub


def test_spark_stub_falls_back_to_contract_context_without_product() -> None:
    """Contract-only selections preserve contract-aware governance contexts."""

    contract_id = "customers"
    version = "3.0.0"

    stub = _spark_stub_for_selection(
        inputs=[{"contract_id": contract_id, "version": version}],
        outputs=[{"contract_id": contract_id, "version": version}],
        context_map=_context_map(contract_id, version),
        read_strategy={},
        write_strategy={},
    )

    assert "contract=ContractReference(" in stub
    assert "DataProductInputBinding(" not in stub
    assert "DataProductOutputBinding(" not in stub
    assert "context={" not in stub
    assert "format=" not in stub and "path=" not in stub
