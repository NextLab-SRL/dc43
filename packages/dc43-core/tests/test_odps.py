from dc43_core import (
    OpenDataProduct,
    DataProductInputPort,
    DataProductOutputPort,
    evolve_odps_to_draft,
)


def test_odps_round_trip():
    product = OpenDataProduct.from_dict(
        {
            "id": "dp.analytics",
            "status": "draft",
            "inputPorts": [
                {
                    "name": "orders",
                    "version": "1.0.0",
                    "contractId": "sales.orders",
                }
            ],
            "outputPorts": [
                {
                    "name": "primary",
                    "version": "1.0.0",
                    "contractId": "dp.analytics.primary",
                }
            ],
        }
    )
    evolve_odps_to_draft(product, existing_versions=["1.0.0"])
    assert any(isinstance(port, DataProductInputPort) for port in product.input_ports)
    assert any(isinstance(port, DataProductOutputPort) for port in product.output_ports)
    assert product.status.lower() == "draft"
