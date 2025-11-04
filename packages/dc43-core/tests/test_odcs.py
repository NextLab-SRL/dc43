from dc43_core import build_odcs, ensure_version, ODCS_REQUIRED


def test_build_and_ensure_version():
    contract = build_odcs(
        contract_id="example.orders",
        version="1.0.0",
        kind="dataset",
        api_version=ODCS_REQUIRED,
    )
    ensure_version(contract)
    assert contract.id == "example.orders"
    assert contract.version == "1.0.0"
