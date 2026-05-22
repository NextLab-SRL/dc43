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


def test_to_model_coerces_numeric_fields():
    from dc43_core import to_model
    # Dictionary with version as integer and apiVersion as float (simulating unquoted YAML)
    raw = {
        "id": "example.orders",
        "version": 5,
        "kind": "dataset",
        "apiVersion": 3.1,
    }
    contract = to_model(raw)
    assert contract.id == "example.orders"
    assert contract.version == "5"
    assert contract.apiVersion == "3.1"

