from types import SimpleNamespace

from dc43_integrations.spark.io import ContractFirstDatasetLocator


def _dummy_contract(custom_properties, *, dataset_id="orders", path="/tmp/orders"):
    server = SimpleNamespace(path=path, customProperties=custom_properties)
    return SimpleNamespace(id=dataset_id, servers=[server])


def test_contract_locator_handles_custom_properties_descriptor():
    locator = ContractFirstDatasetLocator()
    contract = _dummy_contract(property(lambda self: None))

    resolution = locator.for_read(
        contract=contract,
        spark=SimpleNamespace(),
        format=None,
        path=None,
        table=None,
    )

    assert resolution.dataset_id == contract.id
    assert resolution.custom_properties is None


def test_contract_locator_extracts_versioning_options():
    locator = ContractFirstDatasetLocator()
    contract = _dummy_contract(
        [
            {
                "property": "dc43.versioning",
                "value": {
                    "readOptions": {"recursiveFileLookup": True},
                    "writeOptions": {"mergeSchema": False},
                },
            },
            {"property": "dc43.extra", "value": "value"},
        ]
    )

    resolution = locator.for_read(
        contract=contract,
        spark=SimpleNamespace(),
        format=None,
        path=None,
        table=None,
    )

    assert resolution.custom_properties == {
        "dc43.versioning": {
            "readOptions": {"recursiveFileLookup": True},
            "writeOptions": {"mergeSchema": False},
        },
        "dc43.extra": "value",
    }
    assert resolution.read_options == {"recursiveFileLookup": "True"}
    assert resolution.write_options == {"mergeSchema": "False"}
