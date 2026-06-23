from types import SimpleNamespace

from dc43_integrations.spark.io import ContractFirstDatasetLocator

def _dummy_contract(
    custom_properties,
    *,
    dataset_id="orders",
    path="/tmp/orders",
    fmt=None,
):
    server = SimpleNamespace(path=path, customProperties=custom_properties)
    if fmt is not None:
        server.format = fmt
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
                "property": "dc43.core.versioning",
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
        "dc43.core.versioning": {
            "readOptions": {"recursiveFileLookup": True},
            "writeOptions": {"mergeSchema": False},
        },
        "dc43.extra": "value",
    }
    assert resolution.read_options == {"recursiveFileLookup": "True"}
    assert resolution.write_options == {"mergeSchema": "False"}


def test_contract_locator_promotes_delta_path_table_reference():
    locator = ContractFirstDatasetLocator()
    table_name = "analytics.sales.orders"
    contract = _dummy_contract([], path=table_name, fmt="delta")

    resolution = locator.for_write(
        contract=contract,
        df=SimpleNamespace(),
        format=None,
        path=None,
        table=None,
    )

    assert resolution.table == table_name
    assert resolution.path is None


def test_contract_locator_promotes_delta_path_table_reference_on_read():
    locator = ContractFirstDatasetLocator()
    table_name = "analytics.sales.orders"
    contract = _dummy_contract([], path=table_name, fmt="delta")

    resolution = locator.for_read(
        contract=contract,
        spark=SimpleNamespace(),
        format=None,
        path=None,
        table=None,
    )

    assert resolution.table == table_name
    assert resolution.path is None


def test_contract_locator_promotes_table_like_path_when_catalog_confirms():
    locator = ContractFirstDatasetLocator()
    table_name = "analytics.sales.orders"

    class _Catalog:
        def tableExists(self, name: str) -> bool:  # pragma: no cover - simple predicate
            return name == table_name

    spark = SimpleNamespace(catalog=_Catalog())

    resolution = locator.for_read(
        contract=None,
        spark=spark,
        format=None,
        path=table_name,
        table=None,
    )

    assert resolution.table == table_name
    assert resolution.path is None
