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


def test_contract_locator_resolves_databricks_unity_table():
    from open_data_contract_standard.model import OpenDataContractStandard, Server, SchemaObject, SchemaProperty

    contract = OpenDataContractStandard(
        id="sales.orders",
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        servers=[
            Server(server="databricks_prod", type="databricks", catalog="governed", schema="analytics")
        ],
        schema=[
            SchemaObject(name="orders", physicalName="orders", physicalType="table", properties=[
                SchemaProperty(name="id", physicalType="bigint")
            ])
        ]
    )

    locator = ContractFirstDatasetLocator()
    res = locator.for_read(
        contract=contract,
        spark=SimpleNamespace(),
        format=None,
        path=None,
        table=None,
    )

    assert res.table == "governed.analytics.orders"
    assert res.path is None


def test_contract_locator_resolves_multi_schema_object():
    from open_data_contract_standard.model import OpenDataContractStandard, Server, SchemaObject, SchemaProperty

    contract = OpenDataContractStandard(
        id="sales.crm",
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        servers=[
            Server(server="unity", type="databricks", catalog="governed", schema="crm")
        ],
        schema=[
            SchemaObject(name="customers", physicalName="dim_customers", physicalType="table"),
            SchemaObject(name="transactions", physicalName="fct_transactions", physicalType="table"),
        ]
    )

    locator_default = ContractFirstDatasetLocator()
    res_default = locator_default.for_read(contract=contract, spark=SimpleNamespace(), format=None, path=None, table=None)
    assert res_default.table == "governed.crm.dim_customers"

    locator_tx = ContractFirstDatasetLocator(schema_object="transactions")
    res_tx = locator_tx.for_read(contract=contract, spark=SimpleNamespace(), format=None, path=None, table=None)
    assert res_tx.table == "governed.crm.fct_transactions"

