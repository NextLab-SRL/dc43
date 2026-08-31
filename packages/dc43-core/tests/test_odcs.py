from dc43_core import build_odcs, ensure_version, ODCS_REQUIRED


def test_build_and_ensure_version():
    import pytest
    contract = build_odcs(
        contract_id="example.orders",
        version="1.0.0",
        kind="dataset",
        api_version=ODCS_REQUIRED,
    )
    ensure_version(contract)
    assert contract.id == "example.orders"
    assert contract.version == "1.0.0"

    # Test with 'v' prefixed version
    contract_v = build_odcs(
        contract_id="example.orders",
        version="1.0.0",
        kind="dataset",
        api_version=f"v{ODCS_REQUIRED}",
    )
    ensure_version(contract_v)

    # Test with v3.0.2 explicitly
    contract_302 = build_odcs(
        contract_id="example.orders",
        version="1.0.0",
        kind="dataset",
        api_version="v3.0.2",
    )
    ensure_version(contract_302)

    # Test invalid version
    contract_invalid = build_odcs(
        contract_id="example.orders",
        version="1.0.0",
        kind="dataset",
        api_version="v4.0.0",
    )
    with pytest.raises(ValueError, match="ODCS apiVersion mismatch"):
        ensure_version(contract_invalid)


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


def test_schema_object_and_table_resolution():
    from open_data_contract_standard.model import Server, SchemaObject, SchemaProperty
    from dc43_core import (
        list_schema_objects,
        find_schema_object,
        resolve_table_name,
        resolve_storage_path,
    )

    doc = build_odcs(
        contract_id="sales.orders",
        version="1.0.0",
        kind="DataContract",
        api_version=ODCS_REQUIRED,
        physical_name="orders",
        physical_type="table",
        properties=[SchemaProperty(name="id", physicalType="bigint")],
        servers=[
            Server(server="databricks", type="databricks", catalog="governed", schema="analytics"),
            Server(server="snowflake", type="snowflake", database="prod", schema="sales"),
            Server(server="bigquery", type="bigquery", project="my-project", dataset="dw"),
            Server(server="s3", type="s3", location="s3://my-bucket/orders"),
        ],
    )

    objs = list_schema_objects(doc)
    assert len(objs) == 1
    assert objs[0].physicalName == "orders"
    assert objs[0].physicalType == "table"

    obj = find_schema_object(doc, "orders")
    assert obj is not None
    assert obj.physicalName == "orders"

    # Databricks
    assert resolve_table_name(doc.servers[0], obj) == "governed.analytics.orders"
    # Snowflake
    assert resolve_table_name(doc.servers[1], obj) == "prod.sales.orders"
    # BigQuery
    assert resolve_table_name(doc.servers[2], obj) == "my-project.dw.orders"
    # S3
    assert resolve_storage_path(doc.servers[3], obj) == "s3://my-bucket/orders"


