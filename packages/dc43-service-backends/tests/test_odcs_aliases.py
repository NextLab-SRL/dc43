from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject, SchemaProperty
from dc43_service_backends.core.odcs import as_odcs_dict, to_model


def minimal_contract():
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="demo",
        name="demo",
        schema=[SchemaObject(name="demo", properties=[SchemaProperty(name="field", physicalType="string", required=True)])],
    )


def test_to_model_handles_schema_alias():
    model = minimal_contract()
    data = as_odcs_dict(model)
    assert "schema" in data and "schema_" not in data
    data_with_internal = dict(data)
    data_with_internal["schema_"] = data_with_internal.pop("schema")
    loaded = to_model(data_with_internal)
    assert loaded.schema_
