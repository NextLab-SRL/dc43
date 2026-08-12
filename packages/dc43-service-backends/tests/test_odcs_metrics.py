from open_data_contract_standard.model import (
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_backends.data_quality.backend.engine import (
    evaluate_contract,
    expectation_specs,
)
from dc43_service_backends.data_quality.backend.predicates import expectation_plan, sql_predicate


def test_odcs_metric_duplicate_values_column() -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.duplicates.col",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="order_id",
                        physicalType="string",
                        quality=[DataQuality(metric="duplicateValues", mustBe=0)],
                    )
                ],
            )
        ],
    )
    specs = expectation_specs(contract)
    assert len(specs) == 1
    assert specs[0].rule == "unique"
    assert specs[0].column == "order_id"

    result = evaluate_contract(
        contract,
        schema={"order_id": {"odcs_type": "string"}},
        metrics={"violations.unique_order_id": 3},
    )
    assert not result.ok
    assert any("order_id has 3 duplicate value(s)" in err for err in result.errors)


def test_odcs_metric_duplicate_values_schema_composite() -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.duplicates.schema",
        schema=[
            SchemaObject(
                name="orders",
                quality=[
                    DataQuality(
                        id="orders_unique_tenant_order",
                        metric="duplicateValues",
                        mustBe=0,
                        arguments={"properties": ["tenant_id", "order_id"]},
                    )
                ],
                properties=[
                    SchemaProperty(name="tenant_id", physicalType="string"),
                    SchemaProperty(name="order_id", physicalType="string"),
                ],
            )
        ],
    )
    specs = expectation_specs(contract)
    assert len(specs) == 1
    assert specs[0].rule == "unique_composite"
    assert specs[0].params["properties"] == ["tenant_id", "order_id"]

    result = evaluate_contract(
        contract,
        schema={"tenant_id": {"odcs_type": "string"}, "order_id": {"odcs_type": "string"}},
        metrics={"violations.orders_unique_tenant_order": 2},
    )
    assert not result.ok
    assert any("duplicate row(s) across properties [tenant_id, order_id]" in err for err in result.errors)


def test_odcs_metric_null_values() -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.nulls",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="tenant_id",
                        physicalType="string",
                        quality=[DataQuality(metric="nullValues", mustBe=0)],
                    )
                ],
            )
        ],
    )
    specs = expectation_specs(contract)
    assert len(specs) == 1
    assert specs[0].rule == "not_null"
    assert specs[0].column == "tenant_id"


def test_odcs_metric_missing_values() -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.missing",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="status",
                        physicalType="string",
                        quality=[
                            DataQuality(
                                metric="missingValues",
                                mustBe=0,
                                arguments={"missingValues": [None, "", "N/A"]},
                            )
                        ],
                    )
                ],
            )
        ],
    )
    specs = expectation_specs(contract)
    assert len(specs) == 1
    assert specs[0].rule == "missing_values"
    assert specs[0].params["values"] == [None, "", "N/A"]

    pred = sql_predicate(specs[0])
    assert pred == "NOT (`status` IS NULL OR `status` = '' OR `status` = 'N/A')"

    result = evaluate_contract(
        contract,
        schema={"status": {"odcs_type": "string"}},
        metrics={"violations.missing_values_status": 5},
    )
    assert not result.ok
    assert any("status contains 5 missing value(s)" in err for err in result.errors)


def test_odcs_metric_invalid_values() -> None:
    contract_enum = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.invalid.enum",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="unit",
                        physicalType="string",
                        quality=[
                            DataQuality(
                                metric="invalidValues",
                                mustBe=0,
                                arguments={"validValues": ["pounds", "kg"]},
                            )
                        ],
                    )
                ],
            )
        ],
    )
    specs_enum = expectation_specs(contract_enum)
    assert len(specs_enum) == 1
    assert specs_enum[0].rule == "enum"
    assert specs_enum[0].params["values"] == ["pounds", "kg"]

    contract_regex = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.invalid.regex",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="code",
                        physicalType="string",
                        quality=[
                            DataQuality(
                                metric="invalidValues",
                                mustBe=0,
                                arguments={"pattern": "^[A-Z]{2}[0-9]{2}$"},
                            )
                        ],
                    )
                ],
            )
        ],
    )
    specs_regex = expectation_specs(contract_regex)
    assert len(specs_regex) == 1
    assert specs_regex[0].rule == "regex"
    assert specs_regex[0].params["pattern"] == "^[A-Z]{2}[0-9]{2}$"


def test_odcs_metric_row_count() -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.1.0",
        id="test.rowcount",
        schema=[
            SchemaObject(
                name="orders",
                quality=[
                    DataQuality(
                        id="orders_min_rows",
                        metric="rowCount",
                        mustBeGreaterThan=10,
                    )
                ],
                properties=[SchemaProperty(name="order_id", physicalType="string")],
            )
        ],
    )
    specs = expectation_specs(contract)
    assert len(specs) == 1
    assert specs[0].rule == "row_count"
    assert specs[0].params["gt"] == 10

    # Row count 5 violates mustBeGreaterThan=10
    result = evaluate_contract(
        contract,
        schema={"order_id": {"odcs_type": "string"}},
        metrics={"row_count": 5},
    )
    assert not result.ok
    assert any("row count 5 failed rowCount constraint" in err for err in result.errors)

    # Row count 15 satisfies mustBeGreaterThan=10
    result_ok = evaluate_contract(
        contract,
        schema={"order_id": {"odcs_type": "string"}},
        metrics={"row_count": 15},
    )
    assert result_ok.ok
