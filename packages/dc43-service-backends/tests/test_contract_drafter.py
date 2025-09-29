from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Dict, List

from open_data_contract_standard.model import (  # type: ignore
    CustomProperty,
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_backends.contracts.drafting import draft_from_validation_result
from dc43_service_clients.data_quality import ValidationResult


def _normalise_custom_properties(raw: Any) -> List[Any]:
    """Return ``customProperties`` entries as a list while handling descriptors."""

    if raw is None or isinstance(raw, (str, bytes, bytearray, property)):
        return []
    if isinstance(raw, Mapping):
        iterable = raw.values()
    elif isinstance(raw, Iterable):
        iterable = raw
    else:
        try:
            iterable = list(raw)
        except TypeError:
            return []
    return [item for item in iterable if item is not None]


def _build_contract() -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                    SchemaProperty(
                        name="customer_id",
                        physicalType="bigint",
                        required=True,
                    ),
                    SchemaProperty(
                        name="amount",
                        physicalType="double",
                        required=True,
                        quality=[DataQuality(mustBeGreaterThan=0)],
                    ),
                    SchemaProperty(
                        name="currency",
                        physicalType="string",
                        required=True,
                        quality=[DataQuality(rule="enum", mustBe=["EUR", "USD"])],
                    ),
                ],
            )
        ],
        customProperties=[CustomProperty(property="draft", value=False)],
    )


def _get_property(contract: OpenDataContractStandard, name: str) -> SchemaProperty:
    for obj in contract.schema_ or []:
        for prop in obj.properties or []:
            if prop.name == name:
                return prop
    raise AssertionError(f"Property {name} not found")


def _change_log(contract: OpenDataContractStandard) -> List[Dict[str, object]]:
    for prop in _normalise_custom_properties(contract.customProperties):
        key = getattr(prop, "property", None)
        if key == "draft_change_log":
            value = getattr(prop, "value", None) or []
            try:
                return list(value)
            except TypeError:
                return []
    return []


def _custom_props(contract: OpenDataContractStandard) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    for prop in _normalise_custom_properties(contract.customProperties):
        key = getattr(prop, "property", None)
        if key:
            props[str(key)] = getattr(prop, "value", None)
    return props


def test_draft_preserves_and_updates_quality_rules() -> None:
    base = _build_contract()
    validation = ValidationResult(
        ok=False,
        errors=["column customer_id contains 2 null value(s)"],
        warnings=[],
        metrics={
            "violations.not_null_customer_id": 2,
            "violations.gt_amount": 3,
            "violations.enum_currency": 0,
        },
        schema={
            "order_id": {"odcs_type": "bigint", "nullable": False},
            "customer_id": {"odcs_type": "bigint", "nullable": True},
            "amount": {"odcs_type": "double", "nullable": False},
            "currency": {"odcs_type": "string", "nullable": False},
        },
    )

    draft = draft_from_validation_result(validation=validation, base_contract=base)

    customer = _get_property(draft, "customer_id")
    amount = _get_property(draft, "amount")
    currency = _get_property(draft, "currency")

    assert customer.required is False
    assert amount.quality is None or len(amount.quality) == 0
    assert currency.quality and len(currency.quality) == 1
    assert currency.quality[0].rule == "enum"

    log = _change_log(draft)
    assert any(entry.get("status") == "relaxed" and entry.get("constraint") == "required" for entry in log)
    assert any(
        entry.get("status") == "removed" and "mustBeGreaterThan" in str(entry.get("rule"))
        for entry in log
    )
    assert any(entry.get("status") == "kept" and entry.get("rule") == "enum" for entry in log)
    assert any(entry.get("status") == "error" and entry.get("kind") == "validation" for entry in log)


def test_draft_extends_enum_with_new_values() -> None:
    base = _build_contract()
    validation = ValidationResult(
        ok=False,
        errors=["column currency contains unexpected value(s)"],
        warnings=[],
        metrics={
            "violations.enum_currency": 3,
            "observed.enum_currency": ["CAD", "EUR"],
        },
        schema={
            "order_id": {"odcs_type": "bigint", "nullable": False},
            "customer_id": {"odcs_type": "bigint", "nullable": False},
            "amount": {"odcs_type": "double", "nullable": False},
            "currency": {"odcs_type": "string", "nullable": False},
        },
    )

    draft = draft_from_validation_result(validation=validation, base_contract=base)

    currency = _get_property(draft, "currency")
    assert currency.quality and len(currency.quality) == 1
    enum_rule = currency.quality[0]
    assert enum_rule.mustBe == ["EUR", "USD", "CAD"]

    log = _change_log(draft)
    assert any(
        entry.get("status") == "updated"
        and entry.get("rule") == "enum"
        and ["CAD"] == list(entry.get("details", {}).get("added_values", []))
        for entry in log
    )


def test_draft_version_carries_unique_suffix_and_context() -> None:
    base = _build_contract()
    validation = ValidationResult(
        ok=False,
        errors=["boom"],
        warnings=[],
        metrics={},
        schema={},
    )

    dataset_version = "2024-06-01T12:00:00Z"
    draft = draft_from_validation_result(
        validation=validation,
        base_contract=base,
        dataset_id="test.orders",
        dataset_version=dataset_version,
        draft_context={"pipeline": "orders.pipeline"},
    )

    assert draft.version.startswith("1.1.0-draft-")
    assert "2024-06-01T12-00-00Z" in draft.version

    props = _custom_props(draft)
    context_props = props.get("draft_context") or {}
    assert context_props.get("pipeline") == "orders.pipeline"
    assert context_props.get("dataset_id") == "test.orders"
    assert context_props.get("dataset_version") == dataset_version
    assert props.get("draft_pipeline") == "orders.pipeline"
    assert props.get("provenance", {}).get("dataset_version") == dataset_version

    retry = draft_from_validation_result(
        validation=validation,
        base_contract=base,
        dataset_id="test.orders",
        dataset_version=dataset_version,
        draft_context={"pipeline": "orders.pipeline", "job": "retry"},
    )

    assert retry.version != draft.version
    retry_props = _custom_props(retry)
    retry_context = retry_props.get("draft_context") or {}
    assert retry_context.get("pipeline") == "orders.pipeline"
    assert retry_context.get("job") == "retry"
