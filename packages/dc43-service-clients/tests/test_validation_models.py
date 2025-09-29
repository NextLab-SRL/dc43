from dc43_service_clients.data_quality import ObservationPayload, ValidationResult, coerce_details


def test_validation_result_flags_errors_and_status():
    result = ValidationResult(
        ok=True,
        errors=["missing contract"],
        metrics={"rows": 10},
        status="block",
        reason="validation failed",
    )

    assert not result.ok
    assert result.status == "block"
    assert result.reason == "validation failed"
    assert result.details["errors"] == ["missing contract"]


def test_merge_details_preserves_existing_information():
    result = ValidationResult(ok=True, warnings=["initial"], metrics={"rows": 5})
    result.merge_details({"extra": True})

    assert result.details["extra"] is True
    assert result.details["metrics"] == {"rows": 5}


def test_coerce_details_handles_iterables():
    details = coerce_details([("reason", "ok"), ("status", "warn")])

    assert details == {"reason": "ok", "status": "warn"}


def test_observation_payload_defaults():
    payload = ObservationPayload(metrics={"rows": 1})

    assert payload.schema is None
    assert payload.reused is False
