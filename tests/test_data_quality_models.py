from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from dc43.services.data_quality.models import ValidationResult


class _DictLike:
    """Dictionary-style object returning key/value tuples when iterated."""

    def __iter__(self) -> Iterator[tuple[str, str]]:
        yield ("alpha", "beta")


@dataclass
class _HasItems:
    payload: dict[str, str]

    def items(self) -> Iterator[tuple[str, str]]:
        return iter(self.payload.items())


def test_validation_result_default_details_is_empty() -> None:
    result = ValidationResult()

    assert result.details["status"] == "unknown"
    assert result.details["errors"] == []
    assert result.details["warnings"] == []


def test_validation_result_accepts_iterable_details() -> None:
    result = ValidationResult(details=_DictLike())

    assert result.details["alpha"] == "beta"


def test_validation_result_accepts_items_provider() -> None:
    result = ValidationResult(details=_HasItems({"gamma": "delta"}))

    assert result.details["gamma"] == "delta"


def test_validation_result_ignores_non_iterable_details() -> None:
    class Foo:
        details = property(lambda self: None)  # pragma: no cover - descriptor only

    result = ValidationResult(details=Foo.__dict__["details"])

    assert "details" not in result.details
