"""Compatibility shim for Collibra integration helpers."""

from dc43.components.integration.collibra import (
    CollibraContractGateway,
    ContractSummary,
    HttpCollibraContractGateway,
    StubCollibraContractGateway,
    _semver_key,
)

__all__ = [
    "CollibraContractGateway",
    "ContractSummary",
    "HttpCollibraContractGateway",
    "StubCollibraContractGateway",
    "_semver_key",
]
