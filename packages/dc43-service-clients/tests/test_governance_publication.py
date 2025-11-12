"""Unit tests for governance publication configuration helpers."""

from __future__ import annotations

import pytest

from dc43_service_clients.governance.publication import (
    GovernancePublicationMode,
    resolve_publication_mode,
)


def test_resolve_publication_mode_defaults_to_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DC43_GOVERNANCE_PUBLICATION_MODE", raising=False)
    mode = resolve_publication_mode()
    assert mode is GovernancePublicationMode.LEGACY


def test_resolve_publication_mode_prefers_config_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DC43_GOVERNANCE_PUBLICATION_MODE", raising=False)
    config = {"dc43.governance.publicationMode": "open_data_lineage"}
    mode = resolve_publication_mode(config=config)
    assert mode is GovernancePublicationMode.OPEN_DATA_LINEAGE


def test_resolve_publication_mode_uses_environment_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "open-telemetry")
    config = {"dc43.governance.publicationMode": "legacy"}
    mode = resolve_publication_mode(config=config)
    assert mode is GovernancePublicationMode.OPEN_TELEMETRY


def test_resolve_publication_mode_ignores_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "unsupported")
    config = {"dc43.governance.publicationMode": "unknown"}
    mode = resolve_publication_mode(config=config, default=GovernancePublicationMode.OPEN_DATA_LINEAGE)
    assert mode is GovernancePublicationMode.OPEN_DATA_LINEAGE

