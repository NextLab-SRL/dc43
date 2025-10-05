from __future__ import annotations

from types import SimpleNamespace
from typing import Mapping
import sys

import pytest

from dc43_service_backends.config import ServiceBackendsConfig, UnityCatalogConfig
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.governance.unity_catalog import (
    UnityCatalogLinker,
    build_linker_from_config,
    prefix_table_resolver,
)
from dc43_service_backends.governance.bootstrap import (
    DEFAULT_LINK_HOOK_BUILDER_SPECS,
    build_dataset_contract_link_hooks,
    load_link_hook_builder,
)


def test_linker_updates_table() -> None:
    updates: list[tuple[str, Mapping[str, str]]] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append((table_name, dict(properties)))

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        static_properties={"dc43.catalog_synced": "true"},
    )

    linker.link_dataset_contract(
        dataset_id="table:governed.analytics.orders",
        dataset_version="42",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    assert updates == [
        (
            "governed.analytics.orders",
            {
                "dc43.contract_id": "sales.orders",
                "dc43.contract_version": "0.1.0",
                "dc43.dataset_version": "42",
                "dc43.catalog_synced": "true",
            },
        )
    ]


def test_linker_skips_non_matching_datasets() -> None:
    updates: list[tuple[str, Mapping[str, str]]] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append((table_name, dict(properties)))

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        table_resolver=prefix_table_resolver("table:"),
    )

    linker.link_dataset_contract(
        dataset_id="path:dbfs:/tmp/orders",
        dataset_version="7",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    assert updates == []


class _ContractClient:
    def __init__(self) -> None:
        self.links: list[tuple[str, str, str, str]] = []

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self.links.append((dataset_id, dataset_version, contract_id, contract_version))


class _DQClient:
    pass


def test_local_backend_still_tags_with_remote_contract_client() -> None:
    updates: list[tuple[str, Mapping[str, str]]] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append((table_name, dict(properties)))

    contract_client = _ContractClient()
    backend = LocalGovernanceServiceBackend(
        contract_client=contract_client,
        dq_client=_DQClient(),
        link_hooks=[UnityCatalogLinker(apply_table_properties=_update).link_dataset_contract],
    )

    backend.link_dataset_contract(
        dataset_id="table:governed.analytics.orders",
        dataset_version="1",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    assert contract_client.links == [
        ("table:governed.analytics.orders", "1", "sales.orders", "0.1.0")
    ]
    assert updates == [
        (
            "governed.analytics.orders",
            {
                "dc43.contract_id": "sales.orders",
                "dc43.contract_version": "0.1.0",
                "dc43.dataset_version": "1",
            },
        )
    ]


class _Workspace:
    def __init__(self) -> None:
        self.tables = _Tables()


class _Tables:
    def __init__(self) -> None:
        self.updates: list[tuple[str, Mapping[str, str]]] = []

    def update(self, *, name: str, properties: Mapping[str, str]) -> None:
        self.updates.append((name, dict(properties)))


def test_build_linker_from_config_uses_workspace_builder() -> None:
    workspace = _Workspace()

    def _builder(config: UnityCatalogConfig) -> _Workspace:
        assert config.enabled is True
        assert config.dataset_prefix == "table:"
        return workspace

    config = UnityCatalogConfig(
        enabled=True,
        dataset_prefix="table:",
        static_properties={"team": "governance"},
    )

    linker = build_linker_from_config(config, workspace_builder=_builder)
    assert isinstance(linker, UnityCatalogLinker)

    linker.link_dataset_contract(
        dataset_id="table:governed.analytics.orders",
        dataset_version="1",
        contract_id="sales.orders",
        contract_version="0.2.0",
    )

    assert workspace.tables.updates == [
        (
            "governed.analytics.orders",
            {
                "dc43.contract_id": "sales.orders",
                "dc43.contract_version": "0.2.0",
                "dc43.dataset_version": "1",
                "team": "governance",
            },
        )
    ]


def test_build_linker_from_config_disabled() -> None:
    config = UnityCatalogConfig(enabled=False)
    linker = build_linker_from_config(config, workspace_builder=lambda cfg: _Workspace())
    assert linker is None


def test_build_dataset_contract_link_hooks_uses_unity(monkeypatch) -> None:
    config = ServiceBackendsConfig()
    config.unity_catalog.enabled = True

    linker = UnityCatalogLinker(apply_table_properties=lambda name, props: None)

    monkeypatch.setattr(
        "dc43_service_backends.governance.unity_catalog.build_linker_from_config",
        lambda cfg: linker,
    )

    hooks = build_dataset_contract_link_hooks(config)
    assert hooks == (linker.link_dataset_contract,)


def test_build_dataset_contract_link_hooks_warns_on_failure() -> None:
    config = ServiceBackendsConfig()

    def _broken_builder(*_: object) -> None:
        raise RuntimeError("boom")

    with pytest.warns(RuntimeWarning):
        hooks = build_dataset_contract_link_hooks(
            config,
            extra_builders=[_broken_builder],
            include_defaults=False,
        )

    assert hooks == ()


def test_build_dataset_contract_link_hooks_uses_configured_specs(monkeypatch) -> None:
    config = ServiceBackendsConfig()
    config.governance.dataset_contract_link_builders = (
        "custom.module:builder",
        "custom.module:builder",
    )

    loaded: list[str] = []

    def _loader(spec: str):
        loaded.append(spec)
        return lambda cfg: None

    monkeypatch.setattr(
        "dc43_service_backends.governance.bootstrap.load_link_hook_builder",
        _loader,
    )

    hooks = build_dataset_contract_link_hooks(config, include_defaults=False)
    assert hooks == ()
    assert loaded == ["custom.module:builder"]


def test_build_dataset_contract_link_hooks_warns_on_load_failure(monkeypatch) -> None:
    config = ServiceBackendsConfig()
    config.governance.dataset_contract_link_builders = ("broken.module:builder",)

    def _loader(spec: str):
        raise RuntimeError("load boom")

    monkeypatch.setattr(
        "dc43_service_backends.governance.bootstrap.load_link_hook_builder",
        _loader,
    )

    with pytest.warns(RuntimeWarning):
        hooks = build_dataset_contract_link_hooks(config, include_defaults=False)

    assert hooks == ()


def test_load_link_hook_builder_colon_spec() -> None:
    module = SimpleNamespace(builder=lambda config: ())
    sys.modules["sample.builders"] = module
    try:
        builder = load_link_hook_builder("sample.builders:builder")
        assert builder is module.builder
    finally:
        sys.modules.pop("sample.builders", None)


def test_load_link_hook_builder_dotted_spec() -> None:
    module = SimpleNamespace(nested=SimpleNamespace(factory=lambda config: ()))
    sys.modules["sample.builders2"] = module
    try:
        builder = load_link_hook_builder("sample.builders2.nested.factory")
        assert builder is module.nested.factory
    finally:
        sys.modules.pop("sample.builders2", None)


def test_default_builder_specs_include_unity() -> None:
    assert (
        "dc43_service_backends.governance.unity_catalog:build_link_hooks"
        in DEFAULT_LINK_HOOK_BUILDER_SPECS
    )
