from __future__ import annotations

from types import SimpleNamespace
from typing import Mapping, Sequence
import sys

import pytest

from dc43_service_backends.config import ServiceBackendsConfig, UnityCatalogConfig
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.governance.unity_catalog import (
    UnityCatalogLinker,
    build_linker_from_config,
    prefix_table_resolver,
    sql_table_property_updater,
    sql_table_tag_updater,
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


def test_linker_updates_tags_and_unsets() -> None:
    calls: list[tuple[str, Mapping[str, str], tuple[str, ...]]] = []

    def _tags(table_name: str, tags: Mapping[str, str], unset_tags: Sequence[str] = ()) -> None:
        calls.append((table_name, dict(tags), tuple(unset_tags)))

    linker = UnityCatalogLinker(
        apply_table_tags=_tags,
        static_tags={"dc43.catalog_synced": "true"},
    )

    linker.link_dataset_contract(
        dataset_id="table:governed.analytics.orders",
        dataset_version="42",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    def _metadata(*_: str) -> Mapping[str, object]:
        return {"dc43.contract_id": None}

    linker.metadata_provider = _metadata
    linker.static_tags = {}

    linker.link_dataset_contract(
        dataset_id="table:governed.analytics.orders",
        dataset_version="",
        contract_id="",
        contract_version="",
    )

    assert calls == [
        (
            "governed.analytics.orders",
            {
                "dc43_contract_id": "sales.orders",
                "dc43_contract_version": "0.1.0",
                "dc43_dataset_version": "42",
                "dc43_catalog_synced": "true",
            },
            (),
        ),
        ("governed.analytics.orders", {}, ("dc43_contract_id",)),
    ]


def test_linker_ignores_reserved_property_keys() -> None:
    updates: list[tuple[str, Mapping[str, str]]] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append((table_name, dict(properties)))

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        static_properties={"owner": "team"},
    )

    with pytest.warns(RuntimeWarning):
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
            },
        )
    ]


def test_linker_converts_invalid_tag_names() -> None:
    calls: list[tuple[str, Mapping[str, str], tuple[str, ...]]] = []

    def _tags(table_name: str, tags: Mapping[str, str], unset_tags: Sequence[str] = ()) -> None:
        calls.append((table_name, dict(tags), tuple(unset_tags)))

    def _metadata(*_: str) -> Mapping[str, object]:
        return {
            "dc43.contract:id": "sales.orders",
            "dc43.contract_version": "0.1.0",
            "dc43.dataset_version": "42",
        }

    linker = UnityCatalogLinker(apply_table_tags=_tags)
    linker.metadata_provider = _metadata

    with pytest.warns(RuntimeWarning):
        linker.link_dataset_contract(
            dataset_id="table:governed.analytics.orders",
            dataset_version="42",
            contract_id="sales.orders",
            contract_version="0.1.0",
        )

    assert calls == [
        (
            "governed.analytics.orders",
            {"dc43_contract_id": "sales.orders", "dc43_contract_version": "0.1.0", "dc43_dataset_version": "42"},
            (),
        )
    ]


def test_linker_handles_property_errors() -> None:
    failures: list[str] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        failures.append(table_name)
        raise RuntimeError("permission denied")

    linker = UnityCatalogLinker(apply_table_properties=_update)

    with pytest.warns(RuntimeWarning):
        linker.link_dataset_contract(
            dataset_id="table:governed.analytics.orders",
            dataset_version="42",
            contract_id="sales.orders",
            contract_version="0.1.0",
        )

    assert failures == ["governed.analytics.orders"]


class _SQLConnection:
    def __init__(self, engine: "_SQLEngine") -> None:
        self._engine = engine

    def execute(self, statement) -> None:  # pragma: no cover - exercised via updater
        self._engine.statements.append(str(statement))


class _SQLEngine:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def begin(self):  # pragma: no cover - exercised via updater
        engine = self

        class _Transaction:
            def __enter__(self):
                return _SQLConnection(engine)

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

        return _Transaction()


def test_sql_table_property_updater_executes_statements() -> None:
    engine = _SQLEngine()
    updater = sql_table_property_updater(engine)

    updater("dev.catalog.orders", {"dc43.contract_id": "sales.orders", "env": "dev"})

    assert engine.statements == [
        "ALTER TABLE `dev`.`catalog`.`orders` SET TBLPROPERTIES ('dc43.contract_id'='sales.orders', 'env'='dev')"
    ]


def test_sql_table_tag_updater_executes_statements() -> None:
    engine = _SQLEngine()
    updater = sql_table_tag_updater(engine)

    updater("dev.catalog.orders", {"owner": "team"}, ("outdated",))

    assert engine.statements == [
        "ALTER TABLE `dev`.`catalog`.`orders` UNSET TAGS ('outdated')",
        "ALTER TABLE `dev`.`catalog`.`orders` SET TAGS ('owner'='team')",
    ]


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


def test_build_linker_from_config_prefers_sql_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _SQLEngine()
    dsns: list[str] = []

    def _engine_builder(dsn: str) -> _SQLEngine:
        dsns.append(dsn)
        return engine

    config = UnityCatalogConfig(
        enabled=True,
        sql_dsn="databricks://token:abc@adb.example.com?http_path=/sql/warehouses/demo",
        dataset_prefix="table:",
    )

    linker = build_linker_from_config(
        config,
        engine_builder=_engine_builder,
    )

    assert isinstance(linker, UnityCatalogLinker)
    assert dsns == ["databricks://token:abc@adb.example.com?http_path=/sql/warehouses/demo"]

    linker.link_dataset_contract(
        dataset_id="table:dev.catalog.orders",
        dataset_version="1",
        contract_id="sales.orders",
        contract_version="0.3.0",
    )

    assert engine.statements == [
        "ALTER TABLE `dev`.`catalog`.`orders` SET TBLPROPERTIES ('dc43.contract_id'='sales.orders', 'dc43.contract_version'='0.3.0', 'dc43.dataset_version'='1')"
    ]


def test_build_linker_from_config_with_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _SQLEngine()

    def _engine_builder(dsn: str) -> _SQLEngine:
        return engine

    config = UnityCatalogConfig(
        enabled=True,
        sql_dsn="databricks://token@example",  # property path
        tags_enabled=True,
        static_tags={"env": "dev"},
    )

    linker = build_linker_from_config(
        config,
        engine_builder=_engine_builder,
    )

    assert isinstance(linker, UnityCatalogLinker)

    linker.link_dataset_contract(
        dataset_id="table:dev.catalog.orders",
        dataset_version="2",
        contract_id="sales.orders",
        contract_version="0.4.0",
    )

    assert engine.statements == [
        "ALTER TABLE `dev`.`catalog`.`orders` SET TBLPROPERTIES ('dc43.contract_id'='sales.orders', 'dc43.contract_version'='0.4.0', 'dc43.dataset_version'='2')",
        "ALTER TABLE `dev`.`catalog`.`orders` SET TAGS ('dc43_contract_id'='sales.orders', 'dc43_contract_version'='0.4.0', 'dc43_dataset_version'='2', 'env'='dev')",
    ]


def test_build_linker_from_config_warns_when_tag_dsn_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    config = UnityCatalogConfig(enabled=True, tags_enabled=True)

    with pytest.warns(RuntimeWarning) as recorded:
        linker = build_linker_from_config(config)

    assert linker is None
    assert len(recorded) >= 2


def test_build_linker_from_config_warns_without_sql_dsn() -> None:
    config = UnityCatalogConfig(enabled=True)

    with pytest.warns(RuntimeWarning):
        linker = build_linker_from_config(config)

    assert linker is None


def test_build_linker_from_config_supports_tag_only_flow() -> None:
    tag_engine = _SQLEngine()

    def _engine_builder(dsn: str) -> _SQLEngine:
        assert dsn == "databricks://token:abc@adb.example.com?http_path=/sql/warehouses/tags"
        return tag_engine

    config = UnityCatalogConfig(
        enabled=True,
        tags_enabled=True,
        tags_sql_dsn="databricks://token:abc@adb.example.com?http_path=/sql/warehouses/tags",
    )

    with pytest.warns(RuntimeWarning):
        linker = build_linker_from_config(config, engine_builder=_engine_builder)

    assert isinstance(linker, UnityCatalogLinker)

    linker.link_dataset_contract(
        dataset_id="table:dev.catalog.orders",
        dataset_version="2",
        contract_id="sales.orders",
        contract_version="0.4.0",
    )

    assert tag_engine.statements == [
        "ALTER TABLE `dev`.`catalog`.`orders` SET TAGS ('dc43_contract_id'='sales.orders', 'dc43_contract_version'='0.4.0', 'dc43_dataset_version'='2')",
    ]


def test_build_linker_from_config_disabled() -> None:
    config = UnityCatalogConfig(enabled=False)
    linker = build_linker_from_config(config)
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
