from __future__ import annotations

from types import SimpleNamespace
from typing import Mapping, Sequence
import sys

import pytest

from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject, Server  # type: ignore

from dc43_service_backends.config import (
    ContractStoreConfig,
    DataProductStoreConfig,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
    UnityCatalogConfig,
)
from dc43_service_backends.governance.backend.local import LocalGovernanceServiceBackend
from dc43_service_backends.governance.unity_catalog import (
    ContractTagHandler,
    UnityCatalogLinker,
    colon_tag_handler,
    contract_servers_table_resolver,
    build_link_hooks,
    build_linker_from_config,
    find_table_schema_object,
    parse_colon_tags,
    prefix_table_resolver,
    sql_table_property_updater,
    sql_table_tag_updater,
)
from dc43_service_backends.governance.bootstrap import (
    DEFAULT_LINK_HOOK_BUILDER_SPECS,
    LinkHookContext,
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


def test_linker_uses_contract_servers_before_dataset_id() -> None:
    updates: list[str] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append(table_name)

    contract = OpenDataContractStandard(
        id="sales.orders",
        version="0.1.0",
        kind="DatasetContract",
        apiVersion="3.0.2",
        servers=[
            Server(server="unity", type="catalog", catalog="dev", schema="bronze", dataset="orders"),
            Server(server="unity", type="catalog", catalog="dev", schema="bronze", dataset="orders"),
        ],
    )

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        contract_loader=lambda *_: contract,
        contract_table_resolver=contract_servers_table_resolver,
        table_resolver=prefix_table_resolver("table__"),
    )

    linker.link_dataset_contract(
        dataset_id="ignored.dataset",  # does not match the prefix
        dataset_version="7",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    assert updates == ["dev.bronze.orders"]


def test_linker_uses_contract_server_and_schema_objects() -> None:
    from open_data_contract_standard.model import SchemaObject
    updates: list[str] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append(table_name)

    contract = OpenDataContractStandard(
        id="sales.orders",
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        servers=[
            Server(server="unity", type="databricks", catalog="governed", schema="analytics"),
        ],
        schema=[
            SchemaObject(name="orders", physicalName="orders", physicalType="table"),
            SchemaObject(name="order_items", physicalName="order_items", physicalType="table"),
        ]
    )

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        contract_loader=lambda *_: contract,
        contract_table_resolver=contract_servers_table_resolver,
        table_resolver=prefix_table_resolver("table__"),
    )

    linker.link_dataset_contract(
        dataset_id="ignored.dataset",
        dataset_version="1",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )

    assert updates == ["governed.analytics.orders", "governed.analytics.order_items"]



def test_linker_falls_back_to_dataset_identifier_when_contract_missing() -> None:
    updates: list[str] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append(table_name)

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        contract_loader=lambda *_: None,
        table_resolver=prefix_table_resolver("table:"),
    )

    linker.link_dataset_contract(
        dataset_id="table:dev.analytics.orders",
        dataset_version="9",
        contract_id="sales.orders",
        contract_version="0.2.0",
    )

    assert updates == ["dev.analytics.orders"]


def test_contract_servers_table_resolver_handles_partial_servers() -> None:
    contract = OpenDataContractStandard(
        id="sales.orders",
        version="0.1.0",
        kind="DatasetContract",
        apiVersion="3.0.2",
        servers=[
            Server(server="unity", type="catalog", catalog="dev", schema="bronze", dataset="orders"),
            Server(server="unity", type="catalog", schema="bronze", dataset="orders_rejects"),
            Server(server="raw", type="filesystem", path="dbfs:/raw/orders"),
        ],
    )

    tables = contract_servers_table_resolver(contract)

    assert tables == ("dev.bronze.orders", "bronze.orders_rejects")


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


def test_linker_skips_reserved_tables() -> None:
    updates: list[tuple[str, Mapping[str, str]]] = []

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        updates.append((table_name, dict(properties)))

    linker = UnityCatalogLinker(
        apply_table_properties=_update,
        skip_tables=frozenset({"dev.catalog.contracts"}),
    )

    with pytest.warns(RuntimeWarning):
        linker.link_dataset_contract(
            dataset_id="table:dev.catalog.contracts",
            dataset_version="1",
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


_DC43_TAGS = {"dc43_contract_id": "sales.orders", "dc43_contract_version": "0.1.0", "dc43_dataset_version": "1"}


def _contract_with(*schema_objects: SchemaObject, servers: bool = True) -> OpenDataContractStandard:
    return OpenDataContractStandard(
        id="sales.orders",
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        servers=[Server(server="unity", type="databricks", catalog="governed", schema="analytics")] if servers else None,
        schema=list(schema_objects),
    )


def _link_tags(
    contract: OpenDataContractStandard,
    *,
    dataset_id: str = "ignored.dataset",
    static_tags: Mapping[str, str] | None = None,
    contract_tag_handler: ContractTagHandler | None = colon_tag_handler,
) -> list[tuple[str, dict[str, str]]]:
    calls: list[tuple[str, dict[str, str]]] = []

    def _tags(table_name: str, tags: Mapping[str, str], unset_tags: Sequence[str] = ()) -> None:
        calls.append((table_name, dict(tags)))

    linker = UnityCatalogLinker(
        apply_table_tags=_tags,
        contract_loader=lambda *_: contract,
        contract_table_resolver=contract_servers_table_resolver,
        contract_tag_handler=contract_tag_handler,
        static_tags=static_tags or {},
    )
    linker.link_dataset_contract(
        dataset_id=dataset_id,
        dataset_version="1",
        contract_id="sales.orders",
        contract_version="0.1.0",
    )
    return calls


def test_linker_without_contract_tag_handler_ignores_contract_tags() -> None:
    contract = _contract_with(
        SchemaObject(name="orders", physicalName="orders", tags=["processingLayer:Silver"]),
    )
    contract.tags = ["domain:sales"]

    calls = _link_tags(contract, static_tags={"team": "sales"}, contract_tag_handler=None)

    assert calls == [
        ("governed.analytics.orders", {**_DC43_TAGS, "team": "sales"}),
    ]


def test_linker_applies_contract_tags_per_table() -> None:
    contract = _contract_with(
        SchemaObject(name="orders_v1", physicalName="orders", tags=["processingLayer:Silver", "environnement:prd01"]),
        SchemaObject(name="order_items", physicalName="order_items"),
    )

    calls = _link_tags(contract, static_tags={"environnement": "dev01", "team": "sales"})

    assert calls == [
        (
            "governed.analytics.orders",
            {**_DC43_TAGS, "environnement": "prd01", "team": "sales", "processingLayer": "Silver"},
        ),
        ("governed.analytics.order_items", {**_DC43_TAGS, "environnement": "dev01", "team": "sales"}),
    ]


def test_linker_applies_root_contract_tags_and_schema_overrides() -> None:
    contract = _contract_with(
        SchemaObject(name="orders", physicalName="orders", tags=["layer:Silver", "env:prd"]),
        SchemaObject(name="order_items", physicalName="order_items"),
    )
    contract.tags = ["domain:sales", "layer:Bronze", "sla:daily"]

    calls = _link_tags(contract)

    assert calls == [
        (
            "governed.analytics.orders",
            {
                **_DC43_TAGS,
                "domain": "sales",
                "sla": "daily",
                "layer": "Silver",
                "env": "prd",
            },
        ),
        (
            "governed.analytics.order_items",
            {
                **_DC43_TAGS,
                "domain": "sales",
                "sla": "daily",
                "layer": "Bronze",
            },
        ),
    ]


def test_linker_supports_custom_tag_handler() -> None:
    contract = _contract_with(
        SchemaObject(name="orders", physicalName="orders", tags=["zone=eu-west-1", "confidential"]),
    )
    contract.tags = ["tier=gold"]

    def custom_eq_parser(
        c: OpenDataContractStandard,
        table_name: str,
        obj: SchemaObject | None = None,
    ) -> Mapping[str, str]:
        tags: dict[str, str] = {}
        for item in (c.tags or []) + (obj.tags or [] if obj else []):
            if "=" in item:
                k, v = item.split("=", 1)
                tags[k.strip()] = v.strip()
            else:
                tags[item.strip()] = "true"
        return tags

    calls = _link_tags(contract, contract_tag_handler=custom_eq_parser)

    assert calls == [
        (
            "governed.analytics.orders",
            {
                **_DC43_TAGS,
                "tier": "gold",
                "zone": "eu-west-1",
                "confidential": "true",
            },
        ),
    ]


@pytest.mark.parametrize(
    ("raw_tags", "expected"),
    [
        ([" frequenceMaj : Daily "], {"frequenceMaj": "Daily"}),
        (["certified"], {"certified": ""}),
        (["steward:"], {"steward": ""}),
        (["source:crm:salesforce"], {"source": "crm:salesforce"}),
        ([":orphan", "  "], {}),
        (["layer:Bronze", "layer:Silver"], {"layer": "Silver"}),
    ],
)
def test_linker_parses_contract_tags(raw_tags: list[str], expected: dict[str, str]) -> None:
    calls = _link_tags(_contract_with(SchemaObject(name="orders", tags=raw_tags)))

    assert calls == [("governed.analytics.orders", {**_DC43_TAGS, **expected})]


def test_linker_matches_contract_tags_ignoring_case_and_backticks() -> None:
    contract = _contract_with(SchemaObject(name="orders", tags=["processingLayer:Gold"]), servers=False)

    calls = _link_tags(contract, dataset_id="table:`governed`.`analytics`.`ORDERS`")

    assert calls == [("`governed`.`analytics`.`ORDERS`", {**_DC43_TAGS, "processingLayer": "Gold"})]


def test_linker_ignores_tags_of_other_schema_objects() -> None:
    contract = _contract_with(SchemaObject(name="orders", tags=["processingLayer:Gold"]), servers=False)

    calls = _link_tags(contract, dataset_id="table:governed.analytics.customers")

    assert calls == [("governed.analytics.customers", _DC43_TAGS)]


def test_linker_keeps_dc43_tags_over_contract_tags() -> None:
    contract = _contract_with(SchemaObject(name="orders", tags=["dc43.contract_id:other", "data-owner:sales"]))

    with pytest.warns(RuntimeWarning):
        calls = _link_tags(contract)

    assert calls == [("governed.analytics.orders", {**_DC43_TAGS, "data_owner": "sales"})]


def test_find_table_schema_object_uses_resolvers() -> None:
    obj_orders = SchemaObject(name="orders_v1", physicalName="orders")
    obj_items = SchemaObject(name="items")
    contract = OpenDataContractStandard(
        id="sales.orders",
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        servers=[Server(server="unity", type="databricks", catalog="governed", schema="analytics")],
        schema=[obj_orders, obj_items],
    )

    assert find_table_schema_object(contract, "governed.analytics.orders") is obj_orders
    assert find_table_schema_object(contract, "`governed`.`analytics`.`ORDERS`") is obj_orders
    assert find_table_schema_object(contract, "governed.analytics.items") is obj_items
    assert find_table_schema_object(contract, "governed.analytics.unknown") is None


def test_parse_colon_tags_edge_cases() -> None:
    assert parse_colon_tags(None) == {}
    assert parse_colon_tags([]) == {}
    assert parse_colon_tags(["k:v", "k:override", "no_val", ":no_key", "   "]) == {
        "k": "override",
        "no_val": "",
    }


def test_build_linker_from_config_passes_contract_tag_handler() -> None:
    config = UnityCatalogConfig(enabled=True, tags_enabled=True, tags_sql_dsn="sqlite:///:memory:")
    linker = build_linker_from_config(config, contract_tag_handler=colon_tag_handler)
    assert linker is not None
    assert linker.contract_tag_handler is colon_tag_handler


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


def test_build_link_hooks_injects_reserved_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, tuple[str, ...]] = {}

    def _fake_builder(
        _: UnityCatalogConfig,
        context=None,
        engine_builder=sql_table_property_updater,  # type: ignore[assignment]
        *,
        skip_tables: Sequence[str] | None = None,
        **extra: object,
    ) -> UnityCatalogLinker:
        captured["skip"] = tuple(sorted(skip_tables or ()))

        class _Linker:
            def link_dataset_contract(self, **_: object) -> None:
                pass

        return _Linker()  # type: ignore[return-value]

    monkeypatch.setattr(
        "dc43_service_backends.governance.unity_catalog.build_linker_from_config",
        _fake_builder,
    )

    config = ServiceBackendsConfig(
        unity_catalog=UnityCatalogConfig(enabled=True, sql_dsn="sqlite:///tmp.db"),
        contract_store=ContractStoreConfig(table="dev.catalog.contracts"),
        data_product_store=DataProductStoreConfig(table="dev.catalog.products"),
        governance_store=GovernanceStoreConfig(
            table="dev.catalog.dq_status",
            status_table="dev.catalog.dq_status",
            activity_table="dev.catalog.dq_activity",
            link_table="dev.catalog.dq_links",
            metrics_table="dev.catalog.dq_metrics",
        ),
    )

    hooks = build_link_hooks(config)

    assert hooks is not None and len(hooks) == 1
    assert set(captured["skip"]) == {
        "dev.catalog.contracts",
        "dev.catalog.products",
        "dev.catalog.dq_status",
        "dev.catalog.dq_activity",
        "dev.catalog.dq_links",
        "dev.catalog.dq_metrics",
    }


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


def test_build_link_hooks_passes_contract_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _ContractService:
        def get(self, contract_id: str, contract_version: str) -> None:
            captured["requested"] = (contract_id, contract_version)
            return None

    class _Linker:
        def link_dataset_contract(self, **_: object) -> None:
            pass

    def _fake_builder(
        config: UnityCatalogConfig,
        *,
        contract_loader=None,
        **_: object,
    ) -> _Linker:
        captured["loader"] = contract_loader
        return _Linker()

    monkeypatch.setattr(
        "dc43_service_backends.governance.unity_catalog.build_linker_from_config",
        _fake_builder,
    )

    config = ServiceBackendsConfig(
        unity_catalog=UnityCatalogConfig(enabled=True, sql_dsn="sqlite:///tmp.db"),
    )
    context = LinkHookContext(contract_service=_ContractService())

    hooks = build_link_hooks(config, context=context)

    assert hooks is not None and len(hooks) == 1
    loader = captured.get("loader")
    assert callable(loader)
    loader("sales.orders", "0.1.0")
    assert captured["requested"] == ("sales.orders", "0.1.0")


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
        lambda cfg, **kwargs: linker,
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


def test_build_dataset_contract_link_hooks_pass_context() -> None:
    config = ServiceBackendsConfig()
    context = LinkHookContext(contract_service=SimpleNamespace())
    captured: dict[str, object] = {}

    def _builder(_: ServiceBackendsConfig, context: LinkHookContext | None = None):
        captured["context"] = context
        return ()

    hooks = build_dataset_contract_link_hooks(
        config,
        include_defaults=False,
        extra_builders=[_builder],
        context=context,
    )

    assert hooks == ()
    assert captured["context"] is context


def test_build_dataset_contract_link_hooks_uses_configured_specs(monkeypatch) -> None:
    config = ServiceBackendsConfig()
    config.governance.dataset_contract_link_builders = (
        "custom.module:builder",
        "custom.module:builder",
    )

    loaded: list[str] = []

    def _loader(spec: str):
        loaded.append(spec)
        return lambda cfg, context=None: None

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
