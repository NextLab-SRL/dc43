from __future__ import annotations

"""Unity Catalog synchronisation helpers for governance backends."""

from dataclasses import dataclass, field
import warnings
from typing import Any, Callable, Mapping, MutableMapping, Optional, Protocol, Sequence

from dc43_service_backends.config import ServiceBackendsConfig, UnityCatalogConfig

from .hooks import DatasetContractLinkHook


class TablePropertyUpdater(Protocol):
    """Callable that applies table properties in Unity Catalog."""

    def __call__(self, table_name: str, properties: Mapping[str, str]) -> None:
        ...


class TableTagUpdater(Protocol):
    """Callable that applies Unity Catalog tags to a table."""

    def __call__(
        self,
        table_name: str,
        tags: Mapping[str, str],
        unset_tags: Sequence[str] = (),
    ) -> None:
        ...


MetadataProvider = Callable[[str, str, str, str], Mapping[str, object]]
DatasetToTable = Callable[[str], Optional[str]]
EngineBuilder = Callable[[str], Any]


_RESERVED_PROPERTY_KEYS = frozenset({"owner"})
_INVALID_TAG_CHARACTERS = frozenset({".", ",", "-", "=", "/", ":"})


def _default_metadata(
    dataset_id: str,
    dataset_version: str,
    contract_id: str,
    contract_version: str,
) -> Mapping[str, object]:
    properties: MutableMapping[str, object] = {
        "dc43.contract_id": contract_id,
        "dc43.contract_version": contract_version,
    }
    if dataset_version:
        properties["dc43.dataset_version"] = dataset_version
    return properties


def prefix_table_resolver(prefix: str = "table:") -> DatasetToTable:
    """Build a dataset identifier resolver based on a fixed prefix."""

    normalised = prefix or ""

    def _resolve(dataset_id: str) -> Optional[str]:
        if not normalised:
            return dataset_id
        if dataset_id.startswith(normalised):
            return dataset_id[len(normalised) :]
        return None

    return _resolve


def _normalise_property_key(key: str) -> str | None:
    text = str(key).strip()
    if not text:
        return None
    if text.lower() in _RESERVED_PROPERTY_KEYS:
        warnings.warn(
            f"Unity Catalog property '{text}' is reserved and will be ignored.",
            RuntimeWarning,
            stacklevel=3,
        )
        return None
    return text


def _normalise_tag_key(key: str) -> str | None:
    text = str(key).strip()
    if not text:
        return None
    replaced = False
    cleaned = []
    for char in text:
        if char in _INVALID_TAG_CHARACTERS:
            cleaned.append("_")
            replaced = True
        else:
            cleaned.append(char)
    normalised = "".join(cleaned)
    if not normalised:
        return None
    if replaced:
        warnings.warn(
            f"Unity Catalog tag '{text}' contains reserved characters; converted to '{normalised}'.",
            RuntimeWarning,
            stacklevel=3,
        )
    if normalised.lower() in _RESERVED_PROPERTY_KEYS:
        warnings.warn(
            f"Unity Catalog tag '{text}' resolves to a reserved name and will be ignored.",
            RuntimeWarning,
            stacklevel=3,
        )
        return None
    return normalised


def _build_properties(
    metadata: Mapping[str, object],
    extra: Mapping[str, str],
) -> Mapping[str, str]:
    serialised: dict[str, str] = {}
    for source in (extra, metadata):
        for key, value in source.items():
            if value is None:
                continue
            normalised = _normalise_property_key(key)
            if not normalised:
                continue
            serialised[normalised] = str(value)
    return serialised


def _build_tags(
    metadata: Mapping[str, object],
    extra: Mapping[str, str],
) -> tuple[Mapping[str, str], set[str]]:
    serialised: dict[str, str] = {}
    keys: set[str] = set()
    for source in (extra, metadata):
        for key, value in source.items():
            normalised = _normalise_tag_key(key)
            if not normalised:
                continue
            keys.add(normalised)
            if value is None:
                continue
            serialised[normalised] = str(value)
    return serialised, keys


@dataclass(slots=True)
class UnityCatalogLinker:
    """Apply Unity Catalog table properties after governance link operations."""

    apply_table_properties: TablePropertyUpdater | None = None
    apply_table_tags: TableTagUpdater | None = None
    table_resolver: DatasetToTable = prefix_table_resolver()
    metadata_provider: MetadataProvider = _default_metadata
    static_properties: Mapping[str, str] = field(default_factory=dict)
    static_tags: Mapping[str, str] = field(default_factory=dict)

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        table_name = self.table_resolver(dataset_id)
        if table_name is None or not table_name:
            return
        metadata = self.metadata_provider(
            dataset_id,
            dataset_version,
            contract_id,
            contract_version,
        )
        properties = _build_properties(metadata, dict(self.static_properties))
        if properties and self.apply_table_properties:
            try:
                self.apply_table_properties(table_name, properties)
            except Exception as exc:  # pragma: no cover - runtime guard
                warnings.warn(
                    f"Unity Catalog property sync failed for '{table_name}': {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )
        if self.apply_table_tags:
            tags, tag_keys = _build_tags(metadata, dict(self.static_tags))
            unset: tuple[str, ...] = ()
            if not tags and tag_keys:
                unset = tuple(sorted(tag_keys))
            try:
                self.apply_table_tags(table_name, tags, unset)
            except Exception as exc:  # pragma: no cover - runtime guard
                warnings.warn(
                    f"Unity Catalog tag sync failed for '{table_name}': {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )


def _quote_identifier(identifier: str) -> str:
    segments = [segment.strip() for segment in identifier.split(".") if segment.strip()]
    if not segments:
        raise ValueError("Unity Catalog table name is empty")
    escaped = [f"`{segment.replace('`', '``')}`" for segment in segments]
    return ".".join(escaped)


def _quote_literal(value: str) -> str:
    return value.replace("'", "''")


def sql_table_property_updater(engine: Any) -> TablePropertyUpdater:
    """Build a table-property updater backed by a SQLAlchemy engine."""

    try:  # pragma: no cover - optional import when SQL is unused
        from sqlalchemy import text
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError("sqlalchemy is required for SQL-based Unity Catalog tagging") from exc

    def _render_statement(table_name: str, properties: Mapping[str, str]) -> str:
        identifier = _quote_identifier(table_name)
        assignments = ", ".join(
            f"'{_quote_literal(key)}'='{_quote_literal(value)}'"
            for key, value in sorted(properties.items())
        )
        return f"ALTER TABLE {identifier} SET TBLPROPERTIES ({assignments})"

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        if not properties:
            return
        statement = text(_render_statement(table_name, properties))
        with engine.begin() as conn:
            conn.execute(statement)

    return _update


def sql_table_tag_updater(engine: Any) -> TableTagUpdater:
    """Build a Unity Catalog tag updater backed by a SQLAlchemy engine."""

    try:  # pragma: no cover - optional import when SQL is unused
        from sqlalchemy import text
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError("sqlalchemy is required for SQL-based Unity Catalog tagging") from exc

    def _render_set_statement(table_name: str, tags: Mapping[str, str]) -> str:
        identifier = _quote_identifier(table_name)
        assignments = ", ".join(
            f"'{_quote_literal(key)}'='{_quote_literal(value)}'" for key, value in sorted(tags.items())
        )
        return f"ALTER TABLE {identifier} SET TAGS ({assignments})"

    def _render_unset_statement(table_name: str, tag_names: Sequence[str]) -> str:
        identifier = _quote_identifier(table_name)
        assignments = ", ".join(f"'{_quote_literal(name)}'" for name in sorted(tag_names))
        return f"ALTER TABLE {identifier} UNSET TAGS ({assignments})"

    def _update(table_name: str, tags: Mapping[str, str], unset_tags: Sequence[str] = ()) -> None:
        statements = []
        if unset_tags:
            statements.append(text(_render_unset_statement(table_name, unset_tags)))
        if tags:
            statements.append(text(_render_set_statement(table_name, tags)))
        if not statements:
            return
        with engine.begin() as conn:
            for statement in statements:
                conn.execute(statement)

    return _update


def _default_engine_builder(dsn: str) -> Any:
    from sqlalchemy import create_engine  # pragma: no cover - exercised via tests

    return create_engine(dsn)


def build_linker_from_config(
    config: UnityCatalogConfig,
    engine_builder: EngineBuilder = _default_engine_builder,
) -> UnityCatalogLinker | None:
    """Return a Unity Catalog linker configured from backend settings."""

    if not config.enabled:
        return None

    updater: TablePropertyUpdater | None = None
    tag_updater: TableTagUpdater | None = None
    property_engine: Any | None = None

    if config.sql_dsn:
        try:
            property_engine = engine_builder(config.sql_dsn)
        except Exception as exc:  # pragma: no cover - runtime guard
            warnings.warn(
                f"Unity Catalog SQL engine could not be created: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
        else:
            updater = sql_table_property_updater(property_engine)
    else:
        warnings.warn(
            "Unity Catalog property propagation is disabled because unity_catalog.sql_dsn was not provided.",
            RuntimeWarning,
            stacklevel=2,
        )

    if config.tags_enabled:
        tags_dsn = config.tags_sql_dsn or config.sql_dsn
        tag_engine: Any | None = None
        if tags_dsn:
            reuse_property_engine = property_engine is not None and tags_dsn == config.sql_dsn
            if reuse_property_engine:
                tag_engine = property_engine
            else:
                try:
                    tag_engine = engine_builder(tags_dsn)
                except Exception as exc:  # pragma: no cover - runtime guard
                    warnings.warn(
                        f"Unity Catalog tag SQL engine could not be created: {exc}",
                        RuntimeWarning,
                        stacklevel=2,
                    )
        else:
            warnings.warn(
                "Unity Catalog tag propagation is enabled but no sql_dsn/tags_sql_dsn was provided.",
                RuntimeWarning,
                stacklevel=2,
            )
        if tag_engine is not None:
            tag_updater = sql_table_tag_updater(tag_engine)

    resolver = prefix_table_resolver(config.dataset_prefix)
    static_properties = dict(config.static_properties)
    static_tags = dict(config.static_tags)

    if updater is None and tag_updater is None:
        warnings.warn(
            "Unity Catalog integration is enabled but neither property nor tag engines could be initialised.",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    return UnityCatalogLinker(
        apply_table_properties=updater,
        apply_table_tags=tag_updater,
        table_resolver=resolver,
        static_properties=static_properties,
        static_tags=static_tags,
    )


def build_link_hooks(
    config: ServiceBackendsConfig,
) -> Sequence[DatasetContractLinkHook] | None:
    """Return Unity Catalog link hooks derived from ``config``.

    The hooks encapsulate the Unity Catalog linker so the governance bootstrapper
    can keep Databricks-specific concerns outside of the core web application.
    """

    try:
        linker = build_linker_from_config(config.unity_catalog)
    except Exception as exc:  # pragma: no cover - defensive guard
        warnings.warn(
            f"Unity Catalog integration disabled due to error: {exc}",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    if linker is None:
        return None

    return (linker.link_dataset_contract,)


__all__ = [
    "UnityCatalogLinker",
    "TablePropertyUpdater",
    "TableTagUpdater",
    "sql_table_property_updater",
    "sql_table_tag_updater",
    "build_linker_from_config",
    "prefix_table_resolver",
    "build_link_hooks",
]
