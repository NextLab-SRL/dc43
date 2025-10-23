from __future__ import annotations

"""Unity Catalog synchronisation helpers for governance backends."""

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, MutableMapping, Optional, Protocol, Sequence
import warnings

from dc43_service_backends.config import ServiceBackendsConfig, UnityCatalogConfig

from .hooks import DatasetContractLinkHook


class TablePropertyUpdater(Protocol):
    """Callable that applies table properties in Unity Catalog."""

    def __call__(self, table_name: str, properties: Mapping[str, str]) -> None:
        ...


MetadataProvider = Callable[[str, str, str, str], Mapping[str, object]]
DatasetToTable = Callable[[str], Optional[str]]
WorkspaceBuilder = Callable[[UnityCatalogConfig], Any]


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


def _serialise(metadata: Mapping[str, object], extra: Mapping[str, str]) -> Mapping[str, str]:
    serialised: dict[str, str] = dict(extra)
    for key, value in metadata.items():
        if value is None:
            continue
        serialised[str(key)] = str(value)
    return serialised


@dataclass(slots=True)
class UnityCatalogLinker:
    """Apply Unity Catalog table properties after governance link operations."""

    apply_table_properties: TablePropertyUpdater
    table_resolver: DatasetToTable = prefix_table_resolver()
    metadata_provider: MetadataProvider = _default_metadata
    static_properties: Mapping[str, str] = field(default_factory=dict)

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
        properties = _serialise(metadata, dict(self.static_properties))
        if properties:
            self.apply_table_properties(table_name, properties)


def workspace_table_property_updater(workspace: Any) -> TablePropertyUpdater:
    """Build a table-property updater backed by the Databricks workspace client.

    The updater expects an instance compatible with
    :class:`databricks.sdk.WorkspaceClient` and forwards property updates to
    ``workspace.tables.update`` so Unity Catalog reflects the latest contract
    linkage metadata.
    """

    def _update(table_name: str, properties: Mapping[str, str]) -> None:
        workspace.tables.update(name=table_name, properties=dict(properties))

    return _update


def _default_workspace_builder(config: UnityCatalogConfig) -> Any:
    try:  # pragma: no cover - optional dependency
        from databricks.sdk import WorkspaceClient
    except ModuleNotFoundError:  # pragma: no cover - fallback when SDK absent
        warnings.warn(
            "databricks-sdk is not installed; Unity Catalog synchronisation is disabled",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    kwargs: dict[str, Any] = {}
    if config.workspace_profile:
        kwargs["profile"] = config.workspace_profile
    if config.workspace_url:
        kwargs["host"] = config.workspace_url
    if config.workspace_token:
        kwargs["token"] = config.workspace_token
    return WorkspaceClient(**kwargs)


def build_linker_from_config(
    config: UnityCatalogConfig,
    *,
    workspace_builder: WorkspaceBuilder = _default_workspace_builder,
) -> UnityCatalogLinker | None:
    """Return a Unity Catalog linker configured from backend settings."""

    if not config.enabled:
        return None

    workspace = workspace_builder(config)
    if workspace is None:
        return None

    updater = workspace_table_property_updater(workspace)
    resolver = prefix_table_resolver(config.dataset_prefix)
    static_properties = dict(config.static_properties)
    return UnityCatalogLinker(
        apply_table_properties=updater,
        table_resolver=resolver,
        static_properties=static_properties,
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
    "workspace_table_property_updater",
    "build_linker_from_config",
    "prefix_table_resolver",
    "build_link_hooks",
]
