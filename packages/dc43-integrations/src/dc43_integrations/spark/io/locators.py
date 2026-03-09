from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyspark.sql import DataFrame, SparkSession
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore

from dc43_service_backends.core.odcs import custom_properties_dict
from dc43_service_backends.core.versioning import SemVer

from dc43_integrations.spark.io.common import (
    dataset_id_from_ref,
    _promote_delta_path_to_table,
    _safe_fs_name,
)
from dc43_integrations.spark.io.resolution import (
    DatasetResolution,
    DatasetLocatorStrategy,
)

logger = logging.getLogger(__name__)


def _timestamp() -> str:
    """Return an ISO timestamp suitable for dataset versioning."""
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    return now.isoformat().replace("+00:00", "Z")


def _ref_from_contract(contract: OpenDataContractStandard) -> tuple[Optional[str], Optional[str]]:
    """Return ``(path, table)`` derived from the contract's first server."""
    if not contract.servers:
        return None, None
    server: Server = contract.servers[0]
    try:
        path = server.path  # type: ignore
    except AttributeError:
        path = None
    if path:
        return path, None
    # Build table name from catalog/schema/database/dataset parts when present
    try:
        last = server.dataset or server.database  # type: ignore
        parts = [server.catalog, server.schema_, last]  # type: ignore
        table = ".".join([p for p in parts if p]) if any(parts) else None
        return None, table
    except AttributeError:
        return None, None


@dataclass
class ContractFirstDatasetLocator:
    """Default locator that favours contract servers over provided hints."""

    clock: Callable[[], str] = _timestamp

    def _resolve_base(
        self,
        contract: Optional[OpenDataContractStandard],
        *,
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
        spark: Optional[SparkSession] = None,
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[Server]]:
        server: Optional[Server] = None
        if contract and contract.servers:
            c_path, c_table = _ref_from_contract(contract)
            server = contract.servers[0]
            if server is not None:
                try:
                    c_format = server.format  # type: ignore[attr-defined]
                except AttributeError:
                    c_format = None
            else:
                c_format = None
            if c_path is not None:
                path = c_path
            if c_table is not None:
                table = c_table
            if c_format is not None and format is None:
                format = c_format
        path, table = _promote_delta_path_to_table(
            path=path,
            table=table,
            format=format,
            spark=spark,
        )
        return path, table, format, server

    def _resolution(
        self,
        contract: Optional[OpenDataContractStandard],
        *,
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
        include_timestamp: bool,
    ) -> DatasetResolution:
        dataset_id = contract.id if contract else dataset_id_from_ref(table=table, path=path)
        dataset_version = self.clock() if include_timestamp else None
        server_props: Optional[Dict[str, Any]] = None
        read_options: Optional[Dict[str, str]] = None
        write_options: Optional[Dict[str, str]] = None
        if contract and contract.servers:
            first = contract.servers[0]
            props = custom_properties_dict(first)
            if props:
                server_props = props
                versioning = props.get(ContractVersionLocator.VERSIONING_PROPERTY)
                if isinstance(versioning, Mapping):
                    read_map = versioning.get("readOptions")
                    if isinstance(read_map, Mapping):
                        read_options = {
                            str(k): str(v)
                            for k, v in read_map.items()
                            if v is not None
                        }
                    write_map = versioning.get("writeOptions")
                    if isinstance(write_map, Mapping):
                        write_options = {
                            str(k): str(v)
                            for k, v in write_map.items()
                            if v is not None
                        }
        return DatasetResolution(
            path=path,
            table=table,
            format=format,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            read_options=read_options,
            write_options=write_options,
            custom_properties=server_props,
            load_paths=None,
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        path, table, format, _ = self._resolve_base(
            contract,
            path=path,
            table=table,
            format=format,
            spark=spark,
        )
        return self._resolution(
            contract,
            path=path,
            table=table,
            format=format,
            include_timestamp=False,
        )

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        path, table, format, _ = self._resolve_base(
            contract,
            path=path,
            table=table,
            format=format,
            spark=None,
        )
        return self._resolution(
            contract,
            path=path,
            table=table,
            format=format,
            include_timestamp=True,
        )


@dataclass
class StaticDatasetLocator:
    """Locator overriding specific fields while delegating to a base strategy."""

    dataset_id: Optional[str] = None
    dataset_version: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    format: Optional[str] = None
    base: DatasetLocatorStrategy = field(default_factory=ContractFirstDatasetLocator)

    def _merge(self, resolution: DatasetResolution) -> DatasetResolution:
        return DatasetResolution(
            path=self.path or resolution.path,
            table=self.table or resolution.table,
            format=self.format or resolution.format,
            dataset_id=self.dataset_id or resolution.dataset_id,
            dataset_version=self.dataset_version or resolution.dataset_version,
            read_options=dict(resolution.read_options or {}),
            write_options=dict(resolution.write_options or {}),
            custom_properties=resolution.custom_properties,
            load_paths=list(resolution.load_paths or []),
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        base_resolution = self.base.for_read(
            contract=contract,
            spark=spark,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(base_resolution)

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        base_resolution = self.base.for_write(
            contract=contract,
            df=df,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(base_resolution)


@dataclass
class ContractVersionLocator:
    """Locator that appends a version directory or time-travel hint."""

    dataset_version: str
    dataset_id: Optional[str] = None
    subpath: Optional[str] = None
    base: DatasetLocatorStrategy = field(default_factory=ContractFirstDatasetLocator)

    VERSIONING_PROPERTY = "dc43.core.versioning"

    @staticmethod
    def _version_key(value: str) -> tuple[int, Tuple[int, int, int] | float | str, str]:
        candidate = value
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(candidate)
            return (0, dt.timestamp(), value)
        except ValueError:
            pass
        try:
            parsed = SemVer.parse(value)
            return (1, (parsed.major, parsed.minor, parsed.patch), value)
        except ValueError:
            return (2, value, value)

    @staticmethod
    def _render_template(template: str, *, version_value: str, safe_value: str) -> str:
        return (
            template.replace("{version}", version_value)
            .replace("{safeVersion}", safe_value)
        )

    @staticmethod
    def _folder_version_value(path: Path) -> str:
        marker = path / ".dc43_version"
        if marker.exists():
            try:
                text = marker.read_text().strip()
            except OSError:
                text = ""
            if text:
                return text
        return path.name

    @classmethod
    def _versioning_config(cls, resolution: DatasetResolution) -> Optional[Mapping[str, Any]]:
        props = resolution.custom_properties or {}
        value = props.get(cls.VERSIONING_PROPERTY)
        if isinstance(value, Mapping):
            return value
        return None

    @classmethod
    def _expand_versioning_paths(
        cls,
        resolution: DatasetResolution,
        *,
        base_path: Optional[str],
        dataset_version: Optional[str],
    ) -> tuple[Optional[List[str]], Dict[str, str]]:
        config = cls._versioning_config(resolution)
        if not config or not base_path or not dataset_version:
            return None, {}

        base = Path(base_path)
        base_dir = base.parent if base.suffix else base
        if not base_dir.exists():
            return None, {}

        include_prior = bool(config.get("includePriorVersions"))
        folder_template = str(config.get("subfolder", "{version}"))
        file_pattern = config.get("filePattern")
        if file_pattern is not None:
            file_pattern = str(file_pattern)
        elif base.suffix:
            file_pattern = base.name

        dataset_version_normalised = dataset_version
        lower = dataset_version.lower()
        entries: List[tuple[str, str]] = []
        try:
            for entry in base_dir.iterdir():
                if not entry.is_dir():
                    continue
                display = cls._folder_version_value(entry)
                entries.append((display, entry.name))
        except FileNotFoundError:
            return None, {}
        if not entries:
            return None, {}
        entries.sort(key=lambda item: cls._version_key(item[0]))

        selected: List[tuple[str, str]] = []
        if lower == "latest":
            alias_key = None
            alias_path = base_dir / dataset_version_normalised
            if alias_path.exists():
                try:
                    resolved_alias = alias_path.resolve()
                except OSError:
                    resolved_alias = alias_path
                if resolved_alias.is_dir():
                    alias_display = cls._folder_version_value(resolved_alias)
                    alias_key = cls._version_key(alias_display)

            if include_prior:
                if alias_key is not None:
                    selected = [
                        entry for entry in entries if cls._version_key(entry[0]) <= alias_key
                    ]
                else:
                    selected = entries
            elif entries:
                if alias_key is not None:
                    selected = [
                        entry for entry in entries if cls._version_key(entry[0]) == alias_key
                    ]
                    if not selected and entries:
                        selected = [entries[-1]]
                else:
                    selected = [entries[-1]]
        else:
            target_key = cls._version_key(dataset_version_normalised)
            eligible = [entry for entry in entries if cls._version_key(entry[0]) <= target_key]
            alias_like = "__" in dataset_version_normalised
            effective_include_prior = include_prior and not alias_like
            if effective_include_prior:
                selected = eligible
            else:
                exact = next((entry for entry in entries if entry[0] == dataset_version_normalised), None)
                if exact:
                    selected = [exact]
                else:
                    safe_candidate = _safe_fs_name(dataset_version_normalised)
                    fallback = next((entry for entry in entries if entry[1] == safe_candidate), None)
                    if fallback:
                        selected = [fallback]
                    elif eligible:
                        selected = [eligible[-1]]

        if not selected:
            candidate_path = base_dir / dataset_version_normalised
            if candidate_path.exists():
                selected = [(dataset_version_normalised, candidate_path.name)]
            else:
                return None, {}

        resolved_paths: List[str] = []
        for display_value, folder_name in selected:
            rendered_folder = cls._render_template(
                folder_template,
                version_value=display_value,
                safe_value=folder_name,
            )
            root = base_dir / rendered_folder if rendered_folder else base_dir
            if not root.exists():
                fallback_root = base_dir / folder_name
                if fallback_root.exists():
                    root = fallback_root
            if file_pattern:
                pattern = cls._render_template(
                    file_pattern,
                    version_value=display_value,
                    safe_value=folder_name,
                )
                matches = list(root.glob(pattern))
                if matches:
                    resolved_paths.extend(str(path) for path in matches)
            else:
                if root.exists():
                    resolved_paths.append(str(root))

        read_opts: Dict[str, str] = {}
        extra_read = config.get("readOptions")
        if isinstance(extra_read, Mapping):
            for k, v in extra_read.items():
                if isinstance(v, bool):
                    read_opts[str(k)] = str(v).lower()
                else:
                    read_opts[str(k)] = str(v)

        return (resolved_paths or None), read_opts

    def _resolve_path(self, resolution: DatasetResolution) -> Optional[str]:
        path = resolution.path
        if not path:
            return None

        fmt = (resolution.format or "").lower()
        if fmt == "delta":
            return path

        base = Path(path)
        safe_component: Optional[str] = None
        if self.dataset_version:
            candidate = _safe_fs_name(self.dataset_version)
            if candidate and candidate != self.dataset_version:
                safe_component = candidate

        if base.suffix:
            version_component = self.dataset_version
            parent = base.parent / base.stem
            if safe_component and version_component:
                preferred_dir = parent / version_component
                if not preferred_dir.exists():
                    version_component = safe_component
            elif safe_component and not version_component:
                version_component = safe_component

            folder = parent / version_component if version_component else parent
            if self.subpath:
                folder = folder / self.subpath
            target = folder / base.name
            return str(target)

        version_component = self.dataset_version
        if safe_component and version_component:
            preferred_dir = base / version_component
            if not preferred_dir.exists():
                version_component = safe_component
        elif safe_component and not version_component:
            version_component = safe_component

        folder = base / version_component if version_component else base
        if self.subpath:
            folder = folder / self.subpath
        return str(folder)

    @staticmethod
    def _delta_time_travel_option(dataset_version: Optional[str]) -> Optional[tuple[str, str]]:
        if not dataset_version:
            return None

        version = dataset_version.strip()
        if not version or version.lower() == "latest":
            return None

        if version.isdigit():
            return "versionAsOf", version

        candidate = version
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            datetime.fromisoformat(candidate)
        except ValueError:
            return None
        return "timestampAsOf", version

    def _merge(
        self,
        contract: Optional[OpenDataContractStandard],
        resolution: DatasetResolution,
    ) -> DatasetResolution:
        resolved_path = self._resolve_path(resolution)
        dataset_id = self.dataset_id or resolution.dataset_id
        if dataset_id is None and contract is not None:
            dataset_id = contract.id
        read_options = dict(resolution.read_options or {})
        write_options = dict(resolution.write_options or {})
        load_paths = list(resolution.load_paths or [])
        base_path_hint = resolution.path
        version_paths, extra_read_options = self._expand_versioning_paths(
            resolution,
            base_path=base_path_hint,
            dataset_version=self.dataset_version,
        )
        if version_paths:
            load_paths = version_paths
            resolved_path = base_path_hint or resolved_path
        if extra_read_options:
            read_options.update(extra_read_options)
        if (resolution.format or "").lower() == "delta":
            option = self._delta_time_travel_option(self.dataset_version)
            if option:
                read_options.setdefault(*option)
        return DatasetResolution(
            path=resolved_path or resolution.path,
            table=resolution.table,
            format=resolution.format,
            dataset_id=dataset_id,
            dataset_version=self.dataset_version,
            read_options=read_options or None,
            write_options=write_options or None,
            custom_properties=resolution.custom_properties,
            load_paths=load_paths or None,
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        base_resolution = self.base.for_read(
            contract=contract,
            spark=spark,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(contract, base_resolution)

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        base_resolution = self.base.for_write(
            contract=contract,
            df=df,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(contract, base_resolution)
