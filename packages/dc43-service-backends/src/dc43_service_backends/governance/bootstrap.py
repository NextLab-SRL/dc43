"""Bootstrap helpers for wiring governance extensions."""

from __future__ import annotations

from typing import Callable, Iterable, Sequence
import warnings

from dc43_service_backends.config import ServiceBackendsConfig

from .hooks import DatasetContractLinkHook
from .unity_catalog import build_linker_from_config

LinkHookBuilder = Callable[[ServiceBackendsConfig], Sequence[DatasetContractLinkHook] | None]


def _unity_catalog_hooks(config: ServiceBackendsConfig) -> Sequence[DatasetContractLinkHook] | None:
    """Return Unity Catalog hook implementations based on configuration."""

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


DEFAULT_LINK_HOOK_BUILDERS: tuple[LinkHookBuilder, ...] = (_unity_catalog_hooks,)


def build_dataset_contract_link_hooks(
    config: ServiceBackendsConfig,
    *,
    include_defaults: bool = True,
    extra_builders: Iterable[LinkHookBuilder] | None = None,
) -> tuple[DatasetContractLinkHook, ...]:
    """Assemble dataset-contract link hooks for the active configuration."""

    builders: list[LinkHookBuilder] = []
    if include_defaults:
        builders.extend(DEFAULT_LINK_HOOK_BUILDERS)
    if extra_builders:
        builders.extend(extra_builders)

    hooks: list[DatasetContractLinkHook] = []
    for builder in builders:
        try:
            result = builder(config)
        except Exception as exc:  # pragma: no cover - defensive guard
            warnings.warn(
                f"Dataset-contract link hook builder failed: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
            continue

        if not result:
            continue

        hooks.extend(result)

    return tuple(hooks)


__all__ = [
    "build_dataset_contract_link_hooks",
    "DEFAULT_LINK_HOOK_BUILDERS",
    "LinkHookBuilder",
]
