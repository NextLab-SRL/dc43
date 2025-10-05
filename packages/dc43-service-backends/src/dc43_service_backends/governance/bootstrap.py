"""Bootstrap helpers for wiring governance extensions."""

from __future__ import annotations

from importlib import import_module
from typing import Callable, Iterable, Sequence
import warnings

from dc43_service_backends.config import ServiceBackendsConfig

from .hooks import DatasetContractLinkHook

LinkHookBuilder = Callable[[ServiceBackendsConfig], Sequence[DatasetContractLinkHook] | None]
LinkHookBuilderSpec = str

DEFAULT_LINK_HOOK_BUILDER_SPECS: tuple[LinkHookBuilderSpec, ...] = (
    "dc43_service_backends.governance.unity_catalog:build_link_hooks",
)


def load_link_hook_builder(spec: LinkHookBuilderSpec) -> LinkHookBuilder:
    """Import and return a link-hook builder referenced by ``spec``.

    The specification accepts the ``module:attribute`` format used by several
    Python plugin systems. When the delimiter is omitted, the right-most dot is
    treated as the attribute separator so dotted attribute paths remain
    supported.
    """

    if not spec:
        raise ValueError("link hook builder specification cannot be empty")

    module_name: str
    attribute_path: str
    if ":" in spec:
        module_name, attribute_path = spec.split(":", 1)
    else:
        module_name, _, attribute_path = spec.rpartition(".")
        if not module_name or not attribute_path:
            raise ValueError(
                "link hook builder specification must include a module and attribute"
            )

    module = import_module(module_name)
    target: object = module
    for part in attribute_path.split("."):
        if not part:
            raise ValueError(
                f"invalid link hook builder attribute path in specification '{spec}'"
            )
        target = getattr(target, part)

    if not callable(target):  # pragma: no cover - defensive guard
        raise TypeError(f"link hook builder '{spec}' is not callable")

    return target  # type: ignore[return-value]


def _builder_specs_from_config(
    config: ServiceBackendsConfig,
    *,
    include_defaults: bool,
) -> tuple[LinkHookBuilderSpec, ...]:
    specs: list[LinkHookBuilderSpec] = []
    if include_defaults:
        specs.extend(DEFAULT_LINK_HOOK_BUILDER_SPECS)
    specs.extend(config.governance.dataset_contract_link_builders)

    ordered: list[LinkHookBuilderSpec] = []
    seen: set[LinkHookBuilderSpec] = set()
    for spec in specs:
        if not spec or spec in seen:
            continue
        seen.add(spec)
        ordered.append(spec)
    return tuple(ordered)


def build_dataset_contract_link_hooks(
    config: ServiceBackendsConfig,
    *,
    include_defaults: bool = True,
    extra_builders: Iterable[LinkHookBuilder] | None = None,
) -> tuple[DatasetContractLinkHook, ...]:
    """Assemble dataset-contract link hooks for the active configuration."""

    builders: list[LinkHookBuilder] = []
    for spec in _builder_specs_from_config(config, include_defaults=include_defaults):
        try:
            builder = load_link_hook_builder(spec)
        except Exception as exc:  # pragma: no cover - defensive guard
            warnings.warn(
                f"Dataset-contract link hook builder '{spec}' failed to load: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
            continue
        builders.append(builder)

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
    "DEFAULT_LINK_HOOK_BUILDER_SPECS",
    "LinkHookBuilder",
    "LinkHookBuilderSpec",
    "load_link_hook_builder",
]
