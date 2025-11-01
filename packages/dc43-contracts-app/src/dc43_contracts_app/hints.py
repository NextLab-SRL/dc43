"""Optional workspace hint registration used by demo integrations."""

from __future__ import annotations

from typing import Callable, Mapping, MutableMapping

__all__ = [
    "get_workspace_hints",
    "register_workspace_hint_supplier",
    "reset_workspace_hint_supplier",
]

_HOOK: Callable[[], Mapping[str, str] | MutableMapping[str, str] | None] | None = None


def register_workspace_hint_supplier(
    supplier: Callable[[], Mapping[str, str] | MutableMapping[str, str] | None]
) -> None:
    """Register ``supplier`` providing filesystem hints for legacy screens."""

    global _HOOK
    _HOOK = supplier


def get_workspace_hints() -> Mapping[str, str] | MutableMapping[str, str] | None:
    """Return the currently registered workspace hints if available."""

    if _HOOK is None:
        return None
    return _HOOK()


def reset_workspace_hint_supplier() -> None:
    """Clear any registered workspace hint supplier."""

    global _HOOK
    _HOOK = None
