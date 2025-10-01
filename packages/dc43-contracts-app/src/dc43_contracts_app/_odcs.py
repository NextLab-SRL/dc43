"""Lightweight helpers for working with ODCS custom properties."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Dict


def normalise_custom_properties(raw: Any) -> list[Any]:
    """Return custom property entries as a list of rich objects or mappings."""

    if raw is None or isinstance(raw, (str, bytes, bytearray)):
        return []
    if isinstance(raw, property):
        return []
    if isinstance(raw, Mapping):
        iterable = raw.values()
    elif isinstance(raw, Iterable):
        iterable = raw
    else:
        try:
            iterable = list(raw)
        except TypeError:
            return []
    return [item for item in iterable if item is not None]


def custom_properties_dict(source: Any) -> Dict[str, Any]:
    """Return a mapping of ``property`` -> ``value`` entries."""

    props: Dict[str, Any] = {}
    for item in normalise_custom_properties(getattr(source, "customProperties", None)):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        else:
            key = getattr(item, "property", None)
            value = getattr(item, "value", None)
        if key:
            props[str(key)] = value
    return props

__all__ = ["custom_properties_dict", "normalise_custom_properties"]
