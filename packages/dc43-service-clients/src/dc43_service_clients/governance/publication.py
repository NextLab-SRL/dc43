"""Helpers for configuring governance publication modes."""

from __future__ import annotations

import os
from enum import Enum
from typing import Mapping, Optional, Sequence


class GovernancePublicationMode(str, Enum):
    """Describe how governance interactions should publish pipeline activity."""

    LEGACY = "legacy"
    OPEN_DATA_LINEAGE = "open_data_lineage"
    OPEN_TELEMETRY = "open_telemetry"

    @classmethod
    def from_value(
        cls,
        value: "GovernancePublicationMode | str | None",
        *,
        default: "GovernancePublicationMode" = None,
    ) -> "GovernancePublicationMode":
        """Normalise ``value`` into a :class:`GovernancePublicationMode`."""

        if default is None:
            default = cls.LEGACY
        if isinstance(value, cls):
            return value
        if value is None:
            return default
        candidate = str(value).strip().lower().replace("-", "_")
        for mode in cls:
            if candidate == mode.value:
                return mode
        raise ValueError(f"Unknown governance publication mode: {value}")


_ENVIRONMENT_KEY = "DC43_GOVERNANCE_PUBLICATION_MODE"
_CONFIG_KEYS: Sequence[str] = (
    "dc43.governance.publicationMode",
    "dc43.governance.publication_mode",
    "governance.publication.mode",
)


def _lookup_configuration(
    config: Mapping[str, str] | None,
) -> Optional[str]:
    for key in _CONFIG_KEYS:
        if not config:
            break
        value = config.get(key)
        if value:
            return value
    return None


def resolve_publication_mode(
    *,
    explicit: "GovernancePublicationMode | str | None" = None,
    config: Mapping[str, str] | None = None,
    env: Mapping[str, str] | None = None,
    default: GovernancePublicationMode = GovernancePublicationMode.LEGACY,
) -> GovernancePublicationMode:
    """Return the configured governance publication mode.

    Parameters
    ----------
    explicit:
        Optional override supplied by the caller.
    config:
        Mapping sourced from the execution environment (for example Spark
        configuration) that may contain publication mode hints.
    env:
        Environment variables.  ``os.environ`` is consulted when omitted.
    default:
        Publication mode returned when no other hint is provided.
    """

    if explicit is not None:
        return GovernancePublicationMode.from_value(explicit, default=default)

    resolved_env = env or os.environ
    candidates: list[str] = []
    env_value = resolved_env.get(_ENVIRONMENT_KEY)
    if env_value:
        candidates.append(env_value)
    if config:
        value = _lookup_configuration(config)
        if value:
            candidates.append(value)

    for value in candidates:
        try:
            return GovernancePublicationMode.from_value(value, default=default)
        except ValueError:
            continue

    return default


__all__ = [
    "GovernancePublicationMode",
    "resolve_publication_mode",
]

