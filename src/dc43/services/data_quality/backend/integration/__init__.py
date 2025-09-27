"""Compatibility exports for engine integrations inside the service backend."""

from dc43.integration.data_quality import *  # noqa: F401,F403

from dc43.integration import data_quality as _integration

__all__ = list(_integration.__all__)
