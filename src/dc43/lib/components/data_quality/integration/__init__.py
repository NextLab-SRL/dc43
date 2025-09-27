"""Compatibility shim for Spark data-quality integration helpers.

The runtime-specific hooks now live under
:mod:`dc43.services.data_quality.backend.integration`.  This module re-exports
those helpers to keep existing imports operational.
"""

from dc43.services.data_quality.backend import integration as _integration
from dc43.services.data_quality.backend.integration import *  # noqa: F401,F403

__all__ = list(_integration.__all__)
