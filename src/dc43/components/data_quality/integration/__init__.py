"""Compatibility shim for Spark data-quality integration helpers.

The runtime-specific hooks now live under :mod:`dc43.integration.data_quality`.
This module re-exports those helpers to keep existing imports operational.
"""

from dc43.integration import data_quality as _integration
from dc43.integration.data_quality import *  # noqa: F401,F403

__all__ = list(_integration.__all__)
