"""Client utilities for interacting with data-quality services.

Prefer importing :class:`ObservationPayload` from :mod:`dc43.lib` when writing
pure library integrations. The symbol remains available here for backwards
compatibility with existing integrations.
"""

from dc43.lib.data_quality import ObservationPayload

from .client import DataQualityServiceClient, LocalDataQualityServiceClient

__all__ = [
    "DataQualityServiceClient",
    "LocalDataQualityServiceClient",
    "ObservationPayload",
]
