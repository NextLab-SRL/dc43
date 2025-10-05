"""Core helpers shared across dc43 packages."""

from __future__ import annotations

from .odcs import (
    BITOL_SCHEMA_URL,
    ODCS_REQUIRED,
    as_odcs_dict,
    build_odcs,
    contract_identity,
    custom_properties_dict,
    ensure_version,
    field_map,
    fingerprint,
    list_properties,
    normalise_custom_properties,
    odcs_package_version,
    to_model,
)
from .odps import (
    ODPS_REQUIRED,
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard as OpenDataProduct,
    as_odps_dict as as_odps_product_dict,
    evolve_to_draft as evolve_odps_to_draft,
    next_draft_version as next_odps_draft_version,
    to_model as to_odps_model,
)
from .versioning import SemVer

__all__ = [
    "BITOL_SCHEMA_URL",
    "ODCS_REQUIRED",
    "ODPS_REQUIRED",
    "SemVer",
    "as_odcs_dict",
    "build_odcs",
    "contract_identity",
    "custom_properties_dict",
    "ensure_version",
    "field_map",
    "fingerprint",
    "list_properties",
    "normalise_custom_properties",
    "odcs_package_version",
    "to_model",
    "DataProductInputPort",
    "DataProductOutputPort",
    "OpenDataProduct",
    "as_odps_product_dict",
    "evolve_odps_to_draft",
    "next_odps_draft_version",
    "to_odps_model",
]
