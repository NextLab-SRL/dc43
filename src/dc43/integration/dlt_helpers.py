from __future__ import annotations

"""Delta Live Tables helpers from ODCS contracts.

Translate ODCS constraints to DLT expectations so that contracts can be
enforced in streaming/batch pipelines with minimal code.
"""

from typing import Dict
from ..odcs import ODCSLike

from .validation import _build_expectations  # internal, but convenient here
from ..odcs import list_properties


def expectations_from_contract(contract: ODCSLike) -> Dict[str, str]:
    """Return a flat dict of expectation_name -> SQL predicate.

    Can be passed to DLT `expect_all` / `expect_all_or_drop`.
    """
    exps: Dict[str, str] = {}
    for f in list_properties(contract):
        exps.update(_build_expectations(f))
    # Only constraints-derived expectations are emitted (no extra contract fields assumed)
    return exps


def apply_dlt_expectations(dlt_module, expectations: Dict[str, str], *, drop: bool = False) -> None:
    """Apply expectations using a provided `dlt` module inside a pipeline function.

    Example usage:
        import dlt
        exps = expectations_from_contract(contract)
        apply_dlt_expectations(dlt, exps, drop=True)
    """
    if drop:
        dlt_module.expect_all_or_drop(expectations)
    else:
        dlt_module.expect_all(expectations)
