from __future__ import annotations

"""Delta Live Tables helpers from ODCS contracts.

Translate ODCS DataQuality rules to DLT expectations.
"""

from typing import Dict
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from ..dq.spark_metrics import expectations_from_contract as _expectations_from_contract


def expectations_from_contract(contract: OpenDataContractStandard) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate derived from DataQuality rules."""
    return _expectations_from_contract(contract)


def apply_dlt_expectations(dlt_module, expectations: Dict[str, str], *, drop: bool = False) -> None:
    """Apply expectations using a provided `dlt` module inside a pipeline function."""
    if drop:
        dlt_module.expect_all_or_drop(expectations)
    else:
        dlt_module.expect_all(expectations)
