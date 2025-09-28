from __future__ import annotations

"""Delta Live Tables helpers."""

from typing import Mapping


def apply_dlt_expectations(
    dlt_module,
    expectations: Mapping[str, str],
    *,
    drop: bool = False,
) -> None:
    """Apply expectations using a provided `dlt` module inside a pipeline function."""
    if drop:
        dlt_module.expect_all_or_drop(dict(expectations))
    else:
        dlt_module.expect_all(dict(expectations))
