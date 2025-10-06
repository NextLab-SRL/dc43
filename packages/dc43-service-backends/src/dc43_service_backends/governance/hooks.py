"""Extension hooks invoked during governance operations."""

from __future__ import annotations

from typing import Protocol


class DatasetContractLinkHook(Protocol):
    """Hook invoked after a dataset is linked to a contract."""

    def __call__(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        ...


__all__ = ["DatasetContractLinkHook"]
