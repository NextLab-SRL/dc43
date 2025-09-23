from __future__ import annotations

from typing import List, Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from ..interface import ContractStore
from dc43.components.integration.collibra import CollibraContractGateway, _semver_key
from dc43.odcs import ensure_version, to_model


class CollibraContractStore(ContractStore):
    """Expose Collibra-managed contracts through the :class:`ContractStore` API."""

    def __init__(
        self,
        gateway: CollibraContractGateway,
        *,
        default_status: str = "Draft",
        status_filter: Optional[str] = None,
    ) -> None:
        self._gateway = gateway
        self._default_status = default_status
        self._status_filter = status_filter

    def put(self, contract: OpenDataContractStandard) -> None:
        ensure_version(contract)
        self._gateway.upsert_contract(contract, status=self._default_status)

    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        payload = self._gateway.get_contract(contract_id, version)
        return to_model(payload)

    def list_contracts(self) -> List[str]:
        return self._gateway.list_contracts()

    def list_versions(self, contract_id: str) -> List[str]:
        summaries = self._gateway.list_versions(contract_id)
        if self._status_filter:
            summaries = [s for s in summaries if s.status == self._status_filter]
        return [s.version for s in summaries]

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        versions = self.list_versions(contract_id)
        if not versions:
            return None
        versions.sort(key=_semver_key)
        return self.get(contract_id, versions[-1])

    def latest_validated(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        """Return the latest contract marked as ``Validated`` if available."""

        try:
            payload = self._gateway.get_validated_contract(contract_id)
        except LookupError:
            return None
        return to_model(payload)


__all__ = ["CollibraContractStore"]
