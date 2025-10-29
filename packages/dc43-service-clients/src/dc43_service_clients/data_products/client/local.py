"""Local adapter delegating data product requests to backend implementations."""

from __future__ import annotations

from typing import Mapping, Optional, TYPE_CHECKING

from dc43_service_clients.odps import OpenDataProductStandard

from .interface import DataProductServiceClient
from .._compat import DataProductRegistrationResult

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from dc43_service_backends.data_products import (
        DataProductServiceBackend,
        LocalDataProductServiceBackend,
    )
else:  # pragma: no cover - help type checkers when optional deps missing
    DataProductServiceBackend = LocalDataProductServiceBackend = object  # type: ignore


class LocalDataProductServiceClient(DataProductServiceClient):
    """Adapter that fulfils the client protocol against a backend instance."""

    def __init__(self, backend: "DataProductServiceBackend | None" = None) -> None:
        if backend is None:
            try:
                from dc43_service_backends.data_products import (  # pylint: disable=import-outside-toplevel
                    LocalDataProductServiceBackend as _LocalDataProductServiceBackend,
                )
            except ModuleNotFoundError:  # pragma: no cover - exercised when backends absent
                from dc43_service_clients.testing import (  # pylint: disable=import-outside-toplevel
                    LocalDataProductServiceBackend as _LocalDataProductServiceBackend,
                )

            backend = _LocalDataProductServiceBackend()
        self._backend = backend

    def put(self, product: OpenDataProductStandard) -> None:
        self._backend.put(product)

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        return self._backend.get(data_product_id, version)

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:
        return self._backend.latest(data_product_id)

    def list_versions(self, data_product_id: str) -> list[str]:
        return list(self._backend.list_versions(data_product_id))

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> Mapping[str, object]:
        listing = self._backend.list_data_products(limit=limit, offset=offset)
        return {
            "items": [str(item) for item in listing.items],
            "total": int(listing.total),
            "limit": listing.limit,
            "offset": listing.offset,
        }

    def register_input_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
        source_data_product: Optional[str] = None,
        source_output_port: Optional[str] = None,
    ) -> DataProductRegistrationResult:
        from dc43_service_clients.odps import DataProductInputPort

        port = DataProductInputPort(
            name=port_name,
            version=contract_version,
            contract_id=contract_id,
        )
        return self._backend.register_input_port(
            data_product_id=data_product_id,
            port=port,
            bump=bump,
            custom_properties=custom_properties,
            source_data_product=source_data_product,
            source_output_port=source_output_port,
        )

    def register_output_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
    ) -> DataProductRegistrationResult:
        from dc43_service_clients.odps import DataProductOutputPort

        port = DataProductOutputPort(
            name=port_name,
            version=contract_version,
            contract_id=contract_id,
        )
        return self._backend.register_output_port(
            data_product_id=data_product_id,
            port=port,
            bump=bump,
            custom_properties=custom_properties,
        )

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        return self._backend.resolve_output_contract(
            data_product_id=data_product_id,
            port_name=port_name,
        )


__all__ = ["LocalDataProductServiceClient"]

