"""HTTP client for interacting with the contract service backend."""

from __future__ import annotations

from typing import Mapping, Optional, Sequence

try:  # pragma: no cover - optional dependency guard
    import httpx
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "httpx is required to use the HTTP contract client. Install "
        "'dc43-service-clients[http]' to enable it."
    ) from exc
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients._http_sync import ensure_response, close_client
from .interface import ContractServiceClient


class RemoteContractServiceClient(ContractServiceClient):
    """Invoke the contract service over HTTP."""

    def __init__(
        self,
        *,
        base_url: str = "http://localhost:8000",
        client: httpx.Client | httpx.AsyncClient | None = None,
        transport: httpx.BaseTransport | None = None,
        headers: Mapping[str, str] | None = None,
        auth: httpx.Auth | tuple[str, str] | None = None,
        token: str | None = None,
        token_header: str = "Authorization",
        token_scheme: str = "Bearer",
    ) -> None:
        self._base_url = base_url.rstrip("/") if base_url else ""
        if client is None:
            header_map = dict(headers or {})
            if token is not None:
                scheme = f"{token_scheme} " if token_scheme else ""
                header_map.setdefault(token_header, f"{scheme}{token}".strip())
            self._client = httpx.Client(
                base_url=self._base_url or None,
                transport=transport,
                headers=header_map or None,
                auth=auth,
            )
            self._owns_client = True
        else:
            self._client = client
            self._owns_client = False
            if headers or auth is not None or token is not None:
                raise ValueError(
                    "Provide custom headers, auth, or tokens when the client is owned by the"
                    " RemoteContractServiceClient."
                )

    def close(self) -> None:
        if self._owns_client:
            close_client(self._client)

    def _request_path(self, path: str) -> str:
        if self._base_url and not path.startswith("http"):
            return f"{self._base_url}{path}"
        return path

    def put(self, contract: OpenDataContractStandard) -> None:
        contract_id = str(contract.id)
        contract_version = str(contract.version)
        if not contract_id:
            raise ValueError("Contract requires an id")
        if not contract_version:
            raise ValueError("Contract requires a version")
        payload = contract.model_dump(by_alias=True, exclude_none=True)
        response = ensure_response(
            self._client.put(
                self._request_path(
                    f"/contracts/{contract_id}/versions/{contract_version}"
                ),
                json=payload,
            )
        )
        response.raise_for_status()

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        response = ensure_response(
            self._client.get(
                self._request_path(f"/contracts/{contract_id}/versions/{contract_version}"),
            )
        )
        response.raise_for_status()
        return OpenDataContractStandard.model_validate(response.json())

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        response = ensure_response(
            self._client.get(self._request_path(f"/contracts/{contract_id}/latest"))
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return OpenDataContractStandard.model_validate(response.json())

    def list_versions(self, contract_id: str) -> Sequence[str]:
        response = ensure_response(
            self._client.get(self._request_path(f"/contracts/{contract_id}/versions"))
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [str(item) for item in payload]
        return []

    def list_contracts(
        self, *, limit: int | None = None, offset: int = 0
    ) -> Mapping[str, object]:
        params: dict[str, object] = {}
        if limit is not None:
            params["limit"] = int(limit)
        if offset:
            params["offset"] = int(offset)
        response = ensure_response(
            self._client.get(
                self._request_path("/contracts"),
                params=params or None,
            )
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping):
            items = payload.get("items")
            payload["items"] = [str(item) for item in items] if isinstance(items, list) else []
            return payload
        if isinstance(payload, list):
            return {
                "items": [str(item) for item in payload],
                "total": len(payload),
                "limit": limit,
                "offset": offset,
            }
        return {"items": [], "total": 0, "limit": limit, "offset": offset}

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        response = ensure_response(
            self._client.post(
                self._request_path("/contracts/link"),
                json={
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                },
            )
        )
        response.raise_for_status()

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        params = {"dataset_version": dataset_version} if dataset_version is not None else None
        response = ensure_response(
            self._client.get(
                self._request_path(f"/contracts/datasets/{dataset_id}/linked"),
                params=params,
            )
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            version = payload.get("contract_version")
            return str(version) if version is not None else None
        return None


__all__ = ["RemoteContractServiceClient"]
