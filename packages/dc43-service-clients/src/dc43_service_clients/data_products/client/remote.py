"""HTTP client for interacting with the data product service."""

from __future__ import annotations

from typing import Mapping, Optional

try:  # pragma: no cover - optional dependency guard
    import httpx
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "httpx is required to use the HTTP data product client. Install "
        "'dc43-service-clients[http]' to enable it."
    ) from exc

from dc43_service_clients.odps import OpenDataProductStandard, to_model
from dc43_service_clients._http_sync import close_client, ensure_response

from .interface import DataProductServiceClient
from .._compat import DataProductRegistrationResult


class RemoteDataProductServiceClient(DataProductServiceClient):
    """Invoke the data product service over HTTP."""

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
                    "Provide custom headers, auth, or tokens when the client is owned "
                    "by the RemoteDataProductServiceClient."
                )

    def close(self) -> None:
        if self._owns_client:
            close_client(self._client)

    def _request_path(self, path: str) -> str:
        if self._base_url and not path.startswith("http"):
            return f"{self._base_url}{path}"
        return path

    def _decode_product(self, payload: Mapping[str, object]) -> OpenDataProductStandard:
        return to_model(payload)

    def _decode_registration(
        self, payload: Mapping[str, object]
    ) -> DataProductRegistrationResult:
        product_payload = payload.get("product") if isinstance(payload, Mapping) else None
        if not isinstance(product_payload, Mapping):
            raise ValueError("Malformed registration payload: missing product")
        product = self._decode_product(product_payload)
        changed = bool(payload.get("changed")) if isinstance(payload, Mapping) else False
        return DataProductRegistrationResult(product=product, changed=changed)

    def put(self, product: OpenDataProductStandard) -> None:
        product_id = str(product.id)
        version = str(product.version) if product.version is not None else ""
        if not product_id:
            raise ValueError("Data product requires an id")
        if not version:
            raise ValueError("Data product requires a version")
        payload = product.to_dict()
        response = ensure_response(
            self._client.put(
                self._request_path(f"/data-products/{product_id}/versions/{version}"),
                json=payload,
            )
        )
        response.raise_for_status()

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        response = ensure_response(
            self._client.get(
                self._request_path(f"/data-products/{data_product_id}/versions/{version}"),
            )
        )
        response.raise_for_status()
        return self._decode_product(response.json())

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:
        response = ensure_response(
            self._client.get(
                self._request_path(f"/data-products/{data_product_id}/latest"),
            )
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return self._decode_product(response.json())

    def list_versions(self, data_product_id: str) -> list[str]:
        response = ensure_response(
            self._client.get(
                self._request_path(f"/data-products/{data_product_id}/versions"),
            )
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [str(item) for item in payload]
        return []

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> Mapping[str, object]:
        params: dict[str, object] = {}
        if limit is not None:
            params["limit"] = int(limit)
        if offset:
            params["offset"] = int(offset)
        response = ensure_response(
            self._client.get(
                self._request_path("/data-products"),
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
        response = ensure_response(
            self._client.post(
                self._request_path(f"/data-products/{data_product_id}/input-ports"),
                json={
                    "port_name": port_name,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "bump": bump,
                    "custom_properties": custom_properties,
                    "source_data_product": source_data_product,
                    "source_output_port": source_output_port,
                },
            )
        )
        response.raise_for_status()
        return self._decode_registration(response.json())

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
        response = ensure_response(
            self._client.post(
                self._request_path(f"/data-products/{data_product_id}/output-ports"),
                json={
                    "port_name": port_name,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "bump": bump,
                    "custom_properties": custom_properties,
                },
            )
        )
        response.raise_for_status()
        return self._decode_registration(response.json())

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        response = ensure_response(
            self._client.get(
                self._request_path(
                    f"/data-products/{data_product_id}/output-ports/{port_name}/contract"
                )
            )
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping):
            cid = payload.get("contract_id")
            cver = payload.get("contract_version")
            if cid is not None and cver is not None:
                return str(cid), str(cver)
        return None


__all__ = ["RemoteDataProductServiceClient"]

