"""HTTP-based implementation of the data-quality client contract."""

from __future__ import annotations

from typing import Mapping, Sequence

import httpx
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients._http_sync import ensure_response, close_client
from dc43_service_clients.data_quality.models import ObservationPayload, ValidationResult
from dc43_service_clients.data_quality.transport import (
    decode_validation_result,
    encode_observation_payload,
)
from .interface import DataQualityServiceClient


class RemoteDataQualityServiceClient(DataQualityServiceClient):
    """Delegate data-quality evaluations to a remote backend over HTTP."""

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
                    " RemoteDataQualityServiceClient."
                )

    def close(self) -> None:
        if self._owns_client:
            close_client(self._client)

    def _request_path(self, path: str) -> str:
        if self._base_url and not path.startswith("http"):
            return f"{self._base_url}{path}"
        return path

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        response = ensure_response(
            self._client.post(
                self._request_path("/data-quality/evaluate"),
                json={
                    "contract": contract.model_dump(by_alias=True, exclude_none=True),
                    "payload": encode_observation_payload(payload),
                },
            )
        )
        response.raise_for_status()
        return decode_validation_result(response.json()) or ValidationResult()

    def describe_expectations(
        self,
        *,
        contract: OpenDataContractStandard,
    ) -> list[dict[str, object]]:
        response = ensure_response(
            self._client.post(
                self._request_path("/data-quality/expectations"),
                json={"contract": contract.model_dump(by_alias=True, exclude_none=True)},
            )
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Sequence):
            return [dict(item) if isinstance(item, Mapping) else {"value": item} for item in payload]
        return []


__all__ = ["RemoteDataQualityServiceClient"]
