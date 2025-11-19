"""HTTP-based governance store proxying to remote observability services.""" 

from __future__ import annotations

from typing import Mapping, Optional, Sequence

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from ._metrics import extract_metrics
from .interface import GovernanceStore

try:  # pragma: no cover - optional dependency guard
    import httpx
except ModuleNotFoundError as exc:  # pragma: no cover - surfaced via builder
    raise ModuleNotFoundError(
        "httpx is required when using the HttpGovernanceStore. Install 'dc43-service-backends[http]' or provide httpx manually.",
    ) from exc


class HttpGovernanceStore(GovernanceStore):
    """Delegate governance persistence to a remote HTTP API."""

    def __init__(
        self,
        base_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        token: str | None = None,
        token_header: str = "Authorization",
        token_scheme: str = "Bearer",
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        auth_headers = dict(headers or {})
        if token:
            auth_headers[token_header] = f"{token_scheme} {token}".strip()
        self._client = httpx.Client(base_url=self._base_url, headers=auth_headers, timeout=timeout)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _status_url(self, dataset_id: str, dataset_version: str) -> str:
        return f"/datasets/{dataset_id}/versions/{dataset_version}/status"

    def _activity_url(self, dataset_id: str, dataset_version: Optional[str] = None) -> str:
        if dataset_version is None:
            return f"/datasets/{dataset_id}/activity"
        return f"/datasets/{dataset_id}/versions/{dataset_version}/activity"

    def _link_url(self, dataset_id: str, dataset_version: str) -> str:
        return f"/datasets/{dataset_id}/versions/{dataset_version}/link"

    def _metrics_url(self, dataset_id: str) -> str:
        return f"/datasets/{dataset_id}/metrics"

    def _datasets_url(self) -> str:
        return "/datasets"

    # ------------------------------------------------------------------
    # Status persistence
    # ------------------------------------------------------------------
    def save_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        status: ValidationResult | None,
    ) -> None:
        payload = None
        if status is not None:
            payload = {
                "contract_id": contract_id,
                "contract_version": contract_version,
                "status": status.status,
                "reason": status.reason,
                "details": status.details,
            }
            metrics_map = extract_metrics(status)
            if metrics_map:
                payload["metrics"] = metrics_map
        response = self._client.request(
            "DELETE" if status is None else "PUT",
            self._status_url(dataset_id, dataset_version),
            json=payload,
        )
        response.raise_for_status()

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        response = self._client.get(self._status_url(dataset_id, dataset_version))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        linked_id = data.get("contract_id")
        linked_version = data.get("contract_version")
        if linked_id and linked_version and (linked_id, linked_version) != (contract_id, contract_version):
            reason = f"dataset linked to contract {linked_id}:{linked_version}"
            return ValidationResult(status="block", reason=reason, details=data)
        return ValidationResult(
            status=str(data.get("status", "unknown")),
            reason=data.get("reason"),
            details=coerce_details(data.get("details")),
        )

    # ------------------------------------------------------------------
    # Dataset links
    # ------------------------------------------------------------------
    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        response = self._client.put(
            self._link_url(dataset_id, dataset_version),
            json={
                "contract_id": contract_id,
                "contract_version": contract_version,
            },
        )
        response.raise_for_status()

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        response = self._client.get(
            self._link_url(dataset_id, dataset_version or "latest")
            if dataset_version is not None
            else f"/datasets/{dataset_id}/link"
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        cid = data.get("contract_id")
        cver = data.get("contract_version")
        if cid and cver:
            return f"{cid}:{cver}"
        return None

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        params: dict[str, str] = {}
        if dataset_version is not None:
            params["dataset_version"] = dataset_version
        if contract_id is not None:
            params["contract_id"] = contract_id
        if contract_version is not None:
            params["contract_version"] = contract_version
        response = self._client.get(self._metrics_url(dataset_id), params=params or None)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [dict(item) for item in data if isinstance(item, Mapping)]
        if isinstance(data, Mapping):
            return [dict(data)]
        return []

    def list_datasets(self) -> Sequence[str]:
        response = self._client.get(self._datasets_url())
        if response.status_code == 404:
            return []
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [str(item) for item in payload]
        if isinstance(payload, Mapping):
            items = payload.get("datasets")
            if isinstance(items, list):
                return [str(item) for item in items]
        return []

    # ------------------------------------------------------------------
    # Pipeline activity
    # ------------------------------------------------------------------
    def record_pipeline_event(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        event: Mapping[str, object],
        lineage_event: Mapping[str, object] | None = None,
    ) -> None:
        payload = dict(event)
        payload.setdefault("contract_id", contract_id)
        payload.setdefault("contract_version", contract_version)
        if lineage_event is not None:
            payload["lineage_event"] = dict(lineage_event)
        response = self._client.post(self._activity_url(dataset_id, dataset_version), json=payload)
        response.raise_for_status()

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        url = (
            self._activity_url(dataset_id, dataset_version)
            if dataset_version is not None
            else self._activity_url(dataset_id)
        )
        params = {"version": dataset_version} if dataset_version is None else None
        response = self._client.get(url, params=params)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [item for item in data if isinstance(item, Mapping)]
        if isinstance(data, Mapping):
            return [data]
        return []


__all__ = ["HttpGovernanceStore"]
