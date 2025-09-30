"""HTTP implementation of the governance client contract."""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Sequence

try:  # pragma: no cover - optional dependency guard
    import httpx
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "httpx is required to use the HTTP governance client. Install "
        "'dc43-service-clients[http]' to enable it."
    ) from exc
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients._http_sync import ensure_response, close_client
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_quality.transport import (
    encode_observation_payload,
    encode_validation_result,
    decode_validation_result,
)
from dc43_service_clients.governance.models import (
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
)
from dc43_service_clients.governance.transport import (
    decode_contract,
    decode_quality_assessment,
    encode_credentials,
    encode_draft_context,
    encode_pipeline_context,
)
from .interface import GovernanceServiceClient


class RemoteGovernanceServiceClient(GovernanceServiceClient):
    """Interact with the governance backend via HTTP requests."""

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
                    " RemoteGovernanceServiceClient."
                )

    def close(self) -> None:
        if self._owns_client:
            close_client(self._client)

    def _request_path(self, path: str) -> str:
        if self._base_url and not path.startswith("http"):
            return f"{self._base_url}{path}"
        return path

    def configure_auth(
        self,
        credentials: GovernanceCredentials | Mapping[str, object] | str | None,
    ) -> None:
        payload: Mapping[str, object] | None
        if isinstance(credentials, GovernanceCredentials):
            payload = encode_credentials(credentials)
        elif isinstance(credentials, Mapping):
            payload = dict(credentials)
        elif isinstance(credentials, str):
            payload = {"token": credentials}
        else:
            payload = None
        response = ensure_response(
            self._client.post(
                self._request_path("/governance/auth"),
                json={"credentials": payload},
            )
        )
        response.raise_for_status()

    def evaluate_dataset(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        operation: str = "read",
        draft_on_violation: bool = False,
    ) -> QualityAssessment:
        payload = observations()
        response = ensure_response(
            self._client.post(
                self._request_path("/governance/evaluate"),
                json={
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "validation": encode_validation_result(validation),
                    "observations": encode_observation_payload(payload),
                    "bump": bump,
                    "context": encode_draft_context(context),
                    "pipeline_context": encode_pipeline_context(pipeline_context),
                    "operation": operation,
                    "draft_on_violation": draft_on_violation,
                },
            )
        )
        response.raise_for_status()
        return decode_quality_assessment(response.json())

    def review_validation_outcome(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        data_format: str | None = None,
        dq_status: ValidationResult | None = None,
        dq_feedback: Mapping[str, object] | None = None,
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        draft_requested: bool = False,
        operation: str | None = None,
    ) -> Optional[OpenDataContractStandard]:
        response = ensure_response(
            self._client.post(
                self._request_path("/governance/review"),
                json={
                    "validation": encode_validation_result(validation),
                    "base_contract": base_contract.model_dump(by_alias=True, exclude_none=True),
                    "bump": bump,
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "data_format": data_format,
                    "dq_status": encode_validation_result(dq_status),
                    "dq_feedback": dict(dq_feedback) if isinstance(dq_feedback, Mapping) else dq_feedback,
                    "context": encode_draft_context(context),
                    "pipeline_context": encode_pipeline_context(pipeline_context),
                    "draft_requested": draft_requested,
                    "operation": operation,
                },
            )
        )
        response.raise_for_status()
        return decode_contract(response.json())

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        response = ensure_response(
            self._client.post(
                self._request_path("/governance/draft"),
                json={
                    "validation": encode_validation_result(validation),
                    "base_contract": base_contract.model_dump(by_alias=True, exclude_none=True),
                    "bump": bump,
                    "context": encode_draft_context(context),
                    "pipeline_context": encode_pipeline_context(pipeline_context),
                },
            )
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("Invalid draft response from governance service")
        return OpenDataContractStandard.model_validate(dict(payload))

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[ValidationResult]:
        response = ensure_response(
            self._client.get(
                self._request_path("/governance/status"),
                params={
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                },
            )
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return decode_validation_result(response.json())

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
                self._request_path("/governance/link"),
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
        response = ensure_response(
            self._client.get(
                self._request_path("/governance/linked"),
                params={
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                }
                if dataset_version is not None
                else {"dataset_id": dataset_id},
            )
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping):
            version = payload.get("contract_version")
            return str(version) if version is not None else None
        return None

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        response = ensure_response(
            self._client.get(
                self._request_path("/governance/activity"),
                params={
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                }
                if dataset_version is not None
                else {"dataset_id": dataset_id},
            )
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [dict(item) if isinstance(item, Mapping) else {"value": item} for item in payload]
        return []


__all__ = ["RemoteGovernanceServiceClient"]
