"""In-process governance backend coordinating contract and quality services."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.core.odcs import contract_identity
from dc43_service_backends.contracts import ContractServiceBackend, ContractStore
from dc43_service_backends.contracts.drafting import draft_from_validation_result
from dc43_service_backends.data_quality import DataQualityServiceBackend
from dc43_service_backends.data_products import (
    DataProductRegistrationResult,
    DataProductServiceBackend,
)
from dc43_service_backends.core.versioning import version_key
from dc43_service_clients.contracts import ContractServiceClient
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    ObservationPayload,
    ValidationResult,
)
from dc43_service_clients.data_products import DataProductServiceClient
from dc43_service_clients.odps import DataProductInputPort, DataProductOutputPort

from .interface import GovernanceServiceBackend
from ..storage import GovernanceStore, InMemoryGovernanceStore
from ..hooks import DatasetContractLinkHook
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
    build_quality_context,
    derive_feedback,
    merge_pipeline_context,
)


class LocalGovernanceServiceBackend(GovernanceServiceBackend):
    """In-process orchestration across contract and data-quality services."""

    def __init__(
        self,
        *,
        contract_client: ContractServiceBackend | ContractServiceClient,
        dq_client: DataQualityServiceBackend | DataQualityServiceClient,
        data_product_client: DataProductServiceBackend | DataProductServiceClient | None = None,
        draft_store: ContractStore | None = None,
        link_hooks: Sequence[DatasetContractLinkHook] | None = None,
        store: GovernanceStore | None = None,
    ) -> None:
        self._contract_client = contract_client
        self._dq_client = dq_client
        self._data_product_client = data_product_client
        self._draft_store = draft_store
        self._credentials: Optional[GovernanceCredentials] = None
        self._link_hooks: tuple[DatasetContractLinkHook, ...] = (
            tuple(link_hooks) if link_hooks else ()
        )
        self._store: GovernanceStore = store or InMemoryGovernanceStore()

    # ------------------------------------------------------------------
    # Contract helpers
    # ------------------------------------------------------------------
    def get_contract(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> OpenDataContractStandard:
        return self._contract_client.get(contract_id, contract_version)

    def latest_contract(
        self,
        *,
        contract_id: str,
    ) -> Optional[OpenDataContractStandard]:
        resolver = getattr(self._contract_client, "latest", None)
        if callable(resolver):
            return resolver(contract_id)
        return None

    def list_contract_versions(self, *, contract_id: str) -> Sequence[str]:
        resolver = getattr(self._contract_client, "list_versions", None)
        if callable(resolver):
            return tuple(resolver(contract_id))
        return ()

    def describe_expectations(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> Sequence[Mapping[str, object]]:
        contract = self.get_contract(
            contract_id=contract_id,
            contract_version=contract_version,
        )
        resolver = getattr(self._dq_client, "describe_expectations", None)
        if callable(resolver):
            return tuple(resolver(contract=contract))
        return ()

    # ------------------------------------------------------------------
    # Authentication lifecycle
    # ------------------------------------------------------------------
    def configure_auth(
        self,
        credentials: GovernanceCredentials | Mapping[str, object] | str | None,
    ) -> None:
        if credentials is None:
            self._credentials = None
            return
        if isinstance(credentials, GovernanceCredentials):
            self._credentials = credentials
            return
        if isinstance(credentials, str):
            self._credentials = GovernanceCredentials(token=credentials)
            return
        token = str(credentials.get("token")) if "token" in credentials else None
        headers = credentials.get("headers") if isinstance(credentials.get("headers"), Mapping) else None
        extra = {
            key: value
            for key, value in credentials.items()
            if key not in {"token", "headers"}
        }
        self._credentials = GovernanceCredentials(
            token=token,
            headers=headers,  # type: ignore[arg-type]
            extra=extra or None,
        )

    @property
    def credentials(self) -> Optional[GovernanceCredentials]:
        return self._credentials

    # ------------------------------------------------------------------
    # Governance orchestration
    # ------------------------------------------------------------------
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
        contract = self._contract_client.get(contract_id, contract_version)

        payload = observations()
        validation = validation or self._dq_client.evaluate(
            contract=contract,
            payload=payload,
        )
        status = self._status_from_validation(validation, operation=operation)

        if status is not None:
            details = dict(status.details)
            if payload.metrics:
                if not details.get("metrics"):
                    details["metrics"] = payload.metrics
                status.metrics = dict(payload.metrics)
            if payload.schema:
                if not details.get("schema"):
                    details["schema"] = payload.schema
                status.schema = dict(payload.schema)
            status.details = details

        self._store.save_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            status=status,
        )

        effective_pipeline = merge_pipeline_context(
            context.pipeline_context if context else None,
            pipeline_context,
            {"io": operation},
        )

        draft: Optional[OpenDataContractStandard] = None
        if draft_on_violation and status and status.status in {"warn", "block"}:
            draft = self.review_validation_outcome(
                validation=validation,
                base_contract=contract,
                bump=bump,
                dataset_id=context.dataset_id if context else dataset_id,
                dataset_version=context.dataset_version if context else dataset_version,
                data_format=context.data_format if context else None,
                dq_status=status,
                dq_feedback=context.dq_feedback if context else None,
                context=context,
                pipeline_context=effective_pipeline,
                draft_requested=True,
                operation=operation,
            )
            if draft is not None and status is not None:
                details = dict(status.details)
                details.setdefault("draft_contract_version", draft.version)
                status.details = details

        self._record_pipeline_activity(
            contract=contract,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            operation=operation,
            pipeline_context=effective_pipeline,
            status=status,
            observations_reused=payload.reused,
        )

        return QualityAssessment(
            status=status,
            validation=validation,
            draft=draft,
            observations_reused=payload.reused,
        )

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
        if not draft_requested:
            return None

        effective_context = build_quality_context(
            context,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=derive_feedback(dq_status, dq_feedback),
            pipeline_context=merge_pipeline_context(
                context.pipeline_context if context else None,
                pipeline_context,
                {"io": operation} if operation else None,
            ),
        )

        draft = self.propose_draft(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            context=effective_context,
            pipeline_context=effective_context.pipeline_context,
        )

        if draft is not None and self._draft_store is not None:
            self._draft_store.put(draft)

        return draft

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        effective_context = build_quality_context(
            context,
            dataset_id=context.dataset_id if context else None,
            dataset_version=context.dataset_version if context else None,
            data_format=context.data_format if context else None,
            dq_feedback=context.dq_feedback if context else None,
            pipeline_context=pipeline_context,
        )

        draft = draft_from_validation_result(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=effective_context.dataset_id,
            dataset_version=effective_context.dataset_version,
            data_format=effective_context.data_format,
            dq_feedback=effective_context.dq_feedback,
            draft_context=effective_context.draft_context,
        )
        if draft is not None and self._draft_store is not None:
            self._draft_store.put(draft)
        return draft

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[ValidationResult]:
        return self._store.load_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        linker = getattr(self._contract_client, "link_dataset_contract", None)
        if callable(linker):
            linker(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                contract_id=contract_id,
                contract_version=contract_version,
            )
        link_value = f"{contract_id}:{contract_version}"
        self._store.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )
        for hook in self._link_hooks:
            hook(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                contract_id=contract_id,
                contract_version=contract_version,
            )

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        resolver = getattr(self._contract_client, "get_linked_contract_version", None)
        if callable(resolver):
            resolved = resolver(dataset_id=dataset_id, dataset_version=dataset_version)
            if resolved is not None:
                return resolved
        return self._store.get_linked_contract_version(
            dataset_id=dataset_id, dataset_version=dataset_version
        )

    def get_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        return self._store.load_metrics(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )

    def list_datasets(self) -> Sequence[str]:
        return self._store.list_datasets()

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]:
        return self._store.load_pipeline_activity(
            dataset_id=dataset_id, dataset_version=dataset_version
        )

    def resolve_read_context(
        self,
        *,
        context: GovernanceReadContext,
    ) -> ResolvedReadPlan:
        contract, contract_id, contract_version = self._resolve_contract_spec(
            contract_reference=context.contract,
            input_binding=context.input_binding,
            output_binding=None,
        )
        dataset_id = context.dataset_id or contract.id or contract_id
        dataset_version = context.dataset_version or contract.version or contract_version
        pipeline = merge_pipeline_context(context.pipeline_context)
        return ResolvedReadPlan(
            contract=contract,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            dataset_format=context.dataset_format,
            input_binding=context.input_binding,
            pipeline_context=pipeline,
            bump=context.bump,
            draft_on_violation=context.draft_on_violation,
        )

    def resolve_write_context(
        self,
        *,
        context: GovernanceWriteContext,
    ) -> ResolvedWritePlan:
        contract, contract_id, contract_version = self._resolve_contract_spec(
            contract_reference=context.contract,
            input_binding=None,
            output_binding=context.output_binding,
        )
        dataset_id = context.dataset_id or contract.id or contract_id
        dataset_version = context.dataset_version or contract.version or contract_version
        pipeline = merge_pipeline_context(context.pipeline_context)
        return ResolvedWritePlan(
            contract=contract,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            dataset_format=context.dataset_format,
            output_binding=context.output_binding,
            pipeline_context=pipeline,
            bump=context.bump,
            draft_on_violation=context.draft_on_violation,
        )

    def evaluate_read_plan(
        self,
        *,
        plan: ResolvedReadPlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            bump=plan.bump,
            pipeline_context=plan.pipeline_context,
            operation="read",
            draft_on_violation=plan.draft_on_violation,
        )

    def evaluate_write_plan(
        self,
        *,
        plan: ResolvedWritePlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            bump=plan.bump,
            pipeline_context=plan.pipeline_context,
            operation="write",
            draft_on_violation=plan.draft_on_violation,
        )

    def register_read_activity(
        self,
        *,
        plan: ResolvedReadPlan,
        assessment: QualityAssessment,
    ) -> None:
        self._register_input_binding(plan=plan)

    def register_write_activity(
        self,
        *,
        plan: ResolvedWritePlan,
        assessment: QualityAssessment,
    ) -> None:
        self.link_dataset_contract(
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
        )
        self._register_output_binding(plan=plan)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_contract_spec(
        self,
        *,
        contract_reference: ContractReference | None,
        input_binding: Any,
        output_binding: Any,
    ) -> Tuple[OpenDataContractStandard, str, str]:
        if contract_reference is not None:
            return self._resolve_contract_reference(contract_reference)
        if input_binding is not None:
            contract_id, contract_version = self._resolve_contract_from_input(input_binding)
            contract = self._contract_client.get(contract_id, contract_version)
            return contract, contract_id, contract.version or contract_version
        if output_binding is not None:
            contract_id, contract_version = self._resolve_contract_from_output(output_binding)
            contract = self._contract_client.get(contract_id, contract_version)
            return contract, contract_id, contract.version or contract_version
        raise ValueError("A contract reference or data-product binding is required")

    def _resolve_contract_reference(
        self, reference: ContractReference
    ) -> Tuple[OpenDataContractStandard, str, str]:
        version = reference.resolved_version()
        if version:
            contract = self._contract_client.get(reference.contract_id, version)
            return contract, contract.id or reference.contract_id, contract.version or version
        latest = self.latest_contract(contract_id=reference.contract_id)
        if latest is None:
            raise ValueError(f"No contract found for id '{reference.contract_id}'")
        return latest, latest.id or reference.contract_id, latest.version

    def _resolve_contract_from_input(self, binding: Any) -> Tuple[str, str]:
        client = self._require_data_product_client()
        port_name = getattr(binding, "port_name", None) or getattr(
            binding, "source_output_port", None
        )
        if not port_name:
            raise ValueError("Input binding requires a port_name or source_output_port")
        product = self._data_product_latest(binding.data_product)
        if product is not None:
            finder = getattr(product, "find_input_port", None)
            if callable(finder):
                port = finder(port_name)
                if port and getattr(port, "contract_id", None) and getattr(port, "version", None):
                    return port.contract_id, port.version
        resolver = getattr(client, "resolve_output_contract", None)
        if callable(resolver) and getattr(binding, "source_data_product", None):
            resolved = resolver(
                data_product_id=binding.source_data_product,
                port_name=getattr(binding, "source_output_port", port_name),
            )
            if resolved:
                return resolved
        if product is not None:
            finder = getattr(product, "find_input_port", None)
            if callable(finder):
                port = finder(port_name)
                if port and getattr(port, "contract_id", None) and getattr(port, "version", None):
                    return port.contract_id, port.version
        raise ValueError("Unable to resolve contract from input binding")

    def _resolve_contract_from_output(self, binding: Any) -> Tuple[str, str]:
        client = self._require_data_product_client()
        port_name = getattr(binding, "port_name", None) or "default"
        resolver = getattr(client, "resolve_output_contract", None)
        if callable(resolver):
            resolved = resolver(
                data_product_id=binding.data_product,
                port_name=port_name,
            )
            if resolved:
                return resolved
        product = self._data_product_latest(binding.data_product)
        if product is not None:
            finder = getattr(product, "find_output_port", None)
            if callable(finder):
                port = finder(port_name)
                if port and getattr(port, "contract_id", None) and getattr(port, "version", None):
                    return port.contract_id, port.version
        raise ValueError("Unable to resolve contract from output binding")

    def _require_data_product_client(self) -> object:
        if self._data_product_client is None:
            raise RuntimeError("Data product service is not configured")
        return self._data_product_client

    def _data_product_latest(self, data_product_id: str) -> Any:
        client = self._data_product_client
        if client is None:
            return None
        latest = getattr(client, "latest", None)
        if callable(latest):
            return latest(data_product_id)
        return None

    def _register_input_binding(self, *, plan: ResolvedReadPlan) -> None:
        binding = plan.input_binding
        client = self._data_product_client
        if binding is None or client is None:
            return
        port_name = binding.port_name or binding.source_output_port or "default"
        register = getattr(client, "register_input_port", None)
        if not callable(register):
            return
        port = DataProductInputPort(
            name=port_name,
            version=plan.contract_version,
            contract_id=plan.contract_id,
        )
        try:
            registration: Optional[DataProductRegistrationResult] = register(
                data_product_id=binding.data_product,
                port=port,
                bump=binding.bump,
                custom_properties=binding.custom_properties,
                source_data_product=binding.source_data_product,
                source_output_port=binding.source_output_port,
            )
        except TypeError:
            registration = register(
                data_product_id=binding.data_product,
                port_name=port_name,
                contract_id=plan.contract_id,
                contract_version=plan.contract_version,
                bump=binding.bump,
                custom_properties=binding.custom_properties,
                source_data_product=binding.source_data_product,
                source_output_port=binding.source_output_port,
            )
        self._raise_if_registration_requires_review(
            registration,
            data_product=binding.data_product,
            port_name=port_name,
            binding_type="input",
        )

        product = getattr(registration, "product", None) if registration else None
        self._enforce_input_product_constraints(
            binding=binding,
            port_name=port_name,
            product=product,
        )

    def _register_output_binding(self, *, plan: ResolvedWritePlan) -> None:
        binding = plan.output_binding
        client = self._data_product_client
        if binding is None or client is None:
            return
        port_name = binding.port_name or "default"
        register = getattr(client, "register_output_port", None)
        if not callable(register):
            return
        port = DataProductOutputPort(
            name=port_name,
            version=plan.contract_version,
            contract_id=plan.contract_id,
        )
        try:
            registration: Optional[DataProductRegistrationResult] = register(
                data_product_id=binding.data_product,
                port=port,
                bump=binding.bump,
                custom_properties=binding.custom_properties,
            )
        except TypeError:
            registration = register(
                data_product_id=binding.data_product,
                port_name=port_name,
                contract_id=plan.contract_id,
                contract_version=plan.contract_version,
                bump=binding.bump,
                custom_properties=binding.custom_properties,
            )
        self._raise_if_registration_requires_review(
            registration,
            data_product=binding.data_product,
            port_name=port_name,
            binding_type="output",
        )

        product = getattr(registration, "product", None) if registration else None
        self._enforce_output_product_constraints(
            binding=binding,
            port_name=port_name,
            product=product,
        )

    def _raise_if_registration_requires_review(
        self,
        registration: Optional[DataProductRegistrationResult],
        *,
        data_product: str,
        port_name: str,
        binding_type: str,
    ) -> None:
        if registration is None or not getattr(registration, "changed", False):
            return
        product = getattr(registration, "product", None)
        version = getattr(product, "version", None) or "<unknown>"
        status = (getattr(product, "status", None) or "").lower()
        if status != "draft":
            raise RuntimeError(
                f"Data product {binding_type} registration did not produce a draft version"
            )
        raise RuntimeError(
            f"Data product {data_product} {binding_type} port {port_name} requires review "
            f"at version {version}"
        )

    def _normalise_version_spec(self, spec: Optional[str]) -> Optional[str]:
        if spec is None:
            return None
        value = str(spec).strip()
        if not value:
            return None
        if value.startswith("=="):
            return value[2:].strip() or None
        return value

    def _version_satisfies(self, expected: Optional[str], actual: Optional[str]) -> bool:
        if expected is None or not expected.strip():
            return True
        if not actual:
            return False
        requirement = expected.strip()
        if requirement.startswith("=="):
            target = requirement[2:].strip()
            return not target or actual == target
        if requirement.startswith(">="):
            target = requirement[2:].strip()
            if not target:
                return True
            return version_key(actual) >= version_key(target)
        return actual == requirement

    def _enforce_version_constraint(
        self,
        *,
        expected: Optional[str],
        actual: Optional[str],
        data_product_id: str,
        subject: str,
    ) -> None:
        if self._version_satisfies(expected, actual):
            return
        raise ValueError(
            f"{subject} version {actual or '<unknown>'} does not satisfy {expected} "
            f"for data product {data_product_id}"
        )

    def _load_data_product(
        self,
        *,
        data_product_id: Optional[str],
        version_spec: Optional[str],
        default: Any = None,
    ) -> Any:
        if not data_product_id:
            return default
        client = self._data_product_client
        if client is None:
            return default
        requirement = version_spec.strip() if isinstance(version_spec, str) else ""
        direct_version = None
        if requirement and not requirement.startswith(">="):
            direct_version = self._normalise_version_spec(requirement)
        getter = getattr(client, "get", None)
        if direct_version and callable(getter):
            try:
                return getter(data_product_id, direct_version)
            except Exception:
                raise ValueError(
                    f"Data product {data_product_id} version {direct_version} could not be retrieved"
                )

        latest = None
        resolver = getattr(client, "latest", None)
        if callable(resolver):
            latest = resolver(data_product_id)
            if latest and self._version_satisfies(requirement, getattr(latest, "version", None)):
                return latest

        versions: Sequence[str] = ()
        lister = getattr(client, "list_versions", None)
        if callable(lister):
            versions = list(lister(data_product_id))
        if requirement:
            candidates = [version for version in versions if version]
            for version in sorted(candidates, key=version_key, reverse=True):
                if not self._version_satisfies(requirement, version):
                    continue
                if callable(getter):
                    return getter(data_product_id, version)
        return latest or default

    def _enforce_product_status(
        self,
        *,
        product: Any,
        data_product_id: str,
        operation: str,
    ) -> None:
        status = getattr(product, "status", None)
        version = getattr(product, "version", None)
        if status is None or str(status).strip() == "":
            raise ValueError(
                f"Data product {data_product_id}@{version or '<unknown>'} status '' "
                f"is not allowed for {operation} operations"
            )
        if str(status).strip().lower() != "active":
            raise ValueError(
                f"Data product {data_product_id}@{version or '<unknown>'} status {status!r} "
                f"is not allowed for {operation} operations"
            )

    def _enforce_input_product_constraints(
        self,
        *,
        binding: Any,
        port_name: str,
        product: Any,
    ) -> None:
        data_product_id = getattr(binding, "data_product", None)
        resolved = product or self._load_data_product(
            data_product_id=data_product_id,
            version_spec=getattr(binding, "data_product_version", None),
        )
        if resolved is not None:
            self._enforce_product_status(
                product=resolved,
                data_product_id=data_product_id,
                operation="read",
            )
            self._enforce_version_constraint(
                expected=getattr(binding, "data_product_version", None),
                actual=getattr(resolved, "version", None),
                data_product_id=data_product_id,
                subject="Data product",
            )

        source_product_id = getattr(binding, "source_data_product", None)
        if not source_product_id:
            return
        source_product = self._load_data_product(
            data_product_id=source_product_id,
            version_spec=getattr(binding, "source_data_product_version", None),
        )
        if source_product is None:
            return
        self._enforce_product_status(
            product=source_product,
            data_product_id=source_product_id,
            operation="read",
        )
        self._enforce_version_constraint(
            expected=getattr(binding, "source_data_product_version", None),
            actual=getattr(source_product, "version", None),
            data_product_id=source_product_id,
            subject="Source data product",
        )
        port = getattr(source_product, "find_output_port", lambda name: None)(
            getattr(binding, "source_output_port", port_name)
        )
        if port is None:
            if getattr(binding, "source_contract_version", None):
                raise ValueError(
                    f"Data product {source_product_id} output port "
                    f"{getattr(binding, 'source_output_port', port_name)} is not defined"
                )
            return
        self._enforce_version_constraint(
            expected=getattr(binding, "source_contract_version", None),
            actual=getattr(port, "version", None),
            data_product_id=source_product_id,
            subject="Output port contract",
        )

    def _enforce_output_product_constraints(
        self,
        *,
        binding: Any,
        port_name: str,
        product: Any,
    ) -> None:
        data_product_id = getattr(binding, "data_product", None)
        resolved = product or self._load_data_product(
            data_product_id=data_product_id,
            version_spec=getattr(binding, "data_product_version", None),
        )
        if resolved is None:
            return
        self._enforce_product_status(
            product=resolved,
            data_product_id=data_product_id,
            operation="write",
        )
        self._enforce_version_constraint(
            expected=getattr(binding, "data_product_version", None),
            actual=getattr(resolved, "version", None),
            data_product_id=data_product_id,
            subject="Data product",
        )

    def _status_from_validation(self, validation: ValidationResult, *, operation: str) -> ValidationResult:
        metrics = validation.metrics or {}
        violation_total = 0
        for key, value in metrics.items():
            if not key.startswith("violations."):
                continue
            if isinstance(value, (int, float)) and value > 0:
                violation_total += int(value)

        if validation.errors or not validation.ok:
            reason = validation.errors[0] if validation.errors else None
            return ValidationResult(
                status="block",
                reason=reason,
                details={
                    "errors": list(validation.errors),
                    "warnings": list(validation.warnings),
                    "violations": violation_total or len(validation.errors),
                },
            )

        if violation_total > 0:
            reason = (
                validation.warnings[0]
                if validation.warnings
                else "Data-quality violations detected"
            )
            details: Dict[str, Any] = {
                "warnings": list(validation.warnings),
                "violations": violation_total,
            }
            if operation == "write":
                details.setdefault("operation", operation)
                return ValidationResult(
                    status="block",
                    reason=reason,
                    details=details,
                )
            return ValidationResult(
                status="warn",
                reason=reason,
                details=details,
            )

        if validation.warnings:
            return ValidationResult(
                status="warn",
                reason=validation.warnings[0],
                details={
                    "warnings": list(validation.warnings),
                    "violations": violation_total,
                },
            )

        return ValidationResult(status="ok", details={"violations": 0})

    def _record_pipeline_activity(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        operation: str,
        pipeline_context: Optional[Mapping[str, Any]],
        status: Optional[ValidationResult],
        observations_reused: bool,
    ) -> None:
        cid, cver = contract_identity(contract)
        entry: Dict[str, Any] = {
            "operation": operation,
            "contract_id": cid,
            "contract_version": cver,
            "pipeline_context": dict(pipeline_context or {}),
            "observations_reused": observations_reused,
        }
        if status:
            entry["dq_status"] = status.status
            if status.reason:
                entry["dq_reason"] = status.reason
            if status.details:
                entry["dq_details"] = status.details
        event = dict(entry)
        event.setdefault(
            "recorded_at",
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        context_payload = event.get("pipeline_context")
        if isinstance(context_payload, Mapping):
            event["pipeline_context"] = dict(context_payload)
        elif context_payload is None:
            event["pipeline_context"] = {}
        self._store.record_pipeline_event(
            contract_id=cid,
            contract_version=cver,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            event=event,
        )

__all__ = ["LocalGovernanceServiceBackend"]
