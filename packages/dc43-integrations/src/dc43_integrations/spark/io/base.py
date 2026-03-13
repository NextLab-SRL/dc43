from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Type,
    Union,
    overload,
    Literal,
    Iterable,
)
from typing import Tuple
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .common import GovernanceSparkReadRequest, GovernanceSparkWriteRequest

from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
from dc43_service_clients.data_quality import ValidationResult, ObservationPayload
from dc43_service_clients.data_products import (
    DataProductServiceClient,
    DataProductInputBinding,
    DataProductOutputBinding,
    normalise_input_binding,
    normalise_output_binding,
)
from dc43_service_clients.odps import OpenDataProductStandard
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance import (
    normalise_pipeline_context,
    GovernancePublicationMode,
    resolve_publication_mode,
    QualityAssessment,
)
from dc43_service_clients.governance.models import (
    ResolvedReadPlan,
    ResolvedWritePlan,
)
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_backends.core.odcs import contract_identity, ensure_version

from dc43_integrations.spark.data_quality import (
    build_metrics_payload,
    collect_observations,
)
from dc43_integrations.spark.open_data_lineage import build_lineage_run_event
from dc43_integrations.spark.open_telemetry import record_telemetry_span
from dc43_integrations.spark.validation import apply_contract

from dc43_integrations.spark.io.common import (
    PipelineContextLike,
    dataset_id_from_ref,
    _annotate_observation_scope,
    get_delta_version,
    _as_governance_service,
    _evaluate_with_service,
    resolve_dataset_version,
)
from dc43_integrations.spark.io.resolution import (
    DatasetResolution,
    DatasetLocatorStrategy,
)
from dc43_integrations.spark.io.locators import (
    ContractFirstDatasetLocator,
    _ref_from_contract,
    _timestamp,
)
from dc43_integrations.spark.io.status import (
    ReadStatusStrategy,
    DefaultReadStatusStrategy,
    ReadStatusContext,
)
from dc43_integrations.spark.io.validation import (
    _resolve_contract,
    _check_contract_version,
    _enforce_contract_status,
    _select_data_product,
    _check_data_product_version,
    _enforce_data_product_status,
    _load_binding_product_version,
    _apply_plan_data_product_policy,
    _paths_compatible,
)
from dc43_integrations.spark.io.streaming import (
    StreamingObservationWriter,
    StreamingInterventionStrategy,
)
from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteRequest,
    WriteStrategyContext,
    WriteViolationStrategy,
)

logger = logging.getLogger(__name__)


@dataclass
class WriteExecutionResult:
    """Return value produced by write executors."""

    result: ValidationResult
    status: Optional[ValidationResult]
    streaming_queries: list[Any]


def _resolve_publication_mode(
    *,
    spark: SparkSession,
    override: GovernancePublicationMode | str | None,
) -> GovernancePublicationMode:
    config: Dict[str, str] | None = None
    try:
        spark_conf = spark.conf  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - Spark may be absent in unit tests
        spark_conf = None
    if spark_conf is not None:
        for key in (
            "dc43.governance.publicationMode",
            "dc43.governance.publication_mode",
            "governance.publication.mode",
        ):
            try:
                value = spark_conf.get(key)
            except Exception:  # pragma: no cover - SparkConf guards may throw
                value = None
            if value:
                config = {key: value}
                break
    return resolve_publication_mode(explicit=override, config=config)


def _supports_dataframe_checkpointing(df: DataFrame) -> bool:
    """Return ``True`` when the active Spark cluster supports checkpointing."""
    try:
        spark = df.sparkSession
    except Exception:
        return True

    try:
        conf = spark.sparkContext.getConf()
    except Exception:
        return True

    indicators: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("spark.databricks.service.serverless.enabled", ("true", "1", "yes")),
        ("spark.databricks.service.serverless", ("true", "1", "yes")),
        ("spark.databricks.service.clusterSource", ("serverless",)),
        ("spark.databricks.clusterUsageTags.clusterAllType", ("serverless",)),
    )
    for key, expected in indicators:
        try:
            val = conf.get(key)
            if val and str(val).lower() in expected:
                return False
        except Exception:
            pass
    return True


class BaseReadExecutor:
    """Shared implementation for batch and streaming read helpers."""

    streaming: bool = False
    require_location: bool = True

    def __init__(
        self,
        *,
        spark: SparkSession,
        request: GovernanceSparkReadRequest | Mapping[str, object],
        governance_service: GovernanceServiceClient,
        enforce: bool,
        auto_cast: bool,
        plan: Optional[ResolvedReadPlan] = None,
    ) -> None:
        self.spark = spark
        self.request = request if isinstance(request, GovernanceSparkReadRequest) else GovernanceSparkReadRequest(**dict(request))
        self.governance_service = governance_service
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.plan = plan
        
        self.contract_id = plan.contract_id if plan else None
        self.expected_contract_version = plan.contract_version if plan else None
        self.user_format = self.request.format if self.request.format is not None else (plan.dataset_format if plan else None)
        self.user_path = self.request.path
        self.user_table = self.request.table
        self.options = dict(self.request.options or {})
        
        self.dp_binding = self.request.context.input_binding if hasattr(self.request.context, 'input_binding') else None
        self.locator = self.request.dataset_locator or ContractFirstDatasetLocator()
        
        handler = getattr(self.request, 'status_strategy', None) or DefaultReadStatusStrategy()
        self.data_product_status_enforce = enforce
        if plan is not None:
            handler, status_enforce = _apply_plan_data_product_policy(handler, plan, default_enforce=enforce)
            self.data_product_status_enforce = status_enforce
        self.status_handler = handler
        
        self.pipeline_context = getattr(self.request.context, 'pipeline_context', None) or (plan.pipeline_context if plan else None)
        self.publication_mode = _resolve_publication_mode(spark=spark, override=self.request.publication_mode)
        self.open_data_lineage_only = (self.publication_mode is GovernancePublicationMode.OPEN_DATA_LINEAGE)
        self.open_telemetry_only = (self.publication_mode is GovernancePublicationMode.OPEN_TELEMETRY)
        self._skip_governance_activity = self.open_data_lineage_only or self.open_telemetry_only
        self._last_read_resolution: Optional[DatasetResolution] = None
        self.warnings: list[str] = []

    def execute(self) -> tuple[DataFrame, Optional[ValidationResult]]:
        """Execute the read pipeline and return the dataframe/status pair."""

        contract = self._resolve_contract()
        resolution = self._resolve_resolution(contract)
        dataframe = self._load_dataframe(resolution)
        streaming_active = self._detect_streaming(dataframe)
        dataset_id, dataset_version = self._dataset_identity(resolution, streaming_active)
        (
            dataframe,
            validation,
            expectation_plan,
            contract_identity_tuple,
            assessment,
        ) = self._apply_contract(
            dataframe,
            contract,
            dataset_id,
            dataset_version,
            streaming_active,
            self.pipeline_context,
        )
        status = self._evaluate_governance(
            dataframe,
            contract,
            validation,
            expectation_plan,
            dataset_id,
            dataset_version,
            streaming_active,
            contract_identity_tuple,
            assessment,
        )
        dataframe = self.status_handler.apply(
            dataframe=dataframe,
            status=status,
            enforce=self.enforce,
            context=ReadStatusContext(
                contract=contract,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            ),
        )

        if self.open_telemetry_only:
            record_telemetry_span(
                operation="read",
                plan=self.plan,
                pipeline_context=self.pipeline_context,
                contract_id=contract.id if contract else None,
                contract_version=contract.version if contract else None,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                validation=validation,
                status=status,
                expectation_plan=expectation_plan,
            )

        return dataframe, status

    def _resolve_contract(self) -> Optional[OpenDataContractStandard]:
        if self.plan is not None:
            contract = self.plan.contract
            ensure_version(contract)
            _check_contract_version(self.expected_contract_version, contract.version)
            _enforce_contract_status(
                handler=self.status_handler,
                contract=contract,
                enforce=self.enforce,
                operation="read",
            )
            return contract

        contract_id = self.contract_id
        expected_version = self.expected_contract_version

        if contract_id is None:
            return None

        contract = _resolve_contract(
            contract_id=contract_id,
            expected_version=expected_version,
            service=None,
            governance=_as_governance_service(self.governance_service),
        )
        ensure_version(contract)
        _check_contract_version(expected_version, contract.version)
        _enforce_contract_status(
            handler=self.status_handler,
            contract=contract,
            enforce=self.enforce,
            operation="read",
        )
        return contract

    def _resolve_resolution(
        self, contract: Optional[OpenDataContractStandard]
    ) -> DatasetResolution:
        resolution = self.locator.for_read(
            contract=contract,
            spark=self.spark,
            format=self.user_format,
            path=self.user_path,
            table=self.user_table,
        )

        # Trust canonical plan locations if provided
        if self.plan is not None:
            if self.plan.dataset_path:
                resolution.path = self.plan.dataset_path
            if self.plan.dataset_table:
                resolution.table = self.plan.dataset_table
            if self.plan.dataset_format and resolution.format is None:
                resolution.format = self.plan.dataset_format

        self._last_read_resolution = resolution

        original_path = self.user_path
        original_table = self.user_table
        original_format = self.user_format

        if contract:
            c_path, c_table = _ref_from_contract(contract)
            c_fmt = contract.servers[0].format if contract.servers else None
            if original_path and c_path and not _paths_compatible(original_path, c_path):
                logger.warning(
                    "Provided path %s does not match contract server path %s",
                    original_path,
                    c_path,
                )
                self.warnings.append(f"Writing to physical path '{original_path}' which does not match contract path '{c_path}'")
            if original_table and c_table and original_table != c_table:
                logger.warning(
                    "Provided table %s does not match contract server table %s",
                    original_table,
                    c_table,
                )
            if original_format and c_fmt and original_format != c_fmt:
                logger.warning(
                    "Provided format %s does not match contract server format %s",
                    original_format,
                    c_fmt,
                )
                self.warnings.append(f"Writing with format '{original_format}' which does not match contract format '{c_fmt}'")
            if resolution.format is None:
                resolution.format = c_fmt

        if self.plan is not None and resolution.format is None:
            resolution.format = self.plan.dataset_format

        if (
            self.require_location
            and not resolution.table
            and not (resolution.path or resolution.load_paths)
        ):
            raise ValueError("Either table or path must be provided for read")

        return resolution

    def _load_dataframe(self, resolution: DatasetResolution) -> DataFrame:
        reader = self.spark.readStream if self.streaming else self.spark.read
        if resolution.format:
            reader = reader.format(resolution.format)

        option_map: Dict[str, str] = {}
        if resolution.read_options:
            option_map.update(resolution.read_options)
        if self.options:
            option_map.update(self.options)
        if option_map:
            reader = reader.options(**option_map)

        target = resolution.load_paths or resolution.path
        if resolution.table:
            return reader.table(resolution.table)
        if target:
            return reader.load(target)
        return reader.load()

    def _detect_streaming(self, dataframe: DataFrame) -> bool:
        try:
            is_streaming = bool(dataframe.isStreaming)  # type: ignore[attr-defined]
        except AttributeError:
            is_streaming = False
        streaming_active = self.streaming or is_streaming
        if streaming_active and not self.streaming:
            logger.info("Detected streaming dataframe; enabling streaming mode")
        return streaming_active

    def _dataset_identity(
        self,
        resolution: DatasetResolution,
        streaming_active: bool,
    ) -> tuple[str, str]:
        dataset_id = (
            (self.plan.dataset_id if self.plan and self.plan.dataset_id else None)
            or resolution.dataset_id
            or dataset_id_from_ref(
                table=resolution.table,
                path=resolution.path,
            )
        )
        observed_version = (
            (self.plan.dataset_version if self.plan and self.plan.dataset_version else None)
            or resolution.dataset_version
            or get_delta_version(
                self.spark,
                table=resolution.table,
                path=resolution.path,
            )
        )
        dataset_version = observed_version or ("unknown" if not streaming_active else _timestamp())
        return dataset_id, dataset_version

    def _apply_contract(
        self,
        dataframe: DataFrame,
        contract: Optional[OpenDataContractStandard],
        dataset_id: str,
        dataset_version: str,
        streaming_active: bool,
        pipeline_context: Optional[PipelineContextLike],
    ) -> tuple[
        DataFrame,
        Optional[ValidationResult],
        list[Mapping[str, Any]],
        Optional[tuple[str, str]],
        Optional[QualityAssessment],
    ]:
        if contract is None:
            return dataframe, None, [], None, None

        governance_client = _as_governance_service(self.governance_service)

        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)

        expectation_plan = list(
            governance_client.describe_expectations(
                contract_id=cid,
                contract_version=cver,
            )
        )

        observed_schema, observed_metrics = collect_observations(
            dataframe,
            contract,
            expectations=expectation_plan,
            collect_metrics=not streaming_active,
        )

        base_pipeline_context = normalise_pipeline_context(pipeline_context)

        def _observations() -> ObservationPayload:
            return ObservationPayload(
                metrics=dict(observed_metrics or {}),
                schema=dict(observed_schema or {}),
                reused=True,
            )

        assessment = governance_client.evaluate_dataset(
            contract_id=cid,
            contract_version=cver,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            validation=None,
            observations=_observations,
            pipeline_context=base_pipeline_context,
            operation="read",
            bump=self.plan.policy.bump if self.plan and self.plan.policy else "minor",
            draft_on_violation=self.plan.policy.draft_on_violation if self.plan and self.plan.policy else False,
        )
        validation = assessment.validation or assessment.status
        if validation is None:
            validation = ValidationResult(
                ok=True, errors=[], warnings=[], metrics={}, schema={}
            )

        if validation:
            if "dataset_id" not in validation.details:
                validation.merge_details({"dataset_id": dataset_id, "dataset_version": dataset_version})
            _annotate_observation_scope(validation, operation="read", scope="input_slice")
            if assessment and assessment.status and assessment.status is not validation:
                _annotate_observation_scope(assessment.status, operation="read", scope="input_slice")

        dataframe = apply_contract(dataframe, contract, auto_cast=self.auto_cast)
        if validation and self.warnings:
            for w in self.warnings:
                if w not in validation.warnings:
                    validation.warnings.append(w)
        return dataframe, validation, expectation_plan, (cid, cver), assessment

    def _evaluate_governance(
        self,
        dataframe: DataFrame,
        contract: Optional[OpenDataContractStandard],
        validation: Optional[ValidationResult],
        expectation_plan: list[Mapping[str, Any]],
        dataset_id: str,
        dataset_version: str,
        streaming_active: bool,
        contract_identity_tuple: Optional[tuple[str, str]],
        assessment: Optional[QualityAssessment],
    ) -> Optional[ValidationResult]:
        governance_client = _as_governance_service(self.governance_service)
        if governance_client is None or contract is None or contract_identity_tuple is None:
            return validation

        cid, cver = contract_identity_tuple
        if assessment is None:
            if validation is None:
                return None

            def _observations() -> ObservationPayload:
                metrics_payload, schema_payload, reused = build_metrics_payload(
                    dataframe,
                    contract,
                    validation=validation,
                    include_schema=True,
                    expectations=expectation_plan,
                    collect_metrics=not streaming_active,
                )
                return ObservationPayload(metrics=metrics_payload, schema=schema_payload, reused=reused)

            assessment = governance_client.evaluate_dataset(
                contract_id=cid,
                contract_version=cver,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                validation=validation,
                observations=_observations,
                pipeline_context=normalise_pipeline_context(self.pipeline_context),
                operation="read",
                bump=self.plan.policy.bump if self.plan and self.plan.policy else "minor",
                draft_on_violation=self.plan.policy.draft_on_violation if self.plan and self.plan.policy else False,
            )

        status = assessment.status
        if status is None and assessment.validation is not None:
            status = assessment.validation
        
        if self.plan is not None and not self._skip_governance_activity:
            governance_client.register_read_activity(plan=self.plan, assessment=assessment)
        
        if self.open_data_lineage_only:
            lineage_event = build_lineage_run_event(
                operation="read",
                plan=self.plan,
                pipeline_context=self.pipeline_context,
                contract_id=cid,
                contract_version=cver,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                binding=self.dp_binding,
                validation=validation,
                status=status,
                expectation_plan=expectation_plan,
            )
            governance_client.publish_lineage_event(event=lineage_event)

        return status or validation


class BatchReadExecutor(BaseReadExecutor):
    """Batch-only read execution."""


class StreamingReadExecutor(BaseReadExecutor):
    """Streaming read execution with dataset version fallbacks."""

    streaming = True
    require_location = False


class BaseWriteExecutor:
    """Shared implementation for batch and streaming contract writes."""

    streaming: bool = False

    def __init__(
        self,
        *,
        df: DataFrame,
        request: GovernanceSparkWriteRequest | Mapping[str, object],
        governance_service: GovernanceServiceClient,
        enforce: bool,
        auto_cast: bool,
        streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
        plan: Optional[ResolvedWritePlan] = None,
    ) -> None:
        self.df = df
        self.request = request if isinstance(request, GovernanceSparkWriteRequest) else GovernanceSparkWriteRequest(**dict(request))
        self.governance_service = governance_service
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.plan = plan
        
        self.contract_id = plan.contract_id if plan else None
        self.expected_contract_version = plan.contract_version if plan else None
        self.user_format = self.request.format if self.request.format is not None else (plan.dataset_format if plan else None)
        self.format = self.user_format
        self.path = self.request.path
        self.table = self.request.table
        self.options = dict(self.request.options or {})
        self.mode = self.request.mode or "append"
        
        self.dp_output_binding = self.request.context.output_binding if hasattr(self.request.context, 'output_binding') else None
        self.locator = self.request.dataset_locator or ContractFirstDatasetLocator()
        
        self.publication_mode = _resolve_publication_mode(spark=df.sparkSession, override=self.request.publication_mode)
        self.open_data_lineage_only = (self.publication_mode is GovernancePublicationMode.OPEN_DATA_LINEAGE)
        self.open_telemetry_only = (self.publication_mode is GovernancePublicationMode.OPEN_TELEMETRY)
        self._skip_governance_activity = self.open_data_lineage_only or self.open_telemetry_only
        self._last_write_resolution: Optional[DatasetResolution] = None
        self.warnings: list[str] = []
        
        self.strategy = getattr(self.request, 'violation_strategy', None) or NoOpWriteViolationStrategy()
        self.data_product_status_enforce = enforce
        if plan is not None:
             self.strategy, self.data_product_status_enforce = _apply_plan_data_product_policy(self.strategy, plan, default_enforce=enforce)
        
        self.streaming_intervention_strategy = getattr(self.request, 'streaming_intervention_strategy', None)
        self.writer_modifier = getattr(self.request, 'writer_modifier', None)
        self.observation_writer_modifier = getattr(self.request, 'observation_writer_modifier', None)
        self.streaming_batch_callback = streaming_batch_callback
        self.pipeline_context = getattr(self.request.context, 'pipeline_context', None) or (plan.pipeline_context if plan else None)

    def execute(self) -> WriteExecutionResult:
        df = self.df
        contract_id = self.contract_id
        expected_contract_version = self.expected_contract_version
        path = self.path
        table = self.table
        format = self.format
        options = dict(self.options)
        mode = self.mode
        enforce = self.enforce
        auto_cast = self.auto_cast
        governance_service = self.governance_service
        dp_output_binding = self.dp_output_binding
        locator = self.locator
        strategy = self.strategy
        pipeline_context = self.pipeline_context
        streaming_intervention_strategy = self.streaming_intervention_strategy

        resolved_contract_id = contract_id
        resolved_expected_version = expected_contract_version
        governance_plan = self.plan
        
        if resolved_contract_id is not None:
            contract_id = resolved_contract_id
        if expected_contract_version is None and resolved_expected_version is not None:
            expected_contract_version = resolved_expected_version

        contract = governance_plan.contract if governance_plan else None
        if not contract and contract_id:
            contract = _resolve_contract(
                contract_id=contract_id,
                expected_version=expected_contract_version,
                service=None,
                governance=_as_governance_service(governance_service),
            )
        
        if contract:
            ensure_version(contract)
            _check_contract_version(expected_contract_version, contract.version)
            _enforce_contract_status(handler=strategy, contract=contract, enforce=enforce, operation="write")

        resolution = locator.for_write(contract=contract, df=df, format=format, path=path, table=table)
        self._last_write_resolution = resolution
        path, table, format = resolution.path, resolution.table, resolution.format

        # Trust canonical plan locations if provided
        if governance_plan:
            path = governance_plan.dataset_path or path
            table = governance_plan.dataset_table or table
            format = governance_plan.dataset_format or format        # Check format and path match
        if contract and contract.servers:
            c_path, c_table = _ref_from_contract(contract)
            c_fmt = contract.servers[0].format if contract.servers else None
            if self.path and c_path and not _paths_compatible(self.path, c_path):
                msg = f"Path {self.path} does not match contract server path {c_path}"
                logger.warning(msg)
                self.warnings.append(msg)
            if self.format and c_fmt and self.format.lower() != c_fmt.lower():
                msg = f"Format {self.format} does not match contract server format {c_fmt}"
                logger.warning(msg)
                self.warnings.append(msg)


        dataset_id = resolution.dataset_id or (governance_plan.dataset_id if governance_plan else None) or dataset_id_from_ref(table=table, path=path)
        dataset_version = resolution.dataset_version or (governance_plan.dataset_version if governance_plan else None)
        
        is_streaming = False
        try:
            is_streaming = bool(df.isStreaming)
        except AttributeError:
            pass
        streaming_active = self.streaming or is_streaming
        if streaming_active and not dataset_version:
            dataset_version = _timestamp()

        governance_client = _as_governance_service(governance_service)
        result = ValidationResult(ok=True, errors=[], warnings=[], metrics={})
        assessment: Optional[QualityAssessment] = None
        expectation_plan: list[Mapping[str, Any]] = []
        
        preflight_version = resolve_dataset_version(dataset_version)
        if governance_plan:
            governance_plan.dataset_version = preflight_version
        
        if contract:
            cid, cver = contract_identity(contract)
            expectation_plan = list(governance_client.describe_expectations(contract_id=cid, contract_version=cver))
            
            schema, metrics = collect_observations(df, contract, expectations=expectation_plan, collect_metrics=not streaming_active)
            dq_validation = None
            
            def _obs(): return ObservationPayload(metrics=dict(metrics or {}), schema=dict(schema or {}), reused=True)
            
            if governance_plan:
                assessment = governance_client.evaluate_write_plan(plan=governance_plan, validation=dq_validation, observations=_obs)
            else:
                assessment = governance_client.evaluate_dataset(contract_id=cid, contract_version=cver, dataset_id=dataset_id, dataset_version=preflight_version, validation=dq_validation, observations=_obs, pipeline_context=normalise_pipeline_context(pipeline_context), operation="write")
            result = assessment.validation or assessment.status or dq_validation or result

            if result:
                _annotate_observation_scope(result, operation="write", scope="pre_write_dataframe")
            if assessment and assessment.status:
                _annotate_observation_scope(assessment.status, operation="write", scope="pre_write_dataframe")

            if not result.ok and self.enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")

            df = apply_contract(df, contract, auto_cast=auto_cast)

        observation_writer: Optional[StreamingObservationWriter] = None
        checkpoint_option = options.get("checkpointLocation") if options else None
        obs_checkpoint = None
        if checkpoint_option:
            obs_checkpoint = f"{checkpoint_option.rstrip('/')}/_dc43_obs"
        if streaming_active and contract:
            observation_writer = StreamingObservationWriter(
                contract=contract,
                expectation_plan=expectation_plan,
                governance_service=governance_service,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                enforce=enforce,
                checkpoint_location=obs_checkpoint,
                intervention=streaming_intervention_strategy,
                progress_callback=self.streaming_batch_callback,
                pipeline_context=pipeline_context,
            )
        expectation_predicates: Mapping[str, str] = {}
        predicates = result.details.get("expectation_predicates")
        if isinstance(predicates, Mapping):
            expectation_predicates = dict(predicates)

        if contract:
            def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
                schema, metrics = collect_observations(
                    new_df, contract, expectations=expectation_plan, collect_metrics=not streaming_active
                )
                def _observations() -> ObservationPayload:
                    return ObservationPayload(metrics=dict(metrics or {}), schema=dict(schema or {}), reused=True)
                follow_up = governance_client.evaluate_dataset(
                    contract_id=contract.id, contract_version=contract.version,
                    dataset_id=dataset_id, dataset_version=dataset_version or "",
                    validation=None, observations=_observations,
                    pipeline_context=normalise_pipeline_context(pipeline_context),
                    operation="write", draft_on_violation=False,
                )
                return follow_up.validation or follow_up.status or ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={})
        else:
            def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
                return ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={})

        context = WriteStrategyContext(
            df=self.df, aligned_df=df, contract=contract, path=path, table=table, format=format, options=options, mode=mode,
            validation=result, dataset_id=dataset_id, dataset_version=preflight_version,
            revalidate=revalidator,
            expectation_predicates=expectation_predicates, pipeline_context=normalise_pipeline_context(pipeline_context),
            streaming=streaming_active, streaming_observation_writer=observation_writer,
            writer_modifier=self.writer_modifier,
            observation_writer_modifier=self.observation_writer_modifier
        )
        violation_plan = strategy.plan(context)
        requests = ([violation_plan.primary] if violation_plan.primary else []) + list(violation_plan.additional)
        
        streaming_queries = []
        primary_status = None
        for i, req in enumerate(requests):
            if req.warnings:
                for w in req.warnings:
                    if w not in result.warnings:
                        result.warnings.append(w)
            status, _, handles = _execute_write_request(req, governance_client=governance_client, enforce=enforce)
            streaming_queries.extend(handles)
            if i == 0: primary_status = status

        if governance_plan and governance_client and assessment and not self._skip_governance_activity:
            governance_client.register_write_activity(plan=governance_plan, assessment=assessment)
        
        if self.open_data_lineage_only and governance_client:
            event = build_lineage_run_event(operation="write", plan=governance_plan, pipeline_context=pipeline_context, contract_id=contract_id, contract_version=expected_contract_version, dataset_id=dataset_id, dataset_version=preflight_version, validation=result, status=primary_status, expectation_plan=expectation_plan)
            governance_client.publish_lineage_event(event=event)
            
        if self.open_telemetry_only:
            record_telemetry_span(
                operation="write",
                plan=governance_plan,
                pipeline_context=pipeline_context,
                contract_id=contract_id,
                contract_version=expected_contract_version,
                dataset_id=dataset_id,
                dataset_version=preflight_version,
                validation=result,
                status=primary_status,
                expectation_plan=expectation_plan,
            )
            
        gov_status = assessment.status if assessment else None
        if result and self.warnings:
            for w in self.warnings:
                if w not in result.warnings:
                    result.warnings.append(w)
        return WriteExecutionResult(result, gov_status, streaming_queries)

def _execute_write_request(
    request: WriteRequest,
    *,
    governance_client: Optional[GovernanceServiceClient],
    enforce: bool,
) -> tuple[Optional[ValidationResult], Optional[ValidationResult], list[Any]]:
    df = request.df
    streaming_handles = []
    
    validation = request.validation_factory() if getattr(request, "validation_factory", None) else None
    observation_writer = request.streaming_observation_writer
    if observation_writer is not None and validation is not None:
        try:
            observation_writer.attach_validation(validation)
        except RuntimeError:
            pass

    if request.streaming:
        writer = df.writeStream.outputMode(request.mode or "append")
        if request.format: writer = writer.format(request.format)
        if request.options: writer = writer.options(**request.options)
        if request.writer_modifier: writer = request.writer_modifier(writer)
        query = writer.toTable(request.table) if request.table else writer.start(request.path)
        streaming_handles.append(query)
        
        if observation_writer is not None and not observation_writer.active:
            metrics_mode = request.mode or "append"
            metrics_query = observation_writer.start(
                df,
                output_mode=metrics_mode,
                modifier=request.observation_writer_modifier,
            )
            streaming_handles.append(metrics_query)
            observation_writer.watch_sink_query(query)
    else:
        writer = df.write.mode(request.mode)
        if request.format: writer = writer.format(request.format)
        if request.options: writer = writer.options(**request.options)
        if request.writer_modifier: writer = request.writer_modifier(writer)
        if request.table: writer.saveAsTable(request.table)
        else: writer.save(request.path)
    if governance_client and request.contract and request.dataset_id:
        # Pre-create streaming sink to avoid TABLE_OR_VIEW_NOT_FOUND during property sync
        if request.streaming:
            try:
                # Need spark session to evaluate options and execute CREATE TABLE
                spark = getattr(df, "sparkSession", getattr(getattr(df, "sql_ctx", None), "sparkSession", None))
                if spark is not None:
                    tgt_fmt = request.format or "delta"
                    if tgt_fmt.lower() == "delta":
                        if request.table:
                            try:
                                if not spark.catalog.tableExists(request.table):
                                    fields_def = ", ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in getattr(df, "schema", [])])
                                    spark.sql(f"CREATE TABLE IF NOT EXISTS {request.table} ({fields_def}) USING {tgt_fmt}")
                            except Exception:
                                empty_df = spark.createDataFrame([], getattr(df, "schema", None))
                                empty_df.write.format(tgt_fmt).mode("append").saveAsTable(request.table)
                        elif request.path:
                            empty_df = spark.createDataFrame([], getattr(df, "schema", None))
                            empty_df.write.format(tgt_fmt).mode("append").save(request.path)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("Failed to pre-create streaming sink: %s", e)
        
        try:
            governance_client.link_dataset_contract(
                dataset_id=request.dataset_id,
                dataset_version=request.dataset_version or "",
                contract_id=request.contract.id,
                contract_version=request.contract.version,
            )
        except Exception:
            # Defensive logging for linkage failure
            import logging
            logging.getLogger(__name__).exception(
                "Failed to link dataset %s to contract %s",
                request.dataset_id, request.contract.id
            )

    return None, validation, streaming_handles


def _execute_read(
    executor_cls: Type[BaseReadExecutor],
    *,
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool,
    auto_cast: bool,
    return_status: bool,
    plan: Optional[ResolvedReadPlan] = None,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    executor = executor_cls(
        spark=spark,
        request=request,
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=auto_cast,
        plan=plan,
    )
    res, status = executor.execute()
    return (res, status) if return_status else res
