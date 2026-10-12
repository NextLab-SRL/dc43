"""Unit tests for governance compatibility detector and violation strategies."""

from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance.models import (
    GovernanceWriteContext,
    ResolvedWritePlan,
)
from dc43_integrations.spark.ddl_constraints import (
    DDLConstraintPolicy,
    DDLConstraintType,
)
from dc43_integrations.spark.io.common import GovernanceSparkWriteRequest
from dc43_integrations.spark.io.compatibility import (
    IncompatibleGovernanceStrategyError,
    analyze_write_compatibility,
)
from dc43_integrations.spark.io.write import write_with_governance
from dc43_integrations.spark.violation_strategy import (
    FlagWriteViolationStrategy,
    SplitWriteViolationStrategy,
)


def make_test_contract(*, include_flag_col: bool = False):
    props = [
        SchemaProperty(name="id", physicalType="bigint", required=True, primaryKey=True),
        SchemaProperty(name="name", physicalType="string", required=True),
        SchemaProperty(name="event_date", physicalType="date", partitioned=True),
    ]
    if include_flag_col:
        props.append(
            SchemaProperty(name="_corrupted_data", physicalType="array<string>", required=False)
        )
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.contract",
        name="Contract",
        status="active",
        schema=[SchemaObject(name="orders", properties=props)],
    )


def test_shadow_column_conflict_detected():
    contract = make_test_contract(include_flag_col=False)
    strategy = FlagWriteViolationStrategy(column_name="_corrupted_data", allow_shadow_columns=False)
    req = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=strategy,
    )

    report = analyze_write_compatibility(req, contract=contract, table_exists=False)
    assert not report.is_compatible
    assert any(i.code == "SHADOW_COLUMN_NOT_IN_CONTRACT" for i in report.issues)


def test_shadow_column_allowed_when_flag_set():
    contract = make_test_contract(include_flag_col=False)
    # Permis explicitement par allow_shadow_columns=True
    strategy = FlagWriteViolationStrategy(column_name="_corrupted_data", allow_shadow_columns=True)
    req = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=strategy,
        constraint_policy=DDLConstraintPolicy.no_nullability(),  # évite le conflit NOT NULL
    )

    report = analyze_write_compatibility(req, contract=contract, table_exists=False)
    assert not any(i.code == "SHADOW_COLUMN_NOT_IN_CONTRACT" for i in report.issues)


def test_shadow_column_resolved_when_in_contract():
    contract = make_test_contract(include_flag_col=True)
    strategy = FlagWriteViolationStrategy(column_name="_corrupted_data", allow_shadow_columns=False)
    req = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=strategy,
        constraint_policy=DDLConstraintPolicy.no_nullability(),
    )

    report = analyze_write_compatibility(req, contract=contract, table_exists=False)
    assert not any(i.code == "SHADOW_COLUMN_NOT_IN_CONTRACT" for i in report.issues)


def test_flag_strategy_physical_not_null_conflict():
    contract = make_test_contract(include_flag_col=True)
    strategy = FlagWriteViolationStrategy()
    req = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=strategy,
        # Par défaut: strict policy avec NOT NULL
    )

    report = analyze_write_compatibility(req, contract=contract, table_exists=False)
    assert not report.is_compatible
    assert any(i.code == "INVARIANT_NOT_NULL_CONFLICT" for i in report.issues)


def test_flag_strategy_conflict_resolved_with_policy():
    contract = make_test_contract(include_flag_col=True)
    strategy = FlagWriteViolationStrategy()
    req = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=strategy,
        constraint_policy=DDLConstraintPolicy.no_nullability(),
    )

    report = analyze_write_compatibility(req, contract=contract, table_exists=False)
    assert report.is_compatible
    assert len(report.issues) == 0


def test_write_with_governance_raises_incompatible_strategy(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df
    mock_spark.catalog.tableExists.return_value = False

    contract = make_test_contract(include_flag_col=False)

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None,
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=FlagWriteViolationStrategy(),  # Incompatible: shadow col + NOT NULL
        compatibility_mode="raise",
    )

    with pytest.raises(IncompatibleGovernanceStrategyError) as exc_info:
        write_with_governance(
            df=mock_df,
            request=request,
            governance_service=mock_gov,
            enforce=True,
            auto_cast=False,
        )

    err_msg = str(exc_info.value)
    assert "Incompatibility detected" in err_msg
    assert "SHADOW_COLUMN_NOT_IN_CONTRACT" in err_msg
    assert "Resolution:" in err_msg


def test_write_with_governance_warn_mode(monkeypatch):
    monkeypatch.setattr("dc43_integrations.spark.validation.col", lambda name: MagicMock())
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_df.sparkSession = mock_spark
    mock_df.sql_ctx.sparkSession = mock_spark
    mock_df.isStreaming = False
    mock_df.columns = ["id", "name", "event_date"]
    mock_df.select.return_value = mock_df
    mock_spark.catalog.tableExists.return_value = False

    contract = make_test_contract(include_flag_col=False)

    mock_gov = MagicMock()
    mock_assessment = SimpleNamespace(
        validation=ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}),
        status=None,
    )
    mock_gov.evaluate_dataset.return_value = mock_assessment
    mock_gov.evaluate_write_plan.return_value = mock_assessment
    mock_gov.resolve_contract.return_value = contract
    mock_gov.resolve_write_context.return_value = ResolvedWritePlan(
        contract=contract,
        contract_id="test.contract",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="1.0.0",
        dataset_format="delta",
        pipeline_context={},
    )

    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(dataset_id="orders"),
        table="orders",
        violation_strategy=FlagWriteViolationStrategy(allow_shadow_columns=True),
        compatibility_mode="warn",
    )

    result = write_with_governance(
        df=mock_df,
        request=request,
        governance_service=mock_gov,
        enforce=True,
        auto_cast=False,
    )

    assert any("INVARIANT_NOT_NULL_CONFLICT" in w for w in result.validation.warnings)


def test_split_strategy_reject_request_has_relaxed_policy():
    strategy = SplitWriteViolationStrategy()
    assert strategy.reject_constraint_policy == DDLConstraintPolicy.relaxed()
