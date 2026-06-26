import pytest
from unittest.mock import MagicMock
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_integrations.spark.data_quality import compute_metrics
from dc43_integrations.spark.io.base import BaseWriteExecutor
from dc43_integrations.spark.violation_strategy import (
    SplitWriteViolationStrategy,
    WriteStrategyContext,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance.models import ResolvedWritePlan, GovernancePolicy


def test_compute_metrics_resilience():
    # Mock dataframe
    mock_df = MagicMock()
    mock_df.columns = ["id", "date_mutation"]
    mock_df.count.return_value = 10
    
    # We mock filter to raise an exception when called to simulate evaluation parse error
    mock_df.filter.side_effect = RuntimeError("Spark SQL simulation parse error")
    
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.resilience",
        name="Resilience",
        schema=[]
    )

    # We manually build expectations with a predicate designed to trigger a SQL parse/execution exception in Spark
    expectations = [
        {
            "key": "bad_predicate",
            "rule": "regex",
            "column": "date_mutation",
            "predicate": "date_mutation > 0 AND (syntax error", # triggers exception
        }
    ]

    metrics = compute_metrics(mock_df, contract, expectations=expectations)
    
    # It should have run total row count successfully, caught the exception on the bad check,
    # and populated the error and violation count (treated as total rows violated).
    assert metrics["row_count"] == 10
    assert metrics["violations.bad_predicate"] == 10
    assert "errors.bad_predicate" in metrics
    assert "Spark SQL simulation parse error" in metrics["errors.bad_predicate"]


def test_date_mutation_cannot_parse_timestamp():
    # Mock dataframe
    mock_df = MagicMock()
    mock_df.columns = ["id", "date_mutation"]
    mock_df.count.return_value = 100
    
    # Simulate CANNOT_PARSE_TIMESTAMP error when filtering / checking the date format
    mock_df.filter.side_effect = RuntimeError(
        "Spark SQL Exception: CANNOT_PARSE_TIMESTAMP. "
        "Cannot parse '2022-04-01' to date with format 'dd/MM/yyyy'."
    )
    
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.date_mutation",
        name="Date Mutation Resilience",
        schema=[
            SchemaObject(
                name="mutations",
                properties=[
                    SchemaProperty(
                        name="date_mutation",
                        physicalType="string",
                        logicalType="date",
                        logicalTypeOptions={"format": "dd/MM/yyyy"}
                    )
                ]
            )
        ]
    )

    expectations = [
        {
            "key": "date_mutation_format",
            "rule": "format",
            "column": "date_mutation",
            "predicate": "to_date(date_mutation, 'dd/MM/yyyy') IS NOT NULL",
        }
    ]

    metrics = compute_metrics(mock_df, contract, expectations=expectations)
    
    # It should have caught the CANNOT_PARSE_TIMESTAMP exception and marked all rows as violated
    assert metrics["row_count"] == 100
    assert metrics["violations.date_mutation_format"] == 100
    assert "errors.date_mutation_format" in metrics
    assert "CANNOT_PARSE_TIMESTAMP" in metrics["errors.date_mutation_format"]


def test_multiple_expectation_rules_resilience():
    # Mock dataframe
    mock_df = MagicMock()
    mock_df.columns = ["id", "date_mutation", "amount"]
    mock_df.count.return_value = 50
    
    # We want one rule to pass (amount > 0) and one to crash (date_mutation format check)
    # mock_df.filter(predicate) is called in compute_metrics.
    # Let's mock filter to return a successful count mock for amount, and raise on date_mutation.
    def mock_filter(predicate_str):
        if "amount" in predicate_str:
            mock_filtered_df = MagicMock()
            mock_filtered_df.count.return_value = 5 # 5 violations for amount
            return mock_filtered_df
        elif "date_mutation" in predicate_str:
            raise RuntimeError("CANNOT_PARSE_TIMESTAMP: date parse error")
        return MagicMock()
        
    mock_df.filter.side_effect = mock_filter

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.multi",
        name="Multi Resilience",
        schema=[]
    )

    expectations = [
        {
            "key": "amount_check",
            "rule": "predicate",
            "column": "amount",
            "predicate": "amount > 0",
        },
        {
            "key": "date_check",
            "rule": "format",
            "column": "date_mutation",
            "predicate": "to_date(date_mutation, 'dd/MM/yyyy') IS NOT NULL",
        }
    ]

    metrics = compute_metrics(mock_df, contract, expectations=expectations)
    
    # The amount_check should run and return 5 violations
    assert metrics["violations.amount_check"] == 5
    assert "errors.amount_check" not in metrics
    
    # The date_check should crash, get caught, and return 50 violations (the total row count) and register the error
    assert metrics["violations.date_check"] == 50
    assert "errors.date_check" in metrics
    assert "CANNOT_PARSE_TIMESTAMP" in metrics["errors.date_check"]


def test_write_executor_resilience_on_collect_observations_crash():
    # A mocked dataframe that throws an exception during count() to simulate read/parse crashes
    mock_df = MagicMock()
    mock_df.sparkSession = MagicMock()
    mock_df.isStreaming = False
    mock_df.columns = ["id", "date_col"]
    mock_df.count.side_effect = RuntimeError("Spark count simulation crash (e.g. CANNOT_PARSE_TIMESTAMP)")

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.obs.crash",
        name="ObsCrash",
        schema=[
            SchemaObject(
                name="obscrash",
                properties=[
                    SchemaProperty(name="id", physicalType="bigint", required=True),
                    SchemaProperty(name="date_col", physicalType="string", required=True),
                ]
            )
        ]
    )

    mock_client = MagicMock()
    mock_client.describe_expectations.return_value = []
    
    plan = ResolvedWritePlan(
        contract=contract,
        contract_id="test.obs.crash",
        contract_version="0.1.0",
        dataset_id="dummy_dataset",
        dataset_version="1.0.0",
        policy=GovernancePolicy(),
    )

    request = {
        "path": "/tmp/dummy",
        "format": "csv",
        "mode": "overwrite",
        "context": {
            "policy": GovernancePolicy(),
            "pipeline_context": {},
        }
    }

    executor = BaseWriteExecutor(
        df=mock_df,
        request=request,
        governance_service=mock_client,
        enforce=False, # strategy handles failure, does not raise immediately
        auto_cast=True,
        plan=plan,
    )

    # Stub evaluate methods to return a valid QualityAssessment
    mock_assessment = MagicMock()
    mock_assessment.validation = ValidationResult(ok=True)
    mock_assessment.status = None
    mock_client.evaluate_dataset.return_value = mock_assessment
    mock_client.evaluate_write_plan.return_value = mock_assessment

    # We mock _execute_write_request to prevent actual writing
    from unittest.mock import patch
    with patch("dc43_integrations.spark.io.base._execute_write_request") as mock_write, \
         patch("dc43_integrations.spark.io.base.apply_contract") as mock_cast:
        mock_write.return_value = (ValidationResult(ok=True), None, [])
        mock_cast.return_value = mock_df
        res = executor.execute()

    # The executor should have caught the exception inside collect_observations,
    # appended the error message to the validation result, and executed successfully.
    assert not res.result.ok
    assert any("Spark execution error" in err for err in res.result.errors)


def test_split_strategy_resilience():
    # Mock aligned_df to throw an exception on count to simulate evaluation crash
    mock_df = MagicMock()
    mock_df.filter.return_value = mock_df
    mock_df.limit.return_value = mock_df
    mock_df.count.side_effect = RuntimeError("Spark filter simulation crash")

    validation = ValidationResult(ok=False, errors=[], warnings=[], metrics={"violations.test": 1})
    context = WriteStrategyContext(
        df=mock_df,
        aligned_df=mock_df,
        contract=None,
        path="/tmp/resilient",
        table=None,
        format="delta",
        options={},
        mode="append",
        validation=validation,
        dataset_id="resilient",
        dataset_version="v1",
        revalidate=lambda _: validation,
        expectation_predicates={"test_predicate": "id > 0"},
        pipeline_context=None,
    )

    strategy = SplitWriteViolationStrategy(write_primary_on_violation=False)
    # The strategy planning shouldn't crash. It should catch the filter exception, log a warning,
    # and return a clean WritePlan with primary=context.base_request() as fallback.
    plan = strategy.plan(context)
    
    assert plan.primary is not None
    assert plan.additional == ()


def test_exact_format_ansi_mode(spark):
    # Enable ANSI mode in Spark session for this test
    original_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        # Create a dataframe with 3 rows: 2 valid dates, 1 invalid date format (yyyy-MM-dd instead of dd/MM/yyyy)
        data = [("01/04/2022",), ("02/04/2022",), ("2022-04-01",)]
        df = spark.createDataFrame(data, ["date_mutation"])

        contract = OpenDataContractStandard(
            version="0.1.0",
            kind="DataContract",
            apiVersion="3.0.2",
            id="test.date_mutation_ansi",
            name="Date Mutation ANSI Mode",
            schema=[
                SchemaObject(
                    name="mutations",
                    properties=[
                        SchemaProperty(
                            name="date_mutation",
                            physicalType="string",
                            logicalType="date",
                            logicalTypeOptions={"format": "dd/MM/yyyy"}
                        )
                    ]
                )
            ]
        )

        from dc43_service_backends.data_quality.backend.predicates import sql_predicate
        from dc43_service_backends.data_quality.backend.engine import ExpectationSpec

        spec = ExpectationSpec(
            key="date_mutation_format",
            rule="exact_format",
            column="date_mutation",
            params={"format": "dd/MM/yyyy"},
        )
        predicate = sql_predicate(spec)

        expectations = [
            {
                "key": "date_mutation_format",
                "rule": "exact_format",
                "column": "date_mutation",
                "predicate": predicate,
            }
        ]

        metrics = compute_metrics(df, contract, expectations=expectations)

        # It should NOT crash, and should report exactly 1 violation (the yyyy-MM-dd row)
        assert metrics["row_count"] == 3
        assert metrics["violations.date_mutation_format"] == 1
        assert "errors.date_mutation_format" not in metrics
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def test_strict_flag_strategy_exact_format_ansi(spark):
    from dc43_integrations.spark.violation_strategy import FlagWriteViolationStrategy, StrictWriteViolationStrategy
    from dc43_service_backends.data_quality.backend.predicates import sql_predicate
    from dc43_service_backends.data_quality.backend.engine import ExpectationSpec

    original_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        # Create a dataframe with 3 rows: 2 valid dates, 1 invalid date format (yyyy-MM-dd instead of dd/MM/yyyy)
        data = [("01/04/2022",), ("02/04/2022",), ("2022-04-01",)]
        df = spark.createDataFrame(data, ["date_mutation"])

        contract = OpenDataContractStandard(
            version="0.1.0",
            kind="DataContract",
            apiVersion="3.0.2",
            id="test.date_mutation_ansi",
            name="Date Mutation ANSI Mode",
            schema=[
                SchemaObject(
                    name="mutations",
                    properties=[
                        SchemaProperty(
                            name="date_mutation",
                            physicalType="string",
                            logicalType="date",
                            logicalTypeOptions={"format": "dd/MM/yyyy"}
                        )
                    ]
                )
            ]
        )

        spec = ExpectationSpec(
            key="date_mutation_format",
            rule="exact_format",
            column="date_mutation",
            params={"format": "dd/MM/yyyy"},
        )
        predicate = sql_predicate(spec)

        # Build context
        validation = ValidationResult(
            ok=False,
            errors=[],
            warnings=[],
            metrics={"violations.date_mutation_format": 1},
        )
        
        context = WriteStrategyContext(
            df=df,
            aligned_df=df,
            contract=contract,
            path=None,
            table="mutations_table",
            format="delta",
            options={},
            mode="append",
            validation=validation,
            dataset_id="test.date_mutation_ansi",
            dataset_version="0.1.0",
            revalidate=lambda _: validation,
            expectation_predicates={"date_mutation_format": predicate},
            pipeline_context=None,
        )

        # Exact strategy from the user's example
        violation_strategy = FlagWriteViolationStrategy(column_name="_donnees_corrompues")
        strict_violation_strategy = StrictWriteViolationStrategy(
            base=violation_strategy,
            failure_message="ECHEC : Des lignes invalides ont été détectées et écrites avec un flag."
        )

        # Plan the strategy
        plan = strict_violation_strategy.plan(context)
        
        # Check that the plan generates the correct primary request
        assert plan.primary is not None
        assert "_donnees_corrompues" in plan.primary.df.columns
        
        # Verify spark execution on the flagged dataframe under ANSI mode
        results = plan.primary.df.collect()
        
        # Row 1 and 2: valid -> _donnees_corrompues should be None
        assert results[0]["_donnees_corrompues"] is None
        assert results[1]["_donnees_corrompues"] is None
        # Row 3: invalid -> _donnees_corrompues should contain ["date_mutation_format"]
        assert results[2]["_donnees_corrompues"] == ["date_mutation_format"]
        
        # Strict strategy validation result factory should return ok=False with the user's custom failure message
        strict_res = plan.result_factory()
        assert not strict_res.ok
        assert "ECHEC : Des lignes invalides ont été détectées et écrites avec un flag." in strict_res.errors

    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


