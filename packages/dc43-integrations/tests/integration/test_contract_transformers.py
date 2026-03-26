import pytest
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_integrations.spark.io.transformers import (
    ContractBasedTransformer,
    apply_contract_transformers,
    resolve_transformer,
)


def my_dummy_transformer(df: DataFrame, contract: OpenDataContractStandard) -> DataFrame:
    return df.withColumn("contract_version", F.lit(contract.datasetVersion))

def my_second_transformer(df: DataFrame, contract: OpenDataContractStandard) -> DataFrame:
    return df.withColumn("transformer_two", F.lit(True))

def my_udf_transformer(df: DataFrame, contract: OpenDataContractStandard) -> DataFrame:
    # Ensure no serialization issues by extracting primitive value before UDF definition
    domain_val = contract.datasetDomain
    
    @F.udf(returnType="string")
    def add_domain(val: str) -> str:
        return f"{val}_{domain_val}"
        
    if "name" in df.columns:
        return df.withColumn("name", add_domain(F.col("name")))
    return df

def an_error_transformer(df: DataFrame, contract: OpenDataContractStandard) -> DataFrame:
    raise ValueError("Transformer failed")


def test_resolve_transformer_callable():
    func = resolve_transformer(my_dummy_transformer)
    assert func is my_dummy_transformer


def test_resolve_transformer_string():
    # Attempt to resolve the transformer built in this very test module
    func = resolve_transformer(f"{__name__}:my_dummy_transformer")
    assert func is my_dummy_transformer


def test_resolve_transformer_invalid_string():
    with pytest.raises(ValueError, match="Invalid transformer reference format"):
        resolve_transformer("not_a_valid_format")
        
    with pytest.raises(ValueError, match="not found in module"):
        resolve_transformer(f"{__name__}:does_not_exist")


def test_apply_contract_transformers(spark):
    df = spark.createDataFrame([{"id": 1}])
    
    from unittest.mock import MagicMock
    contract = MagicMock(spec=OpenDataContractStandard)
    contract.datasetDomain = "test"
    contract.datasetName = "test"
    contract.datasetVersion = "1.0.0"

    result = apply_contract_transformers(df, contract, [my_dummy_transformer])
    assert "contract_version" in result.columns
    assert result.first().contract_version == "1.0.0"


def test_apply_multiple_contract_transformers(spark):
    df = spark.createDataFrame([{"id": 1, "name": "foo"}])
    
    from unittest.mock import MagicMock
    contract = MagicMock(spec=OpenDataContractStandard)
    contract.datasetDomain = "testdomain"
    contract.datasetName = "test"
    contract.datasetVersion = "1.0.0"

    # Apply a chain of 3 transformers
    result = apply_contract_transformers(df, contract, [
        my_dummy_transformer,
        my_second_transformer,
        my_udf_transformer
    ])
    
    # Assert all transformations executed in the correct order
    row = result.first()
    assert "contract_version" in result.columns
    assert row.contract_version == "1.0.0"
    assert "transformer_two" in result.columns
    assert row.transformer_two is True
    # Verify the UDF correctly captured the closure without serialization errors
    assert row.name == "foo_testdomain"


def test_apply_contract_transformers_empty(spark):
    df = spark.createDataFrame([{"id": 1}])
    
    from unittest.mock import MagicMock
    contract = MagicMock(spec=OpenDataContractStandard)
    contract.datasetDomain = "test"
    contract.datasetName = "test"
    contract.datasetVersion = "1.0.0"

    result = apply_contract_transformers(df, contract, [])
    assert "contract_version" not in result.columns
    assert result is df


def test_apply_contract_transformers_error(spark):
    df = spark.createDataFrame([{"id": 1}])
    
    from unittest.mock import MagicMock
    contract = MagicMock(spec=OpenDataContractStandard)
    contract.datasetDomain = "test"
    contract.datasetName = "test"
    contract.datasetVersion = "1.0.0"

    with pytest.raises(RuntimeError, match="Data contract transformer failed"):
        apply_contract_transformers(df, contract, [an_error_transformer])
