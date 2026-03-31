import pytest
import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_integrations.spark.io.interceptors import (
    BaseGovernanceInterceptor,
    InterceptorContext,
    resolve_interceptor,
    get_global_interceptors,
)


class MyDummyInterceptor(BaseGovernanceInterceptor):
    def pre_write(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        return df.withColumn("contract_version", F.lit(context.contract.datasetVersion))

class MyErrorInterceptor(BaseGovernanceInterceptor):
    def pre_write(self, context: InterceptorContext, df: DataFrame) -> DataFrame:
        raise ValueError("Interceptor failed")


def test_resolve_interceptor_instance():
    instance = MyDummyInterceptor()
    resolved = resolve_interceptor(instance)
    assert resolved is instance


def test_resolve_interceptor_string():
    # Attempt to resolve the interceptor built in this very test module
    resolved = resolve_interceptor(f"{__name__}:MyDummyInterceptor")
    assert isinstance(resolved, MyDummyInterceptor)


def test_resolve_interceptor_invalid_string():
    with pytest.raises(ValueError, match="Invalid interceptor reference format"):
        resolve_interceptor("not_a_valid_format")
        
    with pytest.raises(ValueError, match="not found in module"):
        resolve_interceptor(f"{__name__}:DoesNotExist")


def test_get_global_interceptors_from_env(monkeypatch):
    monkeypatch.setenv("DC43_GOVERNANCE_INTERCEPTORS", "my_module.MyInterceptor, other_module.OtherInterceptor")
    interceptors = get_global_interceptors()
    assert len(interceptors) == 2
    assert interceptors[0] == "my_module.MyInterceptor"
    assert "other_module.OtherInterceptor" in interceptors


def test_get_global_interceptors_from_spark(spark):
    spark.conf.set("dc43.governance.interceptors.write", "spark_module.MySparkInterceptor")
    interceptors = get_global_interceptors(spark=spark, operation="write")
    assert len(interceptors) == 1
    assert interceptors[0] == "spark_module.MySparkInterceptor"
    spark.conf.unset("dc43.governance.interceptors.write")
