from __future__ import annotations

"""Helpers that materialise random datasets matching ODCS contracts."""

from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple
import re
from urllib.parse import urlparse

from faker import Faker
from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaProperty,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from dc43_service_backends.core.odcs import list_properties
from dc43_service_backends.governance.backend import LocalGovernanceServiceBackend
from dc43_service_clients.contracts import ContractServiceClient
from dc43_service_clients.data_quality import LocalDataQualityServiceClient
from dc43_service_clients.governance import GovernanceServiceClient
from dc43_service_clients.governance.client.local import LocalGovernanceServiceClient
from dc43_integrations.spark.data_quality import spark_type_name
from dc43_integrations.spark.io import (
    ContractVersionLocator,
    DatasetLocatorStrategy,
    write_with_contract,
)


Generator = Callable[[Faker, SchemaProperty], object]

_DECIMAL_PATTERN = re.compile(r"decimal\s*\((\d+)\s*,\s*(\d+)\)", re.IGNORECASE)
_CHAR_PATTERN = re.compile(r"(?:var)?char\s*\((\d+)\)", re.IGNORECASE)


_SPARK_TYPES: dict[str, T.DataType] = {
    "string": T.StringType(),
    "bigint": T.LongType(),
    "long": T.LongType(),
    "int": T.IntegerType(),
    "integer": T.IntegerType(),
    "smallint": T.ShortType(),
    "tinyint": T.ByteType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "boolean": T.BooleanType(),
    "bool": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
    "binary": T.BinaryType(),
}


def _enum_values(field: SchemaProperty) -> Sequence[object]:
    values: List[object] = []
    for rule in getattr(field, "quality", None) or []:
        rule_name = str(getattr(rule, "rule", "")).lower()
        if rule_name == "enum":
            raw = getattr(rule, "mustBe", None) or getattr(rule, "values", None)
            if raw is None:
                continue
            if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes, bytearray)):
                values.extend(raw)
            else:
                values.append(raw)
    return values


def _string_length_hint(type_hint: str) -> int | None:
    match = _CHAR_PATTERN.search(type_hint)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _string_generator(fake: Faker, field: SchemaProperty) -> str:
    choices = _enum_values(field)
    if choices:
        return str(fake.random_element(choices))
    type_hint = str(field.physicalType or field.logicalType or "").lower()
    max_len = _string_length_hint(type_hint)
    if max_len is not None:
        return fake.pystr(min_chars=1, max_chars=max(1, min(max_len, 32)))
    return fake.pystr(min_chars=5, max_chars=24)


def _integer_generator(bounds: Tuple[int, int]) -> Generator:
    def _gen(fake: Faker, _: SchemaProperty) -> int:
        return int(fake.random_int(min=bounds[0], max=bounds[1]))

    return _gen


def _floating_generator(right_digits: int = 6) -> Generator:
    def _gen(fake: Faker, _: SchemaProperty) -> float:
        return float(fake.pyfloat(left_digits=6, right_digits=right_digits))

    return _gen


def _boolean_generator(fake: Faker, _: SchemaProperty) -> bool:
    return bool(fake.pybool())


def _date_generator(fake: Faker, _: SchemaProperty):
    return fake.date_between(start_date="-30d", end_date="today")


def _timestamp_generator(fake: Faker, _: SchemaProperty):
    return fake.date_time_between(start_date="-30d", end_date="now")


def _binary_generator(fake: Faker, _: SchemaProperty) -> bytes:
    return bytes(fake.binary(length=16))


def _decimal_spec(type_hint: str) -> tuple[T.DecimalType, Generator]:
    match = _DECIMAL_PATTERN.search(type_hint)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2))
    else:
        precision, scale = 18, 6
    precision = max(1, precision)
    scale = max(0, min(scale, precision))
    dtype = T.DecimalType(precision, scale)

    left_digits = max(1, precision - scale)

    def _gen(fake: Faker, _: SchemaProperty) -> Decimal:
        return fake.pydecimal(left_digits=left_digits, right_digits=scale, positive=False)

    return dtype, _gen


_INT_BOUNDS: dict[str, Tuple[int, int]] = {
    "tinyint": (-128, 127),
    "smallint": (-32768, 32767),
    "int": (-2_147_483_648, 2_147_483_647),
    "integer": (-2_147_483_648, 2_147_483_647),
    "bigint": (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "long": (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
}


_GENERATORS: dict[str, Generator] = {
    "string": _string_generator,
    "binary": _binary_generator,
    "boolean": _boolean_generator,
    "bool": _boolean_generator,
    "date": _date_generator,
    "timestamp": _timestamp_generator,
    "double": _floating_generator(),
    "float": _floating_generator(4),
}


def _normalise_type_hint(raw: str) -> str:
    return raw.strip().lower()


def _field_spec(field: SchemaProperty) -> tuple[T.DataType, Generator]:
    raw_type = str(field.physicalType or field.logicalType or "string")
    normalized = _normalise_type_hint(raw_type)

    if normalized.startswith("decimal"):
        return _decimal_spec(normalized)

    base = normalized.split("(", 1)[0]
    base = base.split("<", 1)[0]
    canonical = spark_type_name(base)
    dtype = _SPARK_TYPES.get(canonical)
    if dtype is None:
        return T.StringType(), _string_generator

    if canonical in _INT_BOUNDS:
        return dtype, _integer_generator(_INT_BOUNDS[canonical])

    generator = _GENERATORS.get(canonical)
    if generator:
        return dtype, generator

    return dtype, _string_generator


class _InlineContractService(ContractServiceClient):
    """Contract service that exposes a single in-memory contract."""

    def __init__(self, contract: OpenDataContractStandard) -> None:
        self._contract = contract

    def put(self, contract: OpenDataContractStandard) -> None:  # pragma: no cover - unused
        self._validate_identity(contract.id, contract.version)
        self._contract = contract

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        self._validate_identity(contract_id, contract_version)
        return self._contract

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        if contract_id != self._contract.id:
            return None
        return self._contract

    def list_versions(self, contract_id: str) -> Sequence[str]:
        if contract_id != self._contract.id:
            return []
        version = self._contract.version
        return [version] if version else []

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:  # pragma: no cover - not triggered in helper
        self._validate_identity(contract_id, contract_version)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None = None,
    ) -> Optional[str]:  # pragma: no cover - not triggered in helper
        return None

    def _validate_identity(self, contract_id: str, contract_version: str | None) -> None:
        if contract_id != self._contract.id:
            raise ValueError(
                f"Requested contract {contract_id}:{contract_version} does not match "
                f"generated contract {self._contract.id}:{self._contract.version}"
            )
        version = self._contract.version
        if version and contract_version and contract_version != version:
            raise ValueError(
                f"Requested contract version {contract_version} does not match {version}"
            )


class _CachingLocator(DatasetLocatorStrategy):
    """Wrap a locator so repeated writes reuse the same resolution."""

    def __init__(self, base: DatasetLocatorStrategy) -> None:
        self._base = base
        self._write_resolution: object | None = None

    @property
    def write_resolution(self) -> object | None:
        return self._write_resolution

    def for_read(
        self,
        *,
        contract: OpenDataContractStandard | None,
        spark: SparkSession,
        format: str | None,
        path: str | None,
        table: str | None,
    ) -> object:
        return self._base.for_read(
            contract=contract,
            spark=spark,
            format=format,
            path=path,
            table=table,
        )

    def for_write(
        self,
        *,
        contract: OpenDataContractStandard | None,
        df: DataFrame,
        format: str | None,
        path: str | None,
        table: str | None,
    ) -> object:
        if self._write_resolution is None:
            self._write_resolution = self._base.for_write(
                contract=contract,
                df=df,
                format=format,
                path=path,
                table=table,
            )
        return self._write_resolution


def _storage_path_from_resolution(raw: str) -> Path | str:
    """Return an appropriate representation for a resolved storage path."""

    parsed = urlparse(raw)
    if parsed.scheme and (parsed.netloc or raw.startswith(f"{parsed.scheme}:/")):
        # Preserve remote/storage-specific URIs such as s3://bucket or dbfs:/mnt/data
        return raw

    return Path(raw)


def generate_contract_dataset(
    spark: SparkSession,
    contract: OpenDataContractStandard,
    *,
    rows: int = 100,
    dataset_version: str | None = None,
    path: str | Path | None = None,
    faker_locale: str | None = None,
    seed: int | None = None,
    mode: str = "overwrite",
    governance_service: GovernanceServiceClient | None = None,
    dataset_locator: DatasetLocatorStrategy | None = None,
) -> tuple[DataFrame, Path | str]:
    """Generate a Spark ``DataFrame`` aligned to ``contract`` and persist it."""

    if rows <= 0:
        raise ValueError("rows must be a positive integer")

    fake = Faker(faker_locale)
    if seed is not None:
        fake.seed_instance(seed)

    fields: List[SchemaProperty] = [prop for prop in list_properties(contract) if prop.name]
    if not fields:
        raise ValueError("Contract does not expose any schema properties")

    struct_fields: List[T.StructField] = []
    generators: List[Generator] = []
    for field in fields:
        dtype, generator = _field_spec(field)
        struct_fields.append(T.StructField(field.name, dtype, nullable=not bool(field.required)))
        generators.append(generator)

    schema = T.StructType(struct_fields)
    data: List[tuple[object, ...]] = []
    for _ in range(rows):
        row = tuple(generator(fake, field) for generator, field in zip(generators, fields))
        data.append(row)

    df = spark.createDataFrame(data, schema=schema)

    if not contract.id:
        raise ValueError("Contract must define an identifier")
    if not contract.version and not dataset_version:
        raise ValueError("Contract must define a version or dataset_version must be provided")

    target_version = dataset_version or contract.version or "generated"
    locator = dataset_locator or ContractVersionLocator(dataset_version=target_version)
    caching_locator = _CachingLocator(locator)

    dq_client = LocalDataQualityServiceClient()
    service = _InlineContractService(contract)
    governance_client = governance_service or LocalGovernanceServiceClient(
        LocalGovernanceServiceBackend(
            contract_client=service,
            dq_client=dq_client,
        )
    )

    expected_version = f"=={contract.version}" if contract.version else None
    resolved_path = str(path) if path is not None else None

    write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=expected_version,
        path=resolved_path,
        mode=mode,
        enforce=False,
        data_quality_service=dq_client,
        governance_service=governance_client,
        dataset_locator=caching_locator,
    )

    resolution = caching_locator.write_resolution
    if resolution is None:
        raise RuntimeError("Failed to resolve dataset path during write")

    resolved_path_value = getattr(resolution, "path", None)
    if not resolved_path_value:
        raise ValueError("Contract server does not define a storage path")

    storage_path = _storage_path_from_resolution(resolved_path_value)

    return df, storage_path


__all__ = ["generate_contract_dataset"]
