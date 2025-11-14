"""Testing helpers for synthesising contract-aligned Spark datasets."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable, Iterable, List, Mapping, Sequence, Tuple

from faker import Faker
from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaProperty,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from dc43_integrations.spark.data_quality import spark_type_name
from dc43_integrations.spark.validation import apply_contract


Generator = Callable[[Faker, SchemaProperty], object]

_DECIMAL_PATTERN = re.compile(r"decimal\s*\((\d+)\s*,\s*(\d+)\)", re.IGNORECASE)
_CHAR_PATTERN = re.compile(r"(?:var)?char\s*\((\d+)\)", re.IGNORECASE)


def _list_properties(contract: OpenDataContractStandard) -> List[SchemaProperty]:
    props: List[SchemaProperty] = []
    schema_objects = getattr(contract, "schema_", None) or getattr(contract, "schema", None) or []
    for obj in schema_objects:
        properties = getattr(obj, "properties", None) or []
        for prop in properties:
            if isinstance(prop, SchemaProperty):
                props.append(prop)
            elif isinstance(prop, Mapping):
                try:
                    props.append(SchemaProperty.model_validate(dict(prop)))
                except Exception:
                    continue
    return props


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


_REFERENCE_DATETIME = datetime(2025, 1, 1, 0, 0, 0)


def _date_generator(fake: Faker, _: SchemaProperty):
    start = (_REFERENCE_DATETIME - timedelta(days=30)).date()
    end = _REFERENCE_DATETIME.date()
    return fake.date_between(start_date=start, end_date=end)


def _timestamp_generator(fake: Faker, _: SchemaProperty):
    start = _REFERENCE_DATETIME - timedelta(days=30)
    end = _REFERENCE_DATETIME
    return fake.date_time_between_dates(datetime_start=start, datetime_end=end)


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


def generate_contract_dataset(
    spark: SparkSession,
    contract: OpenDataContractStandard,
    *,
    rows: int = 100,
    faker_locale: str | None = None,
    seed: int | None = None,
    validate_schema: bool = True,
) -> DataFrame:
    """Return a Spark ``DataFrame`` aligned to ``contract``."""

    if rows <= 0:
        raise ValueError("rows must be a positive integer")

    fake = Faker(faker_locale)
    if seed is not None:
        fake.seed_instance(seed)

    fields: List[SchemaProperty] = [prop for prop in _list_properties(contract) if prop.name]
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

    if validate_schema:
        apply_contract(df, contract)

    return df


__all__ = ["generate_contract_dataset"]
