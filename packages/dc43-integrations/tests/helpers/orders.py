from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

from open_data_contract_standard.model import (
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)


DEFAULT_ORDERS_ROWS: Sequence[tuple[int, int, datetime, float, str]] = (
    (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "USD"),
)


def build_orders_contract(base_path: Path | str, *, fmt: str = "parquet") -> OpenDataContractStandard:
    """Return the standard orders contract used across Spark and DLT scenarios."""

    return OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                    SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                    SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                    SchemaProperty(
                        name="amount",
                        physicalType="double",
                        required=True,
                        quality=[DataQuality(mustBeGreaterThan=0.0)],
                    ),
                    SchemaProperty(
                        name="currency",
                        physicalType="string",
                        required=True,
                        quality=[DataQuality(rule="enum", mustBe=["EUR", "USD"])],
                    ),
                ],
            )
        ],
        servers=[Server(server="local", type="filesystem", path=str(base_path), format=fmt)],
    )


def materialise_orders(
    spark,
    destination: Path,
    *,
    rows: Iterable[tuple[int, int, datetime, float, str]] | None = None,
) -> Path:
    """Persist the orders contract dataset to ``destination`` and return the path."""

    destination.mkdir(parents=True, exist_ok=True)
    data = list(rows) if rows is not None else list(DEFAULT_ORDERS_ROWS)
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    df.write.mode("overwrite").format("parquet").save(str(destination))
    return destination
