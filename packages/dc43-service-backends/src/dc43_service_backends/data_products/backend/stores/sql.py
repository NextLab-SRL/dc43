"""SQL-backed data product store implementation."""

from __future__ import annotations

from typing import Sequence
import json

from sqlalchemy import (
    Column,
    DateTime,
    MetaData,
    String,
    Table,
    Text,
    and_,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Engine

from dc43_service_clients.odps import OpenDataProductStandard, as_odps_dict, to_model

from .._shared import _version_key
from .interface import DataProductStore


def _prepare_row(product: OpenDataProductStandard) -> dict[str, object]:
    if not product.version:
        raise ValueError("Data product version is required")
    payload = as_odps_dict(product)
    status = payload.get("status") or product.status or "draft"
    return {
        "data_product_id": product.id,
        "version": product.version,
        "status": status,
        "json": json.dumps(payload, separators=(",", ":")),
    }


class SQLDataProductStore(DataProductStore):
    """Persist ODPS documents in a relational database using SQLAlchemy Core."""

    def __init__(self, engine: Engine, table_name: str = "data_products", schema: str | None = None):
        self.engine = engine
        self.metadata = MetaData(schema=schema)
        self.table = Table(
            table_name,
            self.metadata,
            Column("data_product_id", String(255), primary_key=True, nullable=False),
            Column("version", String(64), primary_key=True, nullable=False),
            Column("status", String(64), nullable=True),
            Column("json", Text, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        )
        self.metadata.create_all(self.engine)

    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        payload = _prepare_row(product)
        with self.engine.begin() as conn:
            stmt = select(self.table.c.data_product_id).where(
                and_(
                    self.table.c.data_product_id == payload["data_product_id"],
                    self.table.c.version == payload["version"],
                )
            )
            exists = conn.execute(stmt).first() is not None
            if exists:
                conn.execute(
                    update(self.table)
                    .where(
                        and_(
                            self.table.c.data_product_id == payload["data_product_id"],
                            self.table.c.version == payload["version"],
                        )
                    )
                    .values(payload)
                )
            else:
                conn.execute(insert(self.table).values(payload))

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        with self.engine.begin() as conn:
            stmt = select(self.table.c.json).where(
                and_(
                    self.table.c.data_product_id == data_product_id,
                    self.table.c.version == version,
                )
            )
            row = conn.execute(stmt).first()
        if not row:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return to_model(json.loads(row[0]))

    def latest(self, data_product_id: str) -> OpenDataProductStandard | None:  # noqa: D401
        versions = self.list_versions(data_product_id)
        if not versions:
            return None
        latest_version = max(versions, key=_version_key)
        return self.get(data_product_id, latest_version)

    def list_versions(self, data_product_id: str) -> Sequence[str]:  # noqa: D401
        with self.engine.begin() as conn:
            stmt = (
                select(self.table.c.version)
                .where(self.table.c.data_product_id == data_product_id)
                .order_by(self.table.c.version)
            )
            rows = conn.execute(stmt).scalars().all()
        # Sort using semantic version helper to ensure consistent ordering
        return sorted((str(value) for value in rows), key=_version_key)

    def list_data_product_ids(self) -> Sequence[str]:  # noqa: D401
        with self.engine.begin() as conn:
            stmt = select(self.table.c.data_product_id).distinct().order_by(self.table.c.data_product_id)
            rows = conn.execute(stmt).scalars().all()
        return [str(value) for value in rows]


__all__ = ["SQLDataProductStore", "create_engine"]
