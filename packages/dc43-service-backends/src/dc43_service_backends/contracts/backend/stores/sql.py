"""SQL-backed contract store implementation."""

from __future__ import annotations

from typing import List
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

from ._sql_common import prepare_contract_row
from .interface import ContractStore
from dc43_service_backends.core.odcs import to_model
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


class SQLContractStore(ContractStore):
    """Persist contracts in a relational database using SQLAlchemy Core."""

    def __init__(self, engine: Engine, table_name: str = "contracts", schema: str | None = None):
        self.engine = engine
        self.metadata = MetaData(schema=schema)
        self.table = Table(
            table_name,
            self.metadata,
            Column("contract_id", String(255), primary_key=True, nullable=False),
            Column("version", String(64), primary_key=True, nullable=False),
            Column("name", String(255), nullable=False),
            Column("description", Text),
            Column("json", Text, nullable=False),
            Column("fingerprint", String(64), nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        )
        self.metadata.create_all(self.engine)

    def put(self, contract: OpenDataContractStandard) -> None:
        """Insert or update a contract document."""

        cid, ver, payload = prepare_contract_row(contract)

        with self.engine.begin() as conn:
            stmt = select(self.table.c.contract_id).where(
                and_(
                    self.table.c.contract_id == cid,
                    self.table.c.version == ver,
                )
            )
            exists = conn.execute(stmt).first() is not None
            if exists:
                conn.execute(
                    update(self.table)
                    .where(
                        and_(
                            self.table.c.contract_id == cid,
                            self.table.c.version == ver,
                        )
                    )
                    .values(payload)
                )
            else:
                conn.execute(insert(self.table).values(payload))

    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        """Fetch a contract from the relational table and return it as a model."""

        with self.engine.begin() as conn:
            stmt = select(self.table.c.json).where(
                and_(
                    self.table.c.contract_id == contract_id,
                    self.table.c.version == version,
                )
            )
            row = conn.execute(stmt).first()

        if not row:
            raise KeyError(f"Contract {contract_id}:{version} not found")

        return to_model(json.loads(row[0]))

    def list_contracts(self) -> List[str]:
        with self.engine.begin() as conn:
            stmt = select(self.table.c.contract_id).distinct().order_by(self.table.c.contract_id)
            rows = conn.execute(stmt).scalars().all()
        return list(rows)

    def list_versions(self, contract_id: str) -> List[str]:
        with self.engine.begin() as conn:
            stmt = (
                select(self.table.c.version)
                .where(self.table.c.contract_id == contract_id)
                .order_by(self.table.c.version)
            )
            rows = conn.execute(stmt).scalars().all()
        return list(rows)

__all__ = ["SQLContractStore", "create_engine"]
