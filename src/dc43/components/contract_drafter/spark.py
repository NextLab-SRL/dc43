"""Helpers to generate ODCS drafts from Spark observations."""

from __future__ import annotations

from typing import Any, Dict, Optional

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore

from open_data_contract_standard.model import (  # type: ignore
    CustomProperty,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43.components.data_quality import odcs_type_name_from_spark
from dc43.odcs import contract_identity
from dc43.versioning import SemVer


def draft_from_dataframe(
    df: DataFrame,
    base_contract: OpenDataContractStandard,
    *,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
    dq_feedback: Optional[Dict[str, Any]] = None,
) -> OpenDataContractStandard:
    """Create a draft ODCS document based on a Spark DataFrame."""

    props = []
    for field in df.schema.fields:  # type: ignore[attr-defined]
        odcs_type = odcs_type_name_from_spark(field.dataType)
        props.append(
            SchemaProperty(
                name=field.name,
                physicalType=str(odcs_type),
                required=not field.nullable,
            )
        )

    contract_id, current_version = contract_identity(base_contract)
    semver = SemVer.parse(current_version)
    target_bump = "minor" if bump not in ("major", "patch") else bump
    new_version = str(semver.bump(target_bump))

    custom_props = list(base_contract.customProperties or [])
    custom_props.append(CustomProperty(property="draft", value=True))
    custom_props.append(CustomProperty(property="base_version", value=current_version))
    custom_props.append(
        CustomProperty(
            property="provenance",
            value={"dataset_id": dataset_id, "dataset_version": dataset_version},
        )
    )
    if dq_feedback:
        custom_props.append(CustomProperty(property="dq_feedback", value=dq_feedback))

    schema_name = contract_id
    if base_contract.schema_:
        first = base_contract.schema_[0]
        schema_name = first.name or contract_id

    servers = base_contract.servers
    if dataset_id:
        fmt = data_format
        if not fmt and base_contract.servers:
            fmt = base_contract.servers[0].format
        if dataset_id.startswith("path:"):
            servers = [Server(server="local", type="filesystem", path=dataset_id[5:], format=fmt)]
        elif dataset_id.startswith("table:"):
            servers = [Server(server="local", dataset=dataset_id[6:], format=fmt)]

    draft = OpenDataContractStandard(
        version=new_version,
        kind=base_contract.kind,
        apiVersion=base_contract.apiVersion,
        id=contract_id,
        name=base_contract.name or contract_id,
        description=base_contract.description,
        status="draft",
        schema=[SchemaObject(name=schema_name, properties=props)],
        servers=servers,
        customProperties=custom_props,
    )
    return draft


__all__ = ["draft_from_dataframe"]
