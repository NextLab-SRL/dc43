"""Shared helpers for Databricks Delta versioning demos.

The notebook-friendly utilities defined here mirror the previous script-based
examples so batch and streaming demos can import a single module. Each helper is
safe to call from Databricks notebooks as well as plain Python sessions (for
instance when testing locally).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from open_data_contract_standard.model import (
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_integrations.spark.io import (
    ContractFirstDatasetLocator,
    ContractVersionLocator,
    GovernanceSparkWriteRequest,
    write_stream_with_governance,
    write_with_governance,
)
from dc43_service_clients.data_products import DataProductServiceClient
from dc43_service_clients.governance import GovernanceServiceClient
from dc43_service_clients.governance.models import DatasetContractStatus
from dc43_service_clients.contracts import ContractServiceClient


@dataclass
class VersionedWriteSpec:
    """Describe the data that accompanies a specific contract revision."""

    contract: OpenDataContractStandard
    rows: Iterable[Mapping[str, object]]
    dataset_version: str | None = None


def _ensure_dataset_version(
    spec: VersionedWriteSpec,
    clock: Callable[[], str] | None = None,
) -> str:
    """Return the dataset version for ``spec``, generating one when missing."""

    if spec.dataset_version:
        return spec.dataset_version
    generator = clock or ContractFirstDatasetLocator().clock
    spec.dataset_version = generator()
    return spec.dataset_version


def build_contract(
    *,
    version: str,
    contract_id: str,
    table_name: str,
    catalog: str,
    schema: str,
    allowed_currencies: Iterable[str],
    include_discount: bool,
) -> OpenDataContractStandard:
    """Return a contract definition pinned to a Unity Catalog Delta table."""

    properties = [
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
            quality=[DataQuality(rule="enum", mustBe=list(allowed_currencies))],
        ),
    ]
    if include_discount:
        properties.append(
            SchemaProperty(
                name="discount_rate",
                physicalType="double",
                required=False,
                quality=[
                    DataQuality(mustBeGreaterOrEqualTo=0.0),
                    DataQuality(mustBeLessOrEqualTo=1.0),
                ],
            )
        )

    return OpenDataContractStandard(
        version=version,
        kind="DataContract",
        apiVersion="3.0.2",
        id=contract_id,
        name="Orders",
        status="active",
        description=Description(usage="Governed orders fact table"),
        schema=[SchemaObject(name="orders", properties=properties)],
        servers=[
            Server(
                server="unity-catalog",
                type="catalog",
                catalog=catalog,
                schema=schema,
                dataset=table_name,
                format="delta",
            )
        ],
    )


def make_dataframe(
    spark: SparkSession,
    spec: VersionedWriteSpec,
    *,
    has_discount: bool,
) -> DataFrame:
    """Materialise a dataframe for ``spec`` using Spark types."""

    rows = [dict(row) for row in spec.rows]
    base_columns = ["order_id", "customer_id", "order_ts", "amount", "currency"]
    include_discount = has_discount or any("discount_rate" in row for row in rows)
    columns = list(base_columns)
    if include_discount:
        columns.append("discount_rate")
    schema_map = {
        "order_id": "long",
        "customer_id": "long",
        "order_ts": "string",
        "amount": "double",
        "currency": "string",
        "discount_rate": "double",
    }
    schema = ", ".join(f"{column} {schema_map[column]}" for column in columns)
    ordered_rows = [tuple(row.get(column) for column in columns) for row in rows]
    df = spark.createDataFrame(ordered_rows, schema)
    df = df.withColumn("order_ts", F.to_timestamp("order_ts"))
    return df


def ensure_active_data_product(
    *,
    data_product_service: DataProductServiceClient,
    data_product_id: str,
    port_name: str,
    contract: OpenDataContractStandard,
    physical_location: str,
) -> None:
    """Create or evolve the output port so it references ``contract``."""

    registration = data_product_service.register_output_port(
        data_product_id=data_product_id,
        port_name=port_name,
        contract_id=contract.id,
        contract_version=contract.version,
    )
    product = registration.product
    product.status = "active"
    if not product.version:
        product.version = "1.0.0"
    elif "-draft" in product.version:
        # Draft registrations are not usable for governed writes, so normalise the
        # auto-evolved identifier back to the release version immediately.
        product.version = product.version.split("-draft", 1)[0]
    if not product.name:
        product.name = "Orders analytics"
    if not product.description:
        product.description = {"usage": "Orders fact outputs"}
    port = product.find_output_port(port_name)
    if port is not None:
        custom_props = list(getattr(port, "custom_properties", []))
        location_prop = {
            "property": "dc43.output.physical_location",
            "value": physical_location,
        }
        if location_prop not in custom_props:
            custom_props.append(location_prop)
        port.custom_properties = custom_props  # type: ignore[attr-defined]
    data_product_service.put(product)
    return product


def contract_has_discount(contract: OpenDataContractStandard) -> bool:
    for obj in contract.schema_ or []:
        for prop in obj.properties or []:
            if prop.name == "discount_rate":
                return True
    return False


def register_contracts(
    contract_service: ContractServiceClient,
    contracts: Iterable[OpenDataContractStandard],
) -> None:
    for contract in contracts:
        contract_service.put(contract)


def write_dataset_version(
    *,
    spark: SparkSession,
    spec: VersionedWriteSpec,
    dataset_id: str,
    data_product_id: str,
    output_port: str,
    data_product_version: str | None = None,
    table_name: str,
    governance_service: GovernanceServiceClient,
    enforce: bool,
):
    """Write a governed batch dataset version for ``spec``."""

    df = make_dataframe(
        spark,
        spec,
        has_discount=contract_has_discount(spec.contract),
    )
    dataset_version = _ensure_dataset_version(spec)

    binding = {
        "data_product": data_product_id,
        "port_name": output_port,
    }
    if data_product_version:
        binding["data_product_version"] = data_product_version

    validation, status = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context={
                "contract": {
                    "contract_id": spec.contract.id,
                    "contract_version": spec.contract.version,
                },
                "output_binding": binding,
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
            },
            table=table_name,
            format="delta",
            mode="overwrite",
            options={"mergeSchema": "true"},
            dataset_locator=ContractVersionLocator(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            ),
        ),
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=True,
        return_status=True,
    )
    return validation, status


def collect_status_matrix(
    governance_service: GovernanceServiceClient,
    *,
    dataset_id: str,
) -> list[DatasetContractStatus]:
    """Return sorted compatibility entries for ``dataset_id``."""

    entries = list(
        governance_service.get_status_matrix(
            dataset_id=dataset_id,
        )
    )
    entries.sort(
        key=lambda entry: (
            entry.dataset_version,
            entry.contract_version,
        )
    )
    return entries


def render_markdown_matrix(entries: Iterable[DatasetContractStatus]) -> str:
    """Pivot ``entries`` into a Markdown table."""

    matrix: dict[tuple[str, str], DatasetContractStatus] = {}
    dataset_version_set: set[str] = set()
    contract_version_set: set[str] = set()
    for entry in entries:
        dataset_version_set.add(entry.dataset_version)
        contract_version_set.add(entry.contract_version)
        matrix[(entry.dataset_version, entry.contract_version)] = entry

    dataset_versions = ContractVersionLocator._sorted_versions(dataset_version_set)
    contract_versions = ContractVersionLocator._sorted_versions(contract_version_set)

    if not dataset_versions or not contract_versions:
        return "No compatibility records recorded yet."

    header = (
        "| Dataset version | "
        + " | ".join(f"Contract {version}" for version in contract_versions)
        + " |"
    )
    separator = "|" + " --- |" * (len(contract_versions) + 1)
    rows = [header, separator]
    for dataset_version in dataset_versions:
        cells = [dataset_version]
        for contract_version in contract_versions:
            entry = matrix.get((dataset_version, contract_version))
            if entry is None or entry.status is None:
                cells.append("")
            else:
                status = entry.status.status
                reason = entry.status.reason
                if reason:
                    cells.append(f"{status} ({reason})")
                else:
                    cells.append(status)
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def describe_delta_history(
    spark: SparkSession, table: str
) -> list[Mapping[str, object]]:
    """Return a list of DESCRIBE HISTORY records for ``table``."""

    try:
        history = spark.sql(f"DESCRIBE HISTORY {table}").collect()
    except Exception as exc:  # pragma: no cover - best effort helper
        print(f"Failed to load Delta history for {table}: {exc}")
        return []
    return [row.asDict(recursive=True) for row in history]


def make_streaming_dataframe(
    spark: SparkSession,
    spec: VersionedWriteSpec,
    *,
    has_discount: bool,
) -> DataFrame:
    """Return a streaming dataframe that emits ``spec`` rows once."""

    rows = [dict(row) for row in spec.rows]
    if not rows:
        raise ValueError("Streaming demo requires at least one row per dataset version")

    base_columns = ["order_id", "customer_id", "order_ts", "amount", "currency"]
    include_discount = has_discount or any("discount_rate" in row for row in rows)
    columns = list(base_columns)
    if include_discount:
        columns.append("discount_rate")

    schema_map = {
        "row_index": "long",
        "order_id": "long",
        "customer_id": "long",
        "order_ts": "string",
        "amount": "double",
        "currency": "string",
        "discount_rate": "double",
    }

    indexed_rows: list[tuple[object, ...]] = []
    ordered_columns: list[str] = ["row_index", *columns]
    for index, row in enumerate(rows):
        payload = [index]
        for column in columns:
            payload.append(row.get(column))
        indexed_rows.append(tuple(payload))

    schema = ", ".join(f"{column} {schema_map[column]}" for column in ordered_columns)
    lookup_df = spark.createDataFrame(indexed_rows, schema)

    rate_df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", max(len(rows), 1))
        .option("numRows", len(rows))
        .load()
        .withColumnRenamed("value", "row_index")
        .drop("timestamp")
    )

    joined = rate_df.join(F.broadcast(lookup_df), on="row_index", how="inner")
    df = joined.drop("row_index").select(columns)
    df = df.withColumn("order_ts", F.to_timestamp("order_ts"))
    return df


def extract_streaming_queries(results: Sequence[object]) -> list[object]:
    """Collect streaming query handles from validation/status payloads."""

    handles: list[object] = []
    for result in results:
        details = getattr(result, "details", None)
        if not isinstance(details, Mapping):
            continue
        queries = details.get("streaming_queries")
        if isinstance(queries, Iterable):
            for handle in queries:
                if handle and handle not in handles:
                    handles.append(handle)
    return handles


def drain_streaming_queries(handles: Iterable[object]) -> None:
    """Process any remaining micro-batches and stop streaming handles."""

    for handle in handles:
        if handle is None:
            continue
        try:
            process = getattr(handle, "processAllAvailable", None)
            if callable(process):
                process()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            print(f"Failed to process remaining data for {handle}: {exc}")
        try:
            stop = getattr(handle, "stop", None)
            if callable(stop):
                stop()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            print(f"Failed to stop streaming query {handle}: {exc}")
        try:
            await_termination = getattr(handle, "awaitTermination", None)
            if callable(await_termination):
                await_termination()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            print(f"Failed to await termination for {handle}: {exc}")


def write_streaming_dataset_version(
    *,
    spark: SparkSession,
    spec: VersionedWriteSpec,
    dataset_id: str,
    data_product_id: str,
    output_port: str,
    data_product_version: str | None = None,
    table_name: str,
    governance_service: GovernanceServiceClient,
    enforce: bool,
    checkpoint_root: str,
):
    """Execute a single streaming write for ``spec`` and stop the query."""

    df = make_streaming_dataframe(
        spark,
        spec,
        has_discount=contract_has_discount(spec.contract),
    )

    dataset_version = _ensure_dataset_version(spec)
    checkpoint_path = f"{checkpoint_root.rstrip('/')}/{dataset_version}"
    binding = {
        "data_product": data_product_id,
        "port_name": output_port,
    }
    if data_product_version:
        binding["data_product_version"] = data_product_version

    validation, status = write_stream_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context={
                "contract": {
                    "contract_id": spec.contract.id,
                    "contract_version": spec.contract.version,
                },
                "output_binding": binding,
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
            },
            table=table_name,
            format="delta",
            mode="append",
            options={"checkpointLocation": checkpoint_path},
            dataset_locator=ContractVersionLocator(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            ),
        ),
        governance_service=governance_service,
        enforce=enforce,
        auto_cast=True,
        return_status=True,
    )

    return validation, status
