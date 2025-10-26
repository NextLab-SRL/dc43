from __future__ import annotations

from pathlib import Path

from open_data_contract_standard.model import (  # type: ignore
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_integrations.spark.io import ContractVersionLocator, StaticDatasetLocator
from dc43_integrations.testing import generate_contract_dataset
from dc43_integrations.testing.datasets import _storage_path_from_resolution
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance.models import QualityAssessment

from .helpers.orders import build_orders_contract


def test_generate_contract_dataset_materialises_storage(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders", fmt="parquet")

    df, path = generate_contract_dataset(spark, contract, rows=5, seed=123)

    assert df.count() == 5
    assert set(df.columns) == {"order_id", "customer_id", "order_ts", "amount", "currency"}
    assert path == Path(tmp_path / "orders" / contract.version)
    assert any(path.iterdir())

    stored = spark.read.format("parquet").load(str(path))
    assert stored.count() == 5


def test_generate_contract_dataset_resolves_relative_paths_and_seed(spark, tmp_path):
    base_dir = tmp_path / "data"
    base_dir.mkdir()

    contract = OpenDataContractStandard(
        version="1.2.3",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.random",
        name="Random dataset",
        description=Description(usage="Generated dataset"),
        schema=[
            SchemaObject(
                name="random",
                properties=[
                    SchemaProperty(name="identifier", physicalType="bigint", required=True),
                    SchemaProperty(name="active", physicalType="boolean", required=True),
                    SchemaProperty(name="notes", physicalType="varchar(20)", required=False),
                ],
            )
        ],
        servers=[Server(server="local", type="filesystem", path="datasets/random", format="json")],
    )

    base_target = base_dir / "datasets" / "random"
    locator_one = ContractVersionLocator(
        dataset_version="v1.0.0/test",
        base=StaticDatasetLocator(path=str(base_target)),
    )
    locator_two = ContractVersionLocator(
        dataset_version="v1.0.0/test-second",
        base=StaticDatasetLocator(path=str(base_target)),
    )

    df_one, path_one = generate_contract_dataset(
        spark,
        contract,
        rows=3,
        dataset_locator=locator_one,
        seed=77,
    )
    df_two, path_two = generate_contract_dataset(
        spark,
        contract,
        rows=3,
        dataset_locator=locator_two,
        seed=77,
    )

    assert path_one.parent == path_two.parent == base_target
    assert path_one.name == "v1.0.0_test"
    assert path_two.name == "v1.0.0_test-second"

    assert [tuple(row) for row in df_one.collect()] == [tuple(row) for row in df_two.collect()]
    assert any(path_one.iterdir())
    assert any(path_two.iterdir())


class RecordingGovernanceService:
    def __init__(self) -> None:
        self.evaluations: list[tuple[str, str, str]] = []
        self.links: list[tuple[str, str, str, str]] = []

    def evaluate_dataset(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult | None,
        observations,
        bump: str = "minor",
        context = None,
        pipeline_context = None,
        operation: str = "write",
        draft_on_violation: bool = False,
    ) -> QualityAssessment:
        self.evaluations.append((contract_id, dataset_id, dataset_version))
        # consume observations so downstream code can reuse cached metrics
        payload = observations()
        status = validation or ValidationResult(
            ok=True,
            errors=[],
            warnings=[],
            metrics={},
            status="ok",
        )
        status.merge_details({"schema": payload.schema, "metrics": payload.metrics})
        return QualityAssessment(status=status, draft=None, observations_reused=payload.reused)

    def review_validation_outcome(self, **_kwargs):
        return None

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self.links.append((dataset_id, dataset_version, contract_id, contract_version))

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None = None,
    ) -> str | None:
        return None


def test_generate_contract_dataset_supports_governance_override(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders")
    governance = RecordingGovernanceService()

    df, path = generate_contract_dataset(
        spark,
        contract,
        rows=2,
        seed=42,
        governance_service=governance,
    )

    assert df.count() == 2
    assert path.parent == tmp_path / "orders"
    assert governance.evaluations
    assert governance.evaluations[0][0] == contract.id
    assert governance.links == [
        (contract.id, contract.version, contract.id, contract.version)
    ]


def test_storage_path_from_resolution_preserves_remote_uri():
    remote = "s3://bucket/contracts/orders"
    assert _storage_path_from_resolution(remote) == remote


def test_storage_path_from_resolution_normalises_local_path(tmp_path):
    local = tmp_path / "dataset"
    resolved = _storage_path_from_resolution(str(local))
    assert isinstance(resolved, Path)
    assert resolved == local
