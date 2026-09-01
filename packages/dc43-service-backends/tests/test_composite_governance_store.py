from __future__ import annotations

from pathlib import Path
import pytest

from dc43_service_clients.data_quality import ValidationResult
from dc43_service_backends.governance.backend.stores.memory import InMemoryGovernanceStore
from dc43_service_backends.governance.backend.stores.composite import CompositeGovernanceStore
from dc43_service_backends.config import (
    load_config,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
)
from dc43_service_backends.bootstrap import build_governance_store


def test_composite_broadcast_mode_all_stores_receive_writes() -> None:
    store1 = InMemoryGovernanceStore()
    store2 = InMemoryGovernanceStore()

    composite = CompositeGovernanceStore(
        backends={"store1": store1, "store2": store2}
    )

    vr = ValidationResult(status="ok", metrics={"null_count.id": 0})
    composite.save_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
        status=vr,
    )

    # Both stores received status
    assert store1.load_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
    ) == vr
    assert store2.load_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
    ) == vr

    # Link dataset contract
    composite.link_dataset_contract(
        dataset_id="main.sales.orders",
        dataset_version="v1",
        contract_id="sales.orders",
        contract_version="1.0.0",
    )
    assert store1.get_linked_contract_version(dataset_id="main.sales.orders") == "sales.orders:1.0.0"
    assert store2.get_linked_contract_version(dataset_id="main.sales.orders") == "sales.orders:1.0.0"

    # Activity event
    composite.record_pipeline_event(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
        event={"operation": "write"},
    )
    assert len(store1.load_pipeline_activity(dataset_id="main.sales.orders")) == 1
    assert len(store2.load_pipeline_activity(dataset_id="main.sales.orders")) == 1

    # Datasets list
    assert composite.list_datasets() == ["main.sales.orders"]


def test_composite_routed_mode_with_all_catchall() -> None:
    lakehouse = InMemoryGovernanceStore()
    bi_sql = InMemoryGovernanceStore()
    audit_s3 = InMemoryGovernanceStore()

    composite = CompositeGovernanceStore(
        backends={
            "lakehouse": lakehouse,
            "bi_sql": bi_sql,
            "audit_s3": audit_s3,
        },
        routes={
            "all": ["lakehouse"],
            "metrics": ["bi_sql"],
            "activity": ["lakehouse", "audit_s3"],
        },
    )

    # 1. Status write: routes to "all" -> lakehouse only
    vr = ValidationResult(status="ok")
    composite.save_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
        status=vr,
    )
    assert lakehouse.load_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
    ) is not None
    assert bi_sql.load_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
    ) is None
    assert audit_s3.load_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
    ) is None

    # 2. Activity event: routes to activity -> lakehouse AND audit_s3
    composite.record_pipeline_event(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="main.sales.orders",
        dataset_version="v1",
        event={"operation": "write", "recorded_at": "2026-09-01T10:00:00Z"},
    )
    assert len(lakehouse.load_pipeline_activity(dataset_id="main.sales.orders")) == 1
    assert len(audit_s3.load_pipeline_activity(dataset_id="main.sales.orders")) == 1
    assert len(bi_sql.load_pipeline_activity(dataset_id="main.sales.orders")) == 0


def test_composite_wildcard_routes() -> None:
    store_a = InMemoryGovernanceStore()
    store_b = InMemoryGovernanceStore()

    composite = CompositeGovernanceStore(
        backends={"store_a": store_a, "store_b": store_b},
        routes={"*": ["store_a"], "status": ["store_b"]},
    )

    vr = ValidationResult(status="ok")
    composite.save_status(
        contract_id="c1",
        contract_version="1.0",
        dataset_id="d1",
        dataset_version="v1",
        status=vr,
    )
    # Status goes to store_b
    assert store_b.load_status(contract_id="c1", contract_version="1.0", dataset_id="d1", dataset_version="v1") is not None
    assert store_a.load_status(contract_id="c1", contract_version="1.0", dataset_id="d1", dataset_version="v1") is None

    # Links fallback to wildcard "*" -> store_a
    composite.link_dataset_contract(dataset_id="d1", dataset_version="v1", contract_id="c1", contract_version="1.0")
    assert store_a.get_linked_contract_version(dataset_id="d1") == "c1:1.0"
    assert store_b.get_linked_contract_version(dataset_id="d1") is None


def test_composite_config_loading_from_toml(tmp_path: Path) -> None:
    config_file = tmp_path / "backends.toml"
    config_file.write_text(
        """
[governance_store]
type = "composite"

[governance_store.backends.lakehouse]
type = "memory"
status_table = "dq_status"

[governance_store.backends.bi_sql]
type = "memory"
metrics_table = "dq_metrics"

[governance_store.routes]
all = ["lakehouse"]
metrics = ["bi_sql"]
        """,
        encoding="utf-8",
    )

    conf = load_config(config_file)
    assert conf.governance_store.type == "composite"
    assert "lakehouse" in conf.governance_store.backends
    assert "bi_sql" in conf.governance_store.backends
    assert conf.governance_store.backends["lakehouse"].type == "memory"
    assert conf.governance_store.backends["bi_sql"].type == "memory"
    assert conf.governance_store.routes["all"] == ["lakehouse"]
    assert conf.governance_store.routes["metrics"] == ["bi_sql"]

    # Bootstrap
    store = build_governance_store(conf.governance_store)
    assert isinstance(store, CompositeGovernanceStore)


def test_composite_requires_backends() -> None:
    with pytest.raises(ValueError, match="requires at least one backend"):
        CompositeGovernanceStore(backends={})

    conf = GovernanceStoreConfig(type="composite", backends={})
    with pytest.raises(RuntimeError, match="must declare at least one backend"):
        build_governance_store(conf)
