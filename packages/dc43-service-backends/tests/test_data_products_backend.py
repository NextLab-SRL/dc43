from pathlib import Path

from dc43.core.odps import DataProductInputPort, DataProductOutputPort
from dc43_service_backends.data_products import (
    CollibraDataProductServiceBackend,
    FilesystemDataProductServiceBackend,
    LocalDataProductServiceBackend,
    StubCollibraDataProductAdapter,
)


def test_register_input_port_creates_draft_version() -> None:
    backend = LocalDataProductServiceBackend()

    registration = backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
        source_data_product="dp.source",
        source_output_port="gold",
    )

    assert registration.changed is True
    product = registration.product
    assert product.version is not None
    assert product.status == "draft"
    assert product.input_ports[0].contract_id == "orders"
    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert latest.version == product.version


def test_register_input_port_idempotent() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
    )

    registration = backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
    )

    assert registration.changed is False


def test_register_output_port_updates_version_once() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(name="snapshot", version="1.0.0", contract_id="snapshot"),
    )
    first_version = backend.latest("dp.analytics").version
    update = backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(name="snapshot", version="1.1.0", contract_id="snapshot"),
    )
    assert update.changed is True
    second_version = backend.latest("dp.analytics").version
    assert second_version != first_version


def test_resolve_output_contract_returns_contract_reference() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(name="report", version="2.0.0", contract_id="report"),
    )
    resolved = backend.resolve_output_contract(
        data_product_id="dp.analytics",
        port_name="report",
    )
    assert resolved == ("report", "2.0.0")


def test_filesystem_backend_persists_products(tmp_path: Path) -> None:
    backend = FilesystemDataProductServiceBackend(tmp_path)

    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(name="snapshot", version="1.0.0", contract_id="snapshot"),
    )

    on_disk = list(tmp_path.rglob("*.json"))
    assert on_disk

    reloaded = FilesystemDataProductServiceBackend(tmp_path)
    product = reloaded.latest("dp.analytics")
    assert product is not None
    assert product.output_ports[0].contract_id == "snapshot"


def test_collibra_backend_uses_stub_adapter(tmp_path: Path) -> None:
    adapter = StubCollibraDataProductAdapter(str(tmp_path))
    backend = CollibraDataProductServiceBackend(adapter)

    registration = backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(name="snapshot", version="1.0.0", contract_id="snapshot"),
    )

    assert registration.changed is True
    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert latest.output_ports[0].contract_id == "snapshot"

    adapter.close()
