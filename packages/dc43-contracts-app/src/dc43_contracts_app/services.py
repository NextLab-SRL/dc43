from __future__ import annotations

"""Service client bootstrap helpers for the contracts application.

The contracts UI talks to the governance, contracts, and data-product service
layers exclusively through the client interfaces defined in
``dc43_service_clients``.  Historically the FastAPI app initialised those
clients directly inside ``server.py`` which left the module filled with backend
plumbing.  This module centralises that wiring so the rest of the application
can interact with a small, well-defined surface area regardless of whether the
services are running in-process or behind HTTP.
"""

import os
from dataclasses import dataclass
import logging
from pathlib import Path
from threading import local
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING
from uuid import uuid4

import httpx
from httpx import HTTPStatusError

from dc43_service_backends.bootstrap import BackendSuite, build_backends
from dc43_service_backends.config import (
    DataProductStoreConfig,
    ServiceBackendsConfig,
    load_config as load_service_backends_config,
)
from dc43_service_clients._http_sync import close_client
from dc43_service_clients.contracts import (
    ContractServiceClient,
    LocalContractServiceClient,
    RemoteContractServiceClient,
)
from dc43_service_clients.data_products import (
    DataProductServiceClient,
    LocalDataProductServiceClient,
    RemoteDataProductServiceClient,
)
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    LocalDataQualityServiceClient,
    RemoteDataQualityServiceClient,
)
from dc43_service_clients.governance import (
    GovernanceServiceClient,
    LocalGovernanceServiceClient,
    RemoteGovernanceServiceClient,
)
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_clients.odps import OpenDataProductStandard

from dc43_contracts_app.hints import get_workspace_hints

if TYPE_CHECKING:  # pragma: no cover - import side-effect guard
    from dc43_contracts_app.config import BackendConfig

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ServiceBundle:
    """Container storing the service clients bound to the active backend."""

    contract: ContractServiceClient
    data_product: DataProductServiceClient
    data_quality: DataQualityServiceClient
    governance: GovernanceServiceClient
    http_client: httpx.Client | httpx.AsyncClient | None = None


_SERVICE_BACKENDS_CONFIG: ServiceBackendsConfig | None = None
_SERVICE_BUNDLE: ServiceBundle | None = None
_BACKEND_MODE: str = "embedded"
_BACKEND_BASE_URL: str = "http://dc43-services"
_BACKEND_TOKEN: str = ""
_THREAD_CLIENTS = local()
_SERVICE_BACKENDS_SIGNATURE: Tuple[str | None, str | None, str | None] | None = None


def _coerce_path(value: object) -> Path | None:
    if isinstance(value, Path):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Path(text).expanduser()
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return None


def _workspace_hints() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    try:
        hints = get_workspace_hints()
    except Exception:  # pragma: no cover - defensive import guard
        hints = None
    if hints:
        mapping.update({str(key): str(value) for key, value in hints.items() if value})

    env_root = os.getenv("DC43_CONTRACTS_APP_WORK_DIR") or os.getenv("DC43_DEMO_WORK_DIR")
    if env_root and "root" not in mapping:
        mapping["root"] = env_root

    if "root" not in mapping:
        try:
            from dc43_contracts_app.config import load_config  # local import to avoid cycle

            config = load_config()
        except Exception:  # pragma: no cover - configuration fallback
            config = None
        if config is not None:
            root_path = getattr(config.workspace, "root", None)
            if root_path:
                mapping["root"] = str(root_path)

    return mapping


def _apply_workspace_overrides(config: ServiceBackendsConfig, hints: Mapping[str, str]) -> None:
    root = _coerce_path(hints.get("root"))
    contract_root = _coerce_path(hints.get("contracts_dir")) or (
        root.joinpath("contracts") if root is not None else None
    )
    products_root = _coerce_path(hints.get("products_dir")) or (
        root.joinpath("products") if root is not None else None
    )

    if (
        config.contract_store.type == "filesystem"
        and _coerce_path(config.contract_store.root) is None
        and contract_root is not None
    ):
        config.contract_store.root = contract_root

    data_product_store: DataProductStoreConfig = config.data_product_store
    if (
        data_product_store.type == "filesystem"
        and _coerce_path(data_product_store.root) is None
        and products_root is not None
    ):
        data_product_store.root = products_root


def _workspace_signature(hints: Mapping[str, str]) -> Tuple[str | None, str | None, str | None]:
    def _norm(value: object) -> str | None:
        path = _coerce_path(value)
        return str(path) if path is not None else None

    root = hints.get("root")
    contracts = hints.get("contracts_dir")
    products = hints.get("products_dir")

    if root and not contracts:
        contracts = str(Path(root).expanduser() / "contracts")
    if root and not products:
        products = str(Path(root).expanduser() / "products")

    return _norm(root), _norm(contracts), _norm(products)
def _service_backends_config() -> ServiceBackendsConfig:
    global _SERVICE_BACKENDS_CONFIG, _SERVICE_BACKENDS_SIGNATURE

    hints = _workspace_hints()
    signature = _workspace_signature(hints)

    if _SERVICE_BACKENDS_CONFIG is None or _SERVICE_BACKENDS_SIGNATURE != signature:
        config = load_service_backends_config()
        _apply_workspace_overrides(config, hints)
        _SERVICE_BACKENDS_CONFIG = config
        _SERVICE_BACKENDS_SIGNATURE = signature
    else:
        _apply_workspace_overrides(_SERVICE_BACKENDS_CONFIG, hints)

    return _SERVICE_BACKENDS_CONFIG


def service_backends_config() -> ServiceBackendsConfig:
    """Return the cached :class:`ServiceBackendsConfig` instance."""

    return _service_backends_config()


def _assign_service_clients(bundle: ServiceBundle) -> None:
    global _SERVICE_BUNDLE
    _SERVICE_BUNDLE = bundle


def _close_http_client(client: httpx.Client | httpx.AsyncClient | None) -> None:
    if client is None:
        return
    try:
        close_client(client)
    except Exception:  # pragma: no cover - defensive cleanup
        logger.exception("Failed to close backend HTTP client")


def _close_backend_client() -> None:
    bundle = _SERVICE_BUNDLE
    if bundle is None:
        return
    _close_http_client(bundle.http_client)
    if bundle.http_client is not None:
        bundle.http_client = None


def _clear_thread_clients() -> None:
    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is None:
        return
    try:
        close_client(bundle.get("http_client"))
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to close thread-local backend client")
    finally:
        _THREAD_CLIENTS.bundle = None


def _initialise_remote_bundle(base_url: str) -> ServiceBundle:
    http_client = httpx.Client(base_url=base_url or None)
    return ServiceBundle(
        contract=RemoteContractServiceClient(base_url=base_url, client=http_client),
        data_product=RemoteDataProductServiceClient(base_url=base_url, client=http_client),
        data_quality=RemoteDataQualityServiceClient(base_url=base_url, client=http_client),
        governance=RemoteGovernanceServiceClient(base_url=base_url, client=http_client),
        http_client=http_client,
    )


def _initialise_embedded_bundle(suite: BackendSuite | None) -> ServiceBundle:
    suite = suite or build_backends(_service_backends_config())
    return ServiceBundle(
        contract=LocalContractServiceClient(suite.contract),
        data_product=LocalDataProductServiceClient(suite.data_product),
        data_quality=LocalDataQualityServiceClient(suite.data_quality),
        governance=LocalGovernanceServiceClient(suite.governance),
    )


def _initialise_backend(*, base_url: str | None = None, suite: BackendSuite | None = None) -> None:
    global _BACKEND_BASE_URL, _BACKEND_MODE, _BACKEND_TOKEN

    _close_backend_client()
    _clear_thread_clients()

    client_base_url = base_url.rstrip("/") if base_url else "http://dc43-services"
    if base_url:
        bundle = _initialise_remote_bundle(client_base_url)
        _BACKEND_MODE = "remote"
    else:
        bundle = _initialise_embedded_bundle(suite)
        _BACKEND_MODE = "embedded"

    _BACKEND_BASE_URL = client_base_url
    _BACKEND_TOKEN = uuid4().hex
    _assign_service_clients(bundle)


def configure_backend(
    base_url: str | None = None,
    *,
    config: "BackendConfig" | None = None,
) -> None:
    """Initialise service clients using the configured backend settings."""

    if base_url is not None:
        _initialise_backend(base_url=base_url or None)
        return

    env_url = (
        os.getenv("DC43_CONTRACTS_APP_BACKEND_URL")
        or os.getenv("DC43_DEMO_BACKEND_URL")
    )
    if env_url:
        _initialise_backend(base_url=env_url)
        return

    if config is None:
        from dc43_contracts_app.config import load_config  # local import to avoid cycle

        config = load_config().backend

    mode = (config.mode or "embedded").lower()
    if mode == "remote":
        target_url = config.base_url or config.process.url()
        _initialise_backend(base_url=target_url)
    else:
        _initialise_backend(base_url=None, suite=build_backends(_service_backends_config()))


def backend_mode() -> str:
    return _BACKEND_MODE


def backend_base_url() -> str:
    return _BACKEND_BASE_URL


def backend_token() -> str:
    return _BACKEND_TOKEN


def contract_service_client() -> ContractServiceClient:
    bundle = _SERVICE_BUNDLE
    if bundle is None:
        raise RuntimeError("Contract service client is not configured")
    return bundle.contract


def data_product_service_client() -> Optional[DataProductServiceClient]:
    bundle = _SERVICE_BUNDLE
    return None if bundle is None else bundle.data_product


def data_quality_service_client() -> Optional[DataQualityServiceClient]:
    bundle = _SERVICE_BUNDLE
    return None if bundle is None else bundle.data_quality


def governance_service_client() -> Optional[GovernanceServiceClient]:
    bundle = _SERVICE_BUNDLE
    return None if bundle is None else bundle.governance


def thread_service_clients() -> Tuple[
    ContractServiceClient,
    DataQualityServiceClient,
    GovernanceServiceClient,
]:
    if backend_mode() != "remote":
        contract = contract_service_client()
        dq = data_quality_service_client()
        governance = governance_service_client()
        if dq is None or governance is None:
            raise RuntimeError("Backend clients have not been initialised")
        return contract, dq, governance

    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is not None and bundle.get("token") == _BACKEND_TOKEN:
        return bundle["contract"], bundle["dq"], bundle["governance"]

    if bundle is not None:
        try:
            close_client(bundle.get("http_client"))
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to recycle thread-local backend client")

    http_client = httpx.Client(base_url=_BACKEND_BASE_URL or None)
    contract = RemoteContractServiceClient(base_url=_BACKEND_BASE_URL, client=http_client)
    dq = RemoteDataQualityServiceClient(base_url=_BACKEND_BASE_URL, client=http_client)
    governance = RemoteGovernanceServiceClient(base_url=_BACKEND_BASE_URL, client=http_client)
    _THREAD_CLIENTS.bundle = {
        "token": _BACKEND_TOKEN,
        "http_client": http_client,
        "contract": contract,
        "dq": dq,
        "governance": governance,
    }
    return contract, dq, governance


def _normalise_listing_items(payload: Mapping[str, object] | Sequence[object]) -> List[str]:
    if isinstance(payload, Mapping):
        items = payload.get("items")
        if isinstance(items, list):
            return [str(item) for item in items]
        return []
    if isinstance(payload, list):
        return [str(item) for item in payload]
    return []


def list_contract_ids() -> List[str]:
    service = contract_service_client()
    try:
        payload = service.list_contracts()
    except Exception as exc:  # pragma: no cover - defensive when listing fails
        logger.exception("Failed to list contracts: %s", exc)
        return []

    items = _normalise_listing_items(payload)
    results: List[str] = list(items)
    if isinstance(payload, Mapping):
        total = payload.get("total")
        limit = payload.get("limit")
        offset = payload.get("offset", 0)
        try:
            total_int = int(total) if total is not None else None
        except (TypeError, ValueError):
            total_int = None

        step: int | None = None
        try:
            if isinstance(limit, str) and limit.strip():
                step = int(limit)
            elif isinstance(limit, int):
                step = limit
        except (TypeError, ValueError):
            step = None

        if total_int is not None and total_int > len(results):
            if not step:
                step = max(len(items), 1)
            current_offset = int(offset or 0) + step
            while len(results) < total_int:
                try:
                    page = service.list_contracts(limit=step, offset=current_offset)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.exception("Failed to fetch contract page at offset %s: %s", current_offset, exc)
                    break
                page_items = _normalise_listing_items(page)
                if not page_items:
                    break
                results.extend(page_items)
                current_offset += step

    return results


def contract_versions(contract_id: str) -> List[str]:
    service = contract_service_client()
    try:
        versions = service.list_versions(contract_id)
    except HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise
    except FileNotFoundError:
        return []
    return [str(value) for value in versions]


def get_contract(contract_id: str, contract_version: str) -> OpenDataContractStandard:
    service = contract_service_client()
    try:
        return service.get(contract_id, contract_version)
    except HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise FileNotFoundError(
                f"Contract {contract_id}@{contract_version} not found"
            ) from exc
        raise


def latest_contract(contract_id: str) -> Optional[OpenDataContractStandard]:
    service = contract_service_client()
    try:
        return service.latest(contract_id)
    except HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None
        raise


def put_contract(contract: OpenDataContractStandard) -> None:
    service = contract_service_client()
    service.put(contract)


class ContractServiceAdapter:
    """Compatibility layer exposing store-like helpers for tests."""

    def put(self, contract: OpenDataContractStandard) -> None:
        put_contract(contract)

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return get_contract(contract_id, contract_version)

    def list_contracts(self) -> List[str]:
        return list_contract_ids()

    def list_versions(self, contract_id: str) -> List[str]:
        return contract_versions(contract_id)

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        return latest_contract(contract_id)


store = ContractServiceAdapter()


def list_data_product_ids() -> List[str]:
    service = data_product_service_client()
    if service is None:
        return []
    try:
        payload = service.list_data_products()
    except Exception as exc:  # pragma: no cover - defensive when listing fails
        logger.exception("Failed to list data products via backend: %s", exc)
        return []
    if isinstance(payload, Mapping):
        items = payload.get("items")
        if isinstance(items, list):
            return [str(item) for item in items]
    if isinstance(payload, list):
        return [str(item) for item in payload]
    return []


def latest_data_product(data_product_id: str) -> Optional[OpenDataProductStandard]:
    service = data_product_service_client()
    if service is None:
        return None
    try:
        return service.latest(data_product_id)
    except Exception as exc:  # pragma: no cover - defensive when lookup fails
        logger.exception("Failed to load data product %s: %s", data_product_id, exc)
        return None


__all__ = [
    "backend_base_url",
    "backend_mode",
    "backend_token",
    "configure_backend",
    "contract_service_client",
    "contract_versions",
    "get_contract",
    "latest_contract",
    "latest_data_product",
    "list_contract_ids",
    "list_data_product_ids",
    "put_contract",
    "service_backends_config",
    "store",
    "thread_service_clients",
    "data_product_service_client",
    "data_quality_service_client",
    "governance_service_client",
]

