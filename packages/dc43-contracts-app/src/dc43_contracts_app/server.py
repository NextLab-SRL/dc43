from __future__ import annotations

"""FastAPI demo application for dc43.

This application provides a small Bootstrap-powered UI to manage data
contracts and run an example Spark pipeline that records dataset versions
with their validation status. Contracts are stored on the local
filesystem using :class:`~dc43_service_backends.contracts.backend.stores.FSContractStore` and dataset
metadata lives in a JSON file.

Run the UI directly with::

    uvicorn dc43_demo_app.server:app --reload

or start the full demo (UI + HTTP backend) with::

    dc43-demo

Optional dependencies needed: ``fastapi``, ``uvicorn``, ``jinja2`` and
``pyspark``.
"""

import asyncio
import contextlib
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple, Mapping, Optional, Iterable
from uuid import uuid4
from threading import Lock
import threading
import json
import os
import re
import shutil
import textwrap
from datetime import datetime
from collections import Counter

import httpx
from fastapi import APIRouter, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from httpx import ASGITransport
from fastapi.concurrency import run_in_threadpool

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.web import build_local_app
from dc43_service_clients._http_sync import close_client
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients.odps import OpenDataProductStandard
from ._odcs import custom_properties_dict, normalise_custom_properties
from ._versioning import SemVer
from .config import BackendConfig, ContractsAppConfig, load_config
from .workspace import ContractsAppWorkspace, workspace_from_env
from open_data_contract_standard.model import (
    CustomProperty,
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
    ServiceLevelAgreementProperty,
    Support,
)
from pydantic import ValidationError
from packaging.version import Version, InvalidVersion

# Optional pyspark-based helpers. Keep imports lazy-friendly so the demo UI can
# still load when pyspark is not installed (for example when running fast unit
# tests).
try:  # pragma: no cover - exercised indirectly when pyspark is available
    from dc43_integrations.spark.io import ContractVersionLocator, read_with_contract
except ModuleNotFoundError as exc:  # pragma: no cover - safety net for CI
    if exc.name != "pyspark":
        raise
    ContractVersionLocator = None  # type: ignore[assignment]
    read_with_contract = None  # type: ignore[assignment]

_SPARK_SESSION: Any | None = None
logger = logging.getLogger(__name__)


def _spark_session() -> Any:
    """Return a cached local Spark session for previews."""

    global _SPARK_SESSION
    if _SPARK_SESSION is None:
        from pyspark.sql import SparkSession  # type: ignore

        _SPARK_SESSION = (
            SparkSession.builder.master("local[1]")
            .appName("dc43-preview")
            .getOrCreate()
        )
    return _SPARK_SESSION

BASE_DIR = Path(__file__).resolve().parent

_CONFIG_LOCK = Lock()
_ACTIVE_CONFIG: ContractsAppConfig | None = None
_WORKSPACE_LOCK = Lock()
_WORKSPACE: ContractsAppWorkspace | None = None
WORK_DIR: Path
CONTRACT_DIR: Path
DATA_DIR: Path
RECORDS_DIR: Path
DATASETS_FILE: Path
DATA_PRODUCTS_FILE: Path
DQ_STATUS_DIR: Path
store: FSContractStore
_SETUP_STATE_LOCK = Lock()


def configure_workspace(workspace: ContractsAppWorkspace) -> None:
    """Set the active filesystem layout for the application."""

    global _WORKSPACE, WORK_DIR, CONTRACT_DIR, DATA_DIR, RECORDS_DIR, DATASETS_FILE, DATA_PRODUCTS_FILE, DQ_STATUS_DIR, store

    workspace.ensure()
    WORK_DIR = workspace.root
    CONTRACT_DIR = workspace.contracts_dir
    DATA_DIR = workspace.data_dir
    RECORDS_DIR = workspace.records_dir
    DATASETS_FILE = workspace.datasets_file
    DATA_PRODUCTS_FILE = workspace.data_products_file
    DQ_STATUS_DIR = workspace.dq_status_dir
    _WORKSPACE = workspace
    store = FSContractStore(str(CONTRACT_DIR))
    try:
        os.environ.setdefault("DC43_CONTRACTS_APP_WORK_DIR", str(WORK_DIR))
    except Exception:  # pragma: no cover - defensive
        pass


def current_workspace() -> ContractsAppWorkspace:
    """Return the configured workspace initialising defaults when needed."""

    _current_config()
    global _WORKSPACE
    if _WORKSPACE is None:
        with _WORKSPACE_LOCK:
            if _WORKSPACE is None:
                active = _current_config()
                default_root = (
                    str(active.workspace.root) if active.workspace.root else None
                )
                workspace, _ = workspace_from_env(default_root=default_root)
                configure_workspace(workspace)
    assert _WORKSPACE is not None
    return _WORKSPACE


def _set_active_config(config: ContractsAppConfig) -> ContractsAppConfig:
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        _ACTIVE_CONFIG = config
    return config


def _current_config() -> ContractsAppConfig:
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        if _ACTIVE_CONFIG is None:
            _ACTIVE_CONFIG = load_config()
        return _ACTIVE_CONFIG




def _safe_fs_name(value: str) -> str:
    """Return a filesystem-friendly representation for governance ids."""

    return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in value)


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    """Return decoded JSON for ``path`` or ``None`` on failure."""

    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _version_sort_key(value: str) -> tuple[int, Tuple[int, int, int] | float | str, str]:
    """Sort versions treating ISO timestamps and SemVer intelligently."""

    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
        return (0, dt.timestamp(), value)
    except ValueError:
        pass
    try:
        parsed = SemVer.parse(value)
        return (1, (parsed.major, parsed.minor, parsed.patch), value)
    except ValueError:
        return (2, value, value)


def _sort_versions(entries: Iterable[str]) -> List[str]:
    """Return ``entries`` sorted using :func:`_version_sort_key`."""

    return sorted(entries, key=_version_sort_key)


def _dq_status_dir_for(dataset_id: str) -> Path:
    """Return the directory that stores compatibility statuses for ``dataset_id``."""

    return DQ_STATUS_DIR / _safe_fs_name(dataset_id)


def _dq_status_path(dataset_id: str, dataset_version: str) -> Path:
    """Return the JSON payload path for the supplied dataset/version pair."""

    directory = _dq_status_dir_for(dataset_id)
    return directory / f"{_safe_fs_name(dataset_version)}.json"


def _dq_status_payload(dataset_id: str, dataset_version: str) -> Optional[Dict[str, Any]]:
    """Load the compatibility payload if available."""

    path = _dq_status_path(dataset_id, dataset_version)
    if not path.exists():
        return None
    return _read_json_file(path)


def _dataset_root_for(dataset_id: str, dataset_path: Optional[str] = None) -> Optional[Path]:
    """Return the directory that should contain materialised versions."""

    base: Optional[Path] = None
    if dataset_path:
        try:
            path = Path(dataset_path)
        except (TypeError, ValueError):
            path = None
        if path is not None:
            if path.suffix:
                path = path.parent / path.stem
            if not path.is_absolute():
                path = (Path(DATA_DIR).parent / path).resolve()
            base = path
    if base is None and dataset_id:
        base = DATA_DIR / dataset_id.replace("::", "__")
    return base


def _version_marker_value(folder: Path) -> str:
    """Return the canonical version value for ``folder`` if annotated."""

    marker = folder / ".dc43_version"
    if marker.exists():
        try:
            text = marker.read_text().strip()
        except OSError:
            text = ""
        if text:
            return text
    return folder.name


def _candidate_version_paths(dataset_dir: Path, version: str) -> List[Path]:
    """Return directories that may correspond to ``version``."""

    candidates: List[Path] = []
    direct = dataset_dir / version
    candidates.append(direct)
    safe = dataset_dir / _safe_fs_name(version)
    if safe != direct:
        candidates.append(safe)
    try:
        for entry in dataset_dir.iterdir():
            if not entry.is_dir():
                continue
            if _version_marker_value(entry) == version and entry not in candidates:
                candidates.append(entry)
    except FileNotFoundError:
        return []
    return candidates


def _has_version_materialisation(dataset_dir: Path, version: str) -> bool:
    """Return ``True`` if ``dataset_dir`` contains files for ``version``."""

    lowered = version.lower()
    if lowered in {"latest", "current"} or lowered.startswith("latest__"):
        return True
    for candidate in _candidate_version_paths(dataset_dir, version):
        if candidate.exists():
            return True
    return False


def _existing_version_dir(dataset_dir: Path, version: str) -> Optional[Path]:
    """Return an existing directory matching ``version`` if available."""

    for candidate in _candidate_version_paths(dataset_dir, version):
        if candidate.exists():
            return candidate
    return None


def _target_version_dir(dataset_dir: Path, version: str) -> Path:
    """Return the directory path where ``version`` should be materialised."""

    safe = _safe_fs_name(version)
    if not safe:
        safe = "version"
    return dataset_dir / safe


def _ensure_version_marker(path: Path, version: str) -> None:
    """Record ``version`` inside ``path`` for lookup when sanitised."""

    if not path.exists() or not path.is_dir():
        return
    marker = path / ".dc43_version"
    try:
        marker.write_text(version)
    except OSError:
        pass


def _dq_status_entries(dataset_id: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Return (display_version, stored_version, payload) tuples."""

    directory = _dq_status_dir_for(dataset_id)
    entries: List[Tuple[str, str, Dict[str, Any]]] = []
    if not directory.exists():
        return entries
    for path in directory.glob("*.json"):
        payload = _read_json_file(path) or {}
        display_version = str(payload.get("dataset_version") or path.stem)
        entries.append((display_version, path.stem, payload))
    entries.sort(key=lambda item: _version_sort_key(item[0]))
    return entries


def _dq_status_versions(dataset_id: str) -> List[str]:
    """Return known dataset versions recorded by the governance stub."""

    return [entry[0] for entry in _dq_status_entries(dataset_id)]


def _link_path(target: Path, source: Path) -> None:
    """Create a symlink (or copy fallback) from ``target`` to ``source``."""

    if target.exists() or target.is_symlink():
        try:
            if target.is_symlink() and target.resolve() == source.resolve():
                return
        except OSError:
            pass
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        else:
            target.unlink()

    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        relative = os.path.relpath(source, target.parent)
        target.symlink_to(relative, target_is_directory=source.is_dir())
    except OSError:
        if source.is_dir():
            shutil.copytree(source, target, dirs_exist_ok=True)
        else:
            shutil.copy2(source, target)


def _iter_versions(dataset_dir: Path) -> list[Path]:
    """Return sorted dataset version directories ignoring alias folders."""

    versions: list[Path] = []
    for candidate in dataset_dir.iterdir():
        if not candidate.is_dir():
            continue
        name = candidate.name
        if name == "latest" or name.startswith("latest__"):
            continue
        versions.append(candidate)
    return sorted(versions)


def refresh_dataset_aliases(dataset: str | None = None) -> None:
    """Populate ``latest``/derived aliases for the selected dataset(s)."""

    roots: list[Path]
    if dataset:
        base = DATA_DIR / dataset
        roots = [base] if base.exists() else []
    else:
        roots = [p for p in DATA_DIR.iterdir() if p.is_dir() and "__" not in p.name]

    for dataset_dir in roots:
        versions = _iter_versions(dataset_dir)
        if not versions:
            continue
        latest = versions[-1]
        _link_path(dataset_dir / "latest", latest)

        derived_dirs = sorted(DATA_DIR.glob(f"{dataset_dir.name}__*"))
        for derived_dir in derived_dirs:
            if not derived_dir.is_dir():
                continue
            suffix = derived_dir.name.split("__", 1)[1]
            derived_versions = _iter_versions(derived_dir)
            for version_dir in derived_versions:
                target = dataset_dir / version_dir.name / suffix
                _link_path(target, version_dir)
            if derived_versions:
                _link_path(dataset_dir / f"latest__{suffix}", derived_versions[-1])


def set_active_version(dataset: str, version: str) -> None:
    """Point the ``latest`` alias of ``dataset`` (and derivatives) to ``version``."""

    dataset_dir = DATA_DIR / dataset
    target = _existing_version_dir(dataset_dir, version)
    if target is None:
        target = _target_version_dir(dataset_dir, version)
    if not target.exists():
        raise FileNotFoundError(f"Unknown dataset version: {dataset} {version}")

    _link_path(dataset_dir / "latest", target)

    if "__" not in dataset:
        for derived_dir in DATA_DIR.glob(f"{dataset}__*"):
            suffix = derived_dir.name.split("__", 1)[1]
            derived_target = _existing_version_dir(derived_dir, version)
            if derived_target is None:
                continue
            _link_path(target / suffix, derived_target)
            _link_path(dataset_dir / f"latest__{suffix}", derived_target)
    else:
        base, suffix = dataset.split("__", 1)
        base_dir = DATA_DIR / base
        version_dir = _existing_version_dir(base_dir, version)
        if version_dir is not None and version_dir.exists():
            _link_path(version_dir / suffix, target)
            _link_path(base_dir / f"latest__{suffix}", target)


def register_dataset_version(dataset: str, version: str, source: Path) -> None:
    """Expose ``source`` under ``data/<dataset>/<version>`` via symlink."""

    dataset_dir = DATA_DIR / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)
    target = _target_version_dir(dataset_dir, version)
    _link_path(target, source)
    _ensure_version_marker(target, version)


_STATUS_OPTIONS: List[Tuple[str, str]] = [
    ("", "Unspecified"),
    ("draft", "Draft"),
    ("active", "Active"),
    ("deprecated", "Deprecated"),
    ("retired", "Retired"),
    ("suspended", "Suspended"),
]

_VERSIONING_MODES: List[Tuple[str, str]] = [
    ("", "Not specified"),
    ("delta", "Delta (time-travel compatible)"),
    ("snapshot", "Snapshot folders"),
    ("append", "Append-only log"),
]

_backend_app: FastAPI | None = None
_backend_transport: ASGITransport | None = None
_backend_client: httpx.AsyncClient | None = None
_backend_base_url: str = "http://dc43-services"
_backend_mode: str = "embedded"
_backend_token: str = ""
_THREAD_CLIENTS = threading.local()
contract_service: RemoteContractServiceClient
dq_service: RemoteDataQualityServiceClient
governance_service: RemoteGovernanceServiceClient


def _close_backend_client() -> None:
    """Best-effort close of the shared HTTP client."""

    global _backend_client
    client = _backend_client
    if client is None:
        return
    try:
        asyncio.run(client.aclose())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(client.aclose())
        finally:
            loop.close()
    _backend_client = None


def _clear_thread_clients() -> None:
    """Dispose of thread-local HTTP clients for the current thread."""

    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is None:
        return
    try:
        close_client(bundle["http_client"])
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to close thread-local backend client")
    finally:
        _THREAD_CLIENTS.bundle = None


def _thread_service_clients() -> tuple[
    RemoteContractServiceClient,
    RemoteDataQualityServiceClient,
    RemoteGovernanceServiceClient,
]:
    """Return backend service clients scoped to the current thread."""

    bundle = getattr(_THREAD_CLIENTS, "bundle", None)
    if bundle is not None and bundle.get("token") == _backend_token:
        return (
            bundle["contract"],
            bundle["dq"],
            bundle["governance"],
        )

    if bundle is not None:
        try:
            close_client(bundle["http_client"])
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to recycle thread-local backend client")

    if _backend_mode == "remote":
        http_client: httpx.Client | httpx.AsyncClient = httpx.Client(
            base_url=_backend_base_url or None,
        )
    else:
        assert _backend_app is not None
        http_client = httpx.AsyncClient(
            transport=ASGITransport(app=_backend_app),
            base_url=_backend_base_url or None,
        )

    contract = RemoteContractServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )
    dq = RemoteDataQualityServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )
    governance = RemoteGovernanceServiceClient(
        base_url=_backend_base_url,
        client=http_client,
    )

    _THREAD_CLIENTS.bundle = {
        "token": _backend_token,
        "http_client": http_client,
        "contract": contract,
        "dq": dq,
        "governance": governance,
    }
    return contract, dq, governance


def _initialise_backend(*, base_url: str | None = None) -> None:
    """Configure service clients against an in-process or remote backend."""

    global _backend_app, _backend_transport, _backend_client
    global contract_service, dq_service, governance_service
    global _backend_base_url, _backend_mode, _backend_token

    _close_backend_client()
    _clear_thread_clients()

    client_base_url = (base_url.rstrip("/") if base_url else "http://dc43-services")

    if base_url:
        _backend_app = None
        _backend_transport = None
        _backend_client = httpx.AsyncClient(base_url=client_base_url)
        _backend_mode = "remote"
    else:
        _backend_app = build_local_app(store)
        _backend_transport = ASGITransport(app=_backend_app)
        _backend_client = httpx.AsyncClient(
            transport=_backend_transport,
            base_url=client_base_url,
        )
        _backend_mode = "embedded"

    _backend_base_url = client_base_url
    _backend_token = uuid4().hex

    contract_service = RemoteContractServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )
    dq_service = RemoteDataQualityServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )
    governance_service = RemoteGovernanceServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )


def configure_backend(
    base_url: str | None = None, *, config: BackendConfig | None = None
) -> None:
    """Initialise service clients against the configured backend."""

    if base_url is not None:
        _initialise_backend(base_url=base_url or None)
        return

    env_url = os.getenv("DC43_CONTRACTS_APP_BACKEND_URL") or os.getenv(
        "DC43_DEMO_BACKEND_URL"
    )
    if env_url:
        _initialise_backend(base_url=env_url)
        return

    config = config or _current_config().backend
    mode = (config.mode or "embedded").lower()
    if mode == "remote":
        target_url = config.base_url or config.process.url()
        _initialise_backend(base_url=target_url)
    else:
        _initialise_backend(base_url=None)


def _setup_state_path() -> Path:
    """Return the path that stores onboarding progress information."""

    workspace = current_workspace()
    return workspace.root / "setup_state.json"


def _default_setup_state() -> Dict[str, Any]:
    """Return the default onboarding wizard payload."""

    return {
        "current_step": 1,
        "selected_options": {},
        "configuration": {},
        "completed": False,
    }


def load_setup_state() -> Dict[str, Any]:
    """Read the persisted onboarding state if available."""

    path = _setup_state_path()
    with _SETUP_STATE_LOCK:
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            payload = {}
    state = _default_setup_state()
    if isinstance(payload, Mapping):
        state.update(
            {
                "current_step": int(payload.get("current_step") or 1),
                "selected_options": dict(payload.get("selected_options") or {}),
                "configuration": dict(payload.get("configuration") or {}),
                "completed": bool(payload.get("completed")),
            }
        )
        completed_at = payload.get("completed_at") if isinstance(payload, Mapping) else None
        if isinstance(completed_at, str):
            state["completed_at"] = completed_at
    return state


def save_setup_state(state: Mapping[str, Any]) -> None:
    """Persist onboarding progress to the workspace."""

    path = _setup_state_path()
    serialisable = dict(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _SETUP_STATE_LOCK:
        path.write_text(json.dumps(serialisable, indent=2, sort_keys=True))


def reset_setup_state() -> Dict[str, Any]:
    """Reset onboarding progress and persist the default payload."""

    state = _default_setup_state()
    save_setup_state(state)
    return state


def is_setup_complete() -> bool:
    """Return ``True`` when the onboarding flow has been marked as complete."""

    state = load_setup_state()
    return bool(state.get("completed"))


def configure_from_config(config: ContractsAppConfig | None = None) -> ContractsAppConfig:
    """Apply ``config`` to initialise workspace and backend defaults."""

    config = config or load_config()
    workspace_root = config.workspace.root
    default_root = str(workspace_root) if workspace_root else None
    workspace, _ = workspace_from_env(default_root=default_root)
    configure_workspace(workspace)
    configure_backend(config=config.backend)
    return _set_active_config(config)


# Ensure module-level paths and backend clients are ready for import-time users.
configure_from_config()


def _wait_for_backend(base_url: str, timeout: float = 30.0) -> None:
    """Block until the backend responds or ``timeout`` elapses."""

    deadline = time.monotonic() + timeout
    probe_url = f"{base_url.rstrip('/')}/openapi.json"
    with httpx.Client(timeout=2.0) as client:
        while True:
            try:
                response = client.get(probe_url)
                if response.status_code < 500:
                    return
            except httpx.HTTPError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Backend at {base_url} failed to start within {timeout}s")
            time.sleep(0.2)


async def _expectation_predicates(contract: OpenDataContractStandard) -> Dict[str, str]:
    plan = await asyncio.to_thread(dq_service.describe_expectations, contract=contract)
    mapping: Dict[str, str] = {}
    for item in plan:
        key = item.get("key") if isinstance(item, Mapping) else None
        predicate = item.get("predicate") if isinstance(item, Mapping) else None
        if isinstance(key, str) and isinstance(predicate, str):
            mapping[key] = predicate
    return mapping

router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


SETUP_MODULES: Dict[str, Dict[str, Any]] = {
    "contracts_backend": {
        "title": "Contracts backend",
        "summary": "Where contract definitions are stored and served.",
        "options": {
            "embedded_fs": {
                "label": "Embedded filesystem (local demo)",
                "description": (
                    "Use the built-in filesystem store. Ideal for Docker based demos "
                    "or quick local exploration."
                ),
                "installation": [
                    "Create a shared volume (for example `./volumes/contracts`) and mount it inside the container.",
                    "No external services are required; the FastAPI app will write JSON contracts locally.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_APP_BACKEND_MODE=embedded` in your `.env` file or docker compose overrides.",
                    "Point `DC43_CONTRACTS_APP_WORK_DIR` to the directory configured below so that the UI and backend share storage.",
                ],
                "fields": [
                    {
                        "name": "contracts_dir",
                        "label": "Contracts directory",
                        "placeholder": "/workspace/contracts",
                        "help": "Absolute path inside the container that will hold JSON contract files.",
                        "default_factory": lambda workspace: str(workspace.contracts_dir),
                    },
                    {
                        "name": "work_dir",
                        "label": "Workspace root",
                        "placeholder": "/workspace",
                        "help": "Root path bound to `DC43_CONTRACTS_APP_WORK_DIR` for local assets.",
                        "default_factory": lambda workspace: str(workspace.root),
                    },
                ],
            },
            "remote_http": {
                "label": "Remote HTTP service",
                "description": (
                    "Connect to an existing dc43-service-backends deployment exposed over HTTP."
                ),
                "installation": [
                    "Deploy `dc43-service-backends` to your preferred environment (Kubernetes, ECS, VM, …).",
                    "Expose the `/contracts` API via HTTPS. Ensure the container can resolve the hostname.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_APP_BACKEND_MODE=remote` to stop the UI from starting the embedded backend.",
                    "Provide the base URL below via `DC43_CONTRACTS_APP_BACKEND_URL` or `config/backend.base_url` in TOML.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Service base URL",
                        "placeholder": "https://contracts.example.com",
                        "help": "Public URL for the remote contracts service (no trailing slash).",
                    },
                    {
                        "name": "auth_token",
                        "label": "API token",
                        "placeholder": "Optional bearer token",
                        "help": "Token injected into the `Authorization` header when talking to the remote service.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "governance_catalog": {
        "title": "Governance catalog",
        "summary": "Where datasets and ownership metadata live.",
        "options": {
            "collibra_cloud": {
                "label": "Collibra Data Governance",
                "description": "Synchronise metadata with a Collibra Cloud environment.",
                "installation": [
                    "Provision a Collibra Cloud site with the Data Quality & Observability package enabled.",
                    "Create a service account with access to the community that will hold your contracts.",
                ],
                "configuration_notes": [
                    "Populate the credentials below and export them as `COLLIBRA_CLIENT_ID` / `COLLIBRA_CLIENT_SECRET`.",
                    "Point the integration helper at the domain identifier where contracts should be catalogued.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Site URL",
                        "placeholder": "https://acme.collibra.com",
                        "help": "Base URL of your Collibra instance.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "collibra-service-client",
                        "help": "OAuth client id for the automation user.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "OAuth client secret (store securely).",
                    },
                    {
                        "name": "domain_id",
                        "label": "Target domain",
                        "placeholder": "DATA_PRODUCTS",
                        "help": "Collibra domain id that will contain the registered contracts.",
                    },
                ],
            },
            "databricks_unity": {
                "label": "Databricks Unity Catalog",
                "description": "Register data products into Unity Catalog tables and views.",
                "installation": [
                    "Enable Unity Catalog on your Databricks workspace and assign the automation principal to the catalog.",
                    "Install the `dc43-integrations` wheel on the cluster that will sync governance metadata.",
                ],
                "configuration_notes": [
                    "Provide the workspace information below and export them as `DATABRICKS_HOST`/`DATABRICKS_TOKEN`.",
                    "The catalog and schema determine where Delta tables for contracts will be created.",
                ],
                "fields": [
                    {
                        "name": "workspace_url",
                        "label": "Workspace URL",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL (without `/api/2.0`) for the Databricks workspace.",
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog",
                        "placeholder": "main",
                        "help": "Unity Catalog that will host the governance artifacts.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Schema inside the catalog used for contract tables.",
                    },
                    {
                        "name": "token",
                        "label": "Personal access token",
                        "placeholder": "dapi...",
                        "help": "Databricks PAT with Unity Catalog permissions.",
                    },
                ],
            },
            "delta_sharing": {
                "label": "Delta Sharing catalog",
                "description": "Push contract metadata to a Delta Lake table exposed via Delta Sharing.",
                "installation": [
                    "Create or identify a Delta table that will catalogue contract releases.",
                    "Expose the table via Delta Sharing or grant read access to the consuming platforms.",
                ],
                "configuration_notes": [
                    "The profile path below is read by the `delta-sharing` client used during synchronisation.",
                    "Share and table names determine where each published contract is recorded.",
                ],
                "fields": [
                    {
                        "name": "profile_path",
                        "label": "Profile (.share) path",
                        "placeholder": "/secrets/delta/profile.share",
                        "help": "Filesystem path to the Delta Sharing profile used for authentication.",
                    },
                    {
                        "name": "share",
                        "label": "Share name",
                        "placeholder": "contracts_share",
                        "help": "Delta Sharing share that exposes the governance table.",
                    },
                    {
                        "name": "table",
                        "label": "Table",
                        "placeholder": "governance.contract_registry",
                        "help": "Fully qualified table name that stores contract metadata.",
                    },
                ],
            },
        },
    },
    "data_quality": {
        "title": "Data quality engine",
        "summary": "How data quality rules are executed and reported.",
        "options": {
            "local_expectations": {
                "label": "Local expectations bundle",
                "description": "Run expectations packaged with the application on the filesystem.",
                "installation": [
                    "Create a directory alongside the workspace to store expectation YAML files.",
                    "Populate the folder with templates from `tests/fixtures/expectations` as a starting point.",
                ],
                "configuration_notes": [
                    "Point `DC43_DEMO_EXPECTATIONS_DIR` (or your own env var) to the directory below.",
                    "Use the integration helper to regenerate expectations whenever a contract changes.",
                ],
                "fields": [
                    {
                        "name": "expectations_path",
                        "label": "Expectations directory",
                        "placeholder": "/workspace/expectations",
                        "help": "Location where `.yml` or `.json` expectation suites live.",
                        "default_factory": lambda workspace: str(workspace.records_dir / "expectations"),
                    },
                ],
            },
            "remote_service": {
                "label": "Remote validation service",
                "description": "Delegate validation runs to a hosted dc43 data-quality API.",
                "installation": [
                    "Deploy `dc43-service-backends` with the data-quality component enabled (Spark or SQL).",
                    "Ensure the contracts service and the quality API share the same backing storage.",
                ],
                "configuration_notes": [
                    "Configure the base URL below via `DC43_DATA_QUALITY_URL` or service discovery.",
                    "Optional API keys are injected into the `X-API-Key` header on each request.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Quality service URL",
                        "placeholder": "https://quality.example.com",
                        "help": "Public endpoint for the remote data quality API.",
                    },
                    {
                        "name": "api_key",
                        "label": "API key",
                        "placeholder": "Optional secret",
                        "help": "Shared secret exchanged with the remote validation service.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "compute_orchestration": {
        "title": "Compute & orchestration",
        "summary": "Where data pipelines are executed and monitored.",
        "options": {
            "local_spark": {
                "label": "Local Spark / Delta Lake",
                "description": "Use an in-container Spark runtime materialising Delta tables on disk.",
                "installation": [
                    "Install PySpark (`pip install pyspark==3.5.1 delta-spark`).",
                    "Create a datasets folder (e.g. `./volumes/delta`) and mount it at the path below.",
                ],
                "configuration_notes": [
                    "Set `SPARK_MASTER` and `DELTA_TABLE_PATH` environment variables using the values provided.",
                    "Ensure the contracts workspace has permission to write Delta transaction logs.",
                ],
                "fields": [
                    {
                        "name": "spark_master",
                        "label": "Spark master",
                        "placeholder": "local[*]",
                        "help": "Master URL passed to Spark (e.g. `local[*]` or `spark://host:7077`).",
                        "default": "local[*]",
                    },
                    {
                        "name": "delta_path",
                        "label": "Delta storage path",
                        "placeholder": "/workspace/datasets",
                        "help": "Directory that will host Delta tables produced by the demo pipelines.",
                        "default_factory": lambda workspace: str(workspace.data_dir),
                    },
                ],
            },
            "databricks_jobs": {
                "label": "Databricks Jobs",
                "description": "Trigger Databricks Jobs or Workflows for contract compliant pipelines.",
                "installation": [
                    "Upload the demo wheel to your Databricks workspace and create a Jobs run configuration.",
                    "Grant the automation principal access to the job and target clusters or Delta Live Tables pipeline.",
                ],
                "configuration_notes": [
                    "Populate the workspace host and PAT below (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`).",
                    "Use the job identifier so the UI can call the Jobs API when running validations.",
                ],
                "fields": [
                    {
                        "name": "workspace_url",
                        "label": "Workspace URL",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL for the Databricks workspace that hosts the job.",
                    },
                    {
                        "name": "job_id",
                        "label": "Job or pipeline id",
                        "placeholder": "123456789",
                        "help": "Identifier of the Databricks Job or Delta Live Tables pipeline.",
                    },
                    {
                        "name": "token",
                        "label": "Personal access token",
                        "placeholder": "dapi...",
                        "help": "Token used to authenticate against the Databricks Jobs API.",
                    },
                ],
            },
        },
    },
}


_SETUP_TOTAL_STEPS = 3


def _setup_progress(step: int) -> int:
    """Return a progress percentage for the onboarding wizard."""

    clamped = max(1, min(_SETUP_TOTAL_STEPS, step))
    return int((clamped / _SETUP_TOTAL_STEPS) * 100)


def _requires_configuration(selected: Mapping[str, str], configuration: Mapping[str, Any]) -> bool:
    """Return ``True`` when mandatory fields are missing from ``configuration``."""

    for module_key, option_key in selected.items():
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        config_values = configuration.get(module_key, {})
        for field_meta in option_meta.get("fields", []):
            if field_meta.get("optional"):
                continue
            value = str(config_values.get(field_meta.get("name"), "") or "").strip()
            if not value:
                return True
    return False


def _serialise_field(
    module_key: str,
    field_meta: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any],
    workspace: ContractsAppWorkspace,
) -> Dict[str, Any]:
    """Return template-friendly metadata for a setup field."""

    stored = configuration.get(module_key, {}) if isinstance(configuration, Mapping) else {}
    value = str(stored.get(field_meta.get("name"), "") or "")
    if not value:
        if "default" in field_meta and field_meta["default"] is not None:
            value = str(field_meta["default"])
        else:
            default_factory = field_meta.get("default_factory")
            if callable(default_factory):
                try:
                    value = str(default_factory(workspace))
                except Exception:  # pragma: no cover - defensive defaults
                    value = ""
    return {
        "name": field_meta.get("name"),
        "label": field_meta.get("label"),
        "placeholder": field_meta.get("placeholder", ""),
        "help": field_meta.get("help", ""),
        "optional": bool(field_meta.get("optional")),
        "type": field_meta.get("type", "text"),
        "value": value,
    }


def _build_setup_context(
    request: Request,
    state: Mapping[str, Any],
    *,
    step: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return the template context shared by onboarding views."""

    selected_options_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    selected_options: Dict[str, str] = {}
    if isinstance(selected_options_raw, Mapping):
        selected_options = {str(key): str(value) for key, value in selected_options_raw.items()}

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    configuration: Dict[str, Any] = {}
    if isinstance(configuration_raw, Mapping):
        configuration = {str(key): value for key, value in configuration_raw.items()}

    requested_step = int(state.get("current_step") or 1) if step is None else int(step)
    if requested_step > 1 and not selected_options:
        requested_step = 1

    if requested_step >= _SETUP_TOTAL_STEPS and not state.get("completed"):
        if _requires_configuration(selected_options, configuration):
            requested_step = 2

    if state.get("completed") and step is None:
        requested_step = _SETUP_TOTAL_STEPS

    workspace = current_workspace()

    modules: List[Dict[str, Any]] = []
    for module_key, module_meta in SETUP_MODULES.items():
        options: List[Dict[str, Any]] = []
        for option_key, option_meta in module_meta["options"].items():
            options.append(
                {
                    "key": option_key,
                    "label": option_meta.get("label"),
                    "description": option_meta.get("description", ""),
                    "selected": selected_options.get(module_key) == option_key,
                }
            )
        modules.append(
            {
                "key": module_key,
                "title": module_meta.get("title"),
                "summary": module_meta.get("summary", ""),
                "options": options,
            }
        )

    selected_modules: List[Dict[str, Any]] = []
    for module_key, option_key in selected_options.items():
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        fields = [
            _serialise_field(module_key, field_meta, configuration=configuration, workspace=workspace)
            for field_meta in option_meta.get("fields", [])
        ]
        selected_modules.append(
            {
                "key": module_key,
                "title": module_meta.get("title"),
                "summary": module_meta.get("summary", ""),
                "option_key": option_key,
                "option": {
                    "label": option_meta.get("label"),
                    "description": option_meta.get("description", ""),
                    "installation": list(option_meta.get("installation", [])),
                    "configuration_notes": list(option_meta.get("configuration_notes", [])),
                },
                "fields": fields,
            }
        )

    summary_modules: List[Dict[str, Any]] = []
    for module in selected_modules:
        module_key = module["key"]
        module_config = configuration.get(module_key, {}) if isinstance(configuration, Mapping) else {}
        summary_fields: List[Dict[str, Any]] = []
        for field in module.get("fields", []):
            summary_fields.append(
                {
                    "label": field.get("label"),
                    "value": str(module_config.get(field.get("name"), "") or field.get("value", "")),
                    "optional": field.get("optional", False),
                }
            )
        summary_modules.append(
            {
                "title": module.get("title"),
                "option_label": module["option"].get("label") if isinstance(module.get("option"), Mapping) else "",
                "installation": module.get("option", {}).get("installation", []) if isinstance(module.get("option"), Mapping) else [],
                "configuration_notes": module.get("option", {}).get("configuration_notes", []) if isinstance(module.get("option"), Mapping) else [],
                "fields": summary_fields,
            }
        )

    current_step = max(1, min(_SETUP_TOTAL_STEPS, requested_step))

    return {
        "request": request,
        "step": current_step,
        "progress": _setup_progress(current_step),
        "modules": modules,
        "selected_modules": selected_modules,
        "summary_modules": summary_modules,
        "state": state,
        "errors": errors or [],
        "completed": bool(state.get("completed")),
    }


@dataclass
class DatasetRecord:
    contract_id: str
    contract_version: str
    dataset_name: str = ""
    dataset_version: str = ""
    status: str = "unknown"
    dq_details: Dict[str, Any] = field(default_factory=dict)
    run_type: str = "infer"
    violations: int = 0
    reason: str = ""
    draft_contract_version: str | None = None
    scenario_key: str | None = None
    data_product_id: str = ""
    data_product_port: str = ""
    data_product_role: str = ""


_STATUS_BADGES: Dict[str, str] = {
    "kept": "bg-success",
    "updated": "bg-primary",
    "relaxed": "bg-warning text-dark",
    "removed": "bg-danger",
    "added": "bg-info text-dark",
    "missing": "bg-secondary",
    "error": "bg-danger",
    "warning": "bg-warning text-dark",
    "not_nullable": "bg-info text-dark",
}


_DQ_STATUS_BADGES: Dict[str, str] = {
    "ok": "bg-success",
    "warn": "bg-warning text-dark",
    "block": "bg-danger",
    "stale": "bg-secondary",
    "unknown": "bg-secondary",
}


_CONTRACT_STATUS_BADGES: Dict[str, str] = {
    "active": "bg-success",
    "draft": "bg-warning text-dark",
    "deprecated": "bg-secondary",
}


def _dq_version_records(
    dataset_id: str,
    *,
    contract: Optional[OpenDataContractStandard] = None,
    dataset_path: Optional[str] = None,
    dataset_records: Optional[Iterable[DatasetRecord]] = None,
) -> List[Dict[str, Any]]:
    """Return version → status entries for the supplied dataset id.

    ``dataset_records`` can be provided to scope compatibility information to
    runs that were produced for a specific contract version. This ensures, for
    example, that the compatibility matrix rendered for ``orders`` version
    ``1.0.0`` does not surface the validation outcome that belongs to the
    ``1.1.0`` contract.
    """

    records: List[Dict[str, Any]] = []
    entries = _dq_status_entries(dataset_id)

    scoped_versions: set[str] = set()
    dataset_record_map: Dict[str, DatasetRecord] = {}
    if dataset_records:
        for record in dataset_records:
            if not record.dataset_version:
                continue
            scoped_versions.add(record.dataset_version)
            dataset_record_map[record.dataset_version] = record

    dataset_dir = _dataset_root_for(dataset_id, dataset_path)
    skip_fs_check = False
    if contract and contract.servers:
        server = contract.servers[0]
        fmt = (getattr(server, "format", "") or "").lower()
        if fmt == "delta":
            skip_fs_check = True

    seen_versions: set[str] = set()
    for display_version, stored_version, payload in entries:
        record = dataset_record_map.get(display_version)
        payload_contract_id = str(payload.get("contract_id") or "")
        payload_contract_version = str(payload.get("contract_version") or "")
        if contract and (contract.id or contract.version):
            contract_id_value = contract.id or ""
            if payload_contract_id and payload_contract_version:
                if (
                    payload_contract_id != contract_id_value
                    or payload_contract_version != contract.version
                ):
                    continue
            elif scoped_versions and display_version not in scoped_versions:
                continue
        elif scoped_versions and display_version not in scoped_versions:
            continue
        if not skip_fs_check and dataset_dir is not None:
            if not _has_version_materialisation(dataset_dir, display_version):
                continue
        status_value = str(payload.get("status", "unknown") or "unknown")
        records.append(
            {
                "version": display_version,
                "stored_version": stored_version,
                "status": status_value,
                "status_label": status_value.replace("_", " ").title(),
                "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
                "contract_id": payload_contract_id or (record.contract_id if record else ""),
                "contract_version": payload_contract_version
                or (record.contract_version if record else ""),
                "recorded_at": payload.get("recorded_at"),
            }
        )
        seen_versions.add(display_version)

    # If we scoped by contract runs, surface any versions without a stored DQ
    # payload using the dataset records so the UI can still display a verdict.
    if scoped_versions:
        for missing_version in scoped_versions - seen_versions:
            record = dataset_record_map.get(missing_version)
            status_value = str(record.status or "unknown") if record else "unknown"
            records.append(
                {
                    "version": missing_version,
                    "stored_version": _safe_fs_name(missing_version),
                    "status": status_value,
                    "status_label": status_value.replace("_", " ").title(),
                    "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
                    "contract_id": record.contract_id if record else "",
                    "contract_version": record.contract_version if record else "",
                    "recorded_at": None,
                }
            )

    records.sort(key=lambda item: _version_sort_key(item["version"]))
    return records


def _server_details(contract: OpenDataContractStandard) -> Optional[Dict[str, Any]]:
    """Summarise the first server entry for UI consumption."""

    if not contract.servers:
        return None
    first = contract.servers[0]
    custom: Dict[str, Any] = custom_properties_dict(first)
    dataset_id = contract.id or getattr(first, "dataset", None) or contract.id
    info: Dict[str, Any] = {
        "server": getattr(first, "server", ""),
        "type": getattr(first, "type", ""),
        "format": getattr(first, "format", ""),
        "path": getattr(first, "path", ""),
        "dataset": getattr(first, "dataset", ""),
        "dataset_id": dataset_id,
    }
    if custom:
        info["custom"] = custom
        if "dc43.core.versioning" in custom:
            info["versioning"] = custom.get("dc43.core.versioning")
        if "dc43.pathPattern" in custom:
            info["path_pattern"] = custom.get("dc43.pathPattern")
    return info


def _format_scope(scope: str | None) -> str:
    """Return a human readable label for change log scopes."""

    if not scope or scope == "contract":
        return "Contract"
    if scope.startswith("field:"):
        return f"Field {scope.split(':', 1)[1]}"
    return scope.replace("_", " ").title()


def _stringify_value(value: Any) -> str:
    """Return a readable representation for rule parameter values."""

    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    return str(value)


def _quality_rule_summary(dq: DataQuality) -> Dict[str, Any]:
    """Produce a structured summary for a data-quality rule."""

    conditions: List[str] = []
    if dq.description:
        conditions.append(str(dq.description))

    if dq.mustBeGreaterThan is not None:
        conditions.append(f"Value must be greater than {dq.mustBeGreaterThan}")
    if dq.mustBeGreaterOrEqualTo is not None:
        conditions.append(f"Value must be greater than or equal to {dq.mustBeGreaterOrEqualTo}")
    if dq.mustBeLessThan is not None:
        conditions.append(f"Value must be less than {dq.mustBeLessThan}")
    if dq.mustBeLessOrEqualTo is not None:
        conditions.append(f"Value must be less than or equal to {dq.mustBeLessOrEqualTo}")
    if dq.mustBeBetween:
        low, high = dq.mustBeBetween
        conditions.append(f"Value must be between {low} and {high}")
    if dq.mustNotBeBetween:
        low, high = dq.mustNotBeBetween
        conditions.append(f"Value must not be between {low} and {high}")

    if dq.mustBe is not None:
        if (dq.rule or "").lower() == "regex":
            conditions.append(f"Value must match the pattern {dq.mustBe}")
        elif isinstance(dq.mustBe, (list, tuple, set)):
            conditions.append(
                "Value must be one of: " + ", ".join(str(item) for item in dq.mustBe)
            )
        else:
            conditions.append(f"Value must be {_stringify_value(dq.mustBe)}")

    if dq.mustNotBe is not None:
        if isinstance(dq.mustNotBe, (list, tuple, set)):
            conditions.append(
                "Value must not be any of: "
                + ", ".join(str(item) for item in dq.mustNotBe)
            )
        else:
            conditions.append(f"Value must not be {_stringify_value(dq.mustNotBe)}")

    if dq.query:
        engine = (dq.engine or "spark_sql").replace("_", " ")
        conditions.append(f"Query ({engine}): {dq.query}")

    if not conditions:
        label = dq.rule or dq.name or "rule"
        conditions.append(f"See contract metadata for details on {label}.")

    title = dq.name or dq.rule or "Rule"
    title = title.replace("_", " ").title()

    return {
        "title": title,
        "conditions": conditions,
        "severity": dq.severity,
        "dimension": dq.dimension,
    }


def _field_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return quality rule summaries grouped per field."""

    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        for prop in obj.properties or []:
            rules: List[Dict[str, Any]] = []
            if prop.required:
                rules.append(
                    {
                        "title": "Required",
                        "conditions": [
                            "Field must always be present (non-null values required)."
                        ],
                    }
                )
            if prop.unique:
                rules.append(
                    {
                        "title": "Unique",
                        "conditions": [
                            "Each record must contain a distinct value for this field.",
                        ],
                    }
                )
            for dq in prop.quality or []:
                rules.append(_quality_rule_summary(dq))

            sections.append(
                {
                    "name": prop.name or "",
                    "type": prop.physicalType or "",
                    "required": bool(prop.required),
                    "rules": rules,
                }
            )
    return sections


def _dataset_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return dataset-level quality rules defined on schema objects."""

    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        rules = [_quality_rule_summary(dq) for dq in obj.quality or []]
        if rules:
            sections.append({"name": obj.name or contract.id or "dataset", "rules": rules})
    return sections


def _summarise_change_entry(entry: Mapping[str, Any]) -> str:
    details = entry.get("details")
    if isinstance(details, Mapping):
        for key in ("message", "reason"):
            message = details.get(key)
            if message:
                return str(message)
    target = entry.get("constraint") or entry.get("rule") or entry.get("kind")
    status = entry.get("status")
    if target and status:
        return f"{str(target).replace('_', ' ').title()} {str(status).replace('_', ' ')}."
    if status:
        return str(status).replace("_", " ").title()
    return ""


def _contract_change_log(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Extract change log entries from the contract custom properties."""

    entries: List[Dict[str, Any]] = []
    for prop in normalise_custom_properties(contract.customProperties):
        if isinstance(prop, Mapping):
            key = prop.get("property")
            value = prop.get("value")
        else:
            key = getattr(prop, "property", None)
            value = getattr(prop, "value", None)
        if key != "draft_change_log":
            continue
        try:
            items = list(value or [])
        except TypeError:
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            details = item.get("details")
            details_text = ""
            if details is not None:
                try:
                    details_text = json.dumps(details, indent=2, sort_keys=True, default=str)
                except TypeError:
                    details_text = str(details)
            status = str(item.get("status", ""))
            entries.append(
                {
                    "scope": item.get("scope", ""),
                    "scope_label": _format_scope(item.get("scope")),
                    "kind": item.get("kind", ""),
                    "status": status,
                    "status_label": status.replace("_", " ").title(),
                    "constraint": item.get("constraint"),
                    "rule": item.get("rule"),
                    "summary": _summarise_change_entry(item),
                    "details_text": details_text,
                }
            )
        break
    return entries


def load_records() -> List[DatasetRecord]:
    if not DATASETS_FILE.exists():
        return []
    try:
        raw = json.loads(DATASETS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord]) -> None:
    DATASETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATASETS_FILE.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


def _scenario_dataset_name(params: Mapping[str, Any]) -> str:
    """Return the expected output dataset for a scenario."""

    dataset_name = params.get("dataset_name")
    if dataset_name:
        return str(dataset_name)
    contract_id = params.get("contract_id")
    if contract_id:
        return str(contract_id)
    dataset_id = params.get("dataset_id")
    if dataset_id:
        return str(dataset_id)
    return "result"


def scenario_run_rows(
    records: Iterable[DatasetRecord],
    scenarios: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Return scenario metadata enriched with the latest recorded run."""

    by_dataset: Dict[str, List[DatasetRecord]] = {}
    by_scenario: Dict[str, List[DatasetRecord]] = {}
    for record in records:
        if record.dataset_name:
            by_dataset.setdefault(record.dataset_name, []).append(record)
        if record.scenario_key:
            by_scenario.setdefault(record.scenario_key, []).append(record)

    for entries in by_dataset.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    for entries in by_scenario.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))

    rows: List[Dict[str, Any]] = []
    for key, cfg in scenarios.items():
        params: Mapping[str, Any] = cfg.get("params", {})
        dataset_name = _scenario_dataset_name(params)
        dataset_records: List[DatasetRecord] = list(by_scenario.get(key, []))

        if not dataset_records:
            candidate_records = by_dataset.get(dataset_name, [])
            if candidate_records:
                contract_id = params.get("contract_id")
                contract_version = params.get("contract_version")
                run_type = params.get("run_type")
                filtered: List[DatasetRecord] = []
                for record in candidate_records:
                    if record.scenario_key:
                        continue
                    if contract_id and record.contract_id and record.contract_id != contract_id:
                        continue
                    if (
                        contract_version
                        and record.contract_version
                        and record.contract_version != contract_version
                    ):
                        continue
                    if run_type and record.run_type and record.run_type != run_type:
                        continue
                    filtered.append(record)
                if filtered:
                    dataset_records = filtered
                else:
                    dataset_records = [rec for rec in candidate_records if not rec.scenario_key]

        dataset_records = list(dataset_records)
        dataset_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
        latest_record = dataset_records[-1] if dataset_records else None

        rows.append(
            {
                "key": key,
                "label": cfg.get("label", key.replace("-", " ").title()),
                "description": cfg.get("description"),
                "diagram": cfg.get("diagram"),
                "dataset_name": dataset_name,
                "contract_id": params.get("contract_id"),
                "contract_version": params.get("contract_version"),
                "run_type": params.get("run_type", "infer"),
                "run_count": len(dataset_records),
                "latest": latest_record.__dict__.copy() if latest_record else None,
            }
        )

    return rows


_FLASH_LOCK = Lock()
_FLASH_MESSAGES: Dict[str, Dict[str, str | None]] = {}


def queue_flash(message: str | None = None, error: str | None = None) -> str:
    """Store a transient flash payload and return a lookup token."""

    token = uuid4().hex
    with _FLASH_LOCK:
        _FLASH_MESSAGES[token] = {"message": message, "error": error}
    return token


def pop_flash(token: str) -> Tuple[str | None, str | None]:
    """Return and remove the flash payload associated with ``token``."""

    with _FLASH_LOCK:
        payload = _FLASH_MESSAGES.pop(token, None) or {}
    return payload.get("message"), payload.get("error")


def load_contract_meta() -> List[Dict[str, Any]]:
    """Return contract info derived from the store without extra metadata."""
    meta: List[Dict[str, Any]] = []
    for cid in store.list_contracts():
        for ver in store.list_versions(cid):
            try:
                contract = store.get(cid, ver)
            except FileNotFoundError:
                continue
            server = (contract.servers or [None])[0]
            path = ""
            if server:
                parts: List[str] = []
                if getattr(server, "path", None):
                    parts.append(server.path)
                if getattr(server, "dataset", None):
                    parts.append(server.dataset)
                path = "/".join(parts)
            meta.append({"id": cid, "version": ver, "path": path})
    return meta


def save_contract_meta(meta: List[Dict[str, Any]]) -> None:
    """No-op retained for backwards compatibility."""
    return None


def contract_to_dict(c: OpenDataContractStandard) -> Dict[str, Any]:
    """Return a plain dict for a contract using public field aliases."""
    try:
        return c.model_dump(by_alias=True, exclude_none=True)
    except AttributeError:  # pragma: no cover - Pydantic v1 fallback
        return c.dict(by_alias=True, exclude_none=True)  # type: ignore[call-arg]


def _flatten_schema_entries(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return a flattened list of schema properties for UI displays."""

    entries: List[Dict[str, Any]] = []
    for obj in getattr(contract, "schema_", None) or []:
        object_name = str(getattr(obj, "name", "") or "")
        prefix = f"{object_name}." if object_name else ""
        for prop in getattr(obj, "properties", None) or []:
            field_name = str(getattr(prop, "name", "") or "")
            full_name = f"{prefix}{field_name}".strip(".")
            entries.append(
                {
                    "field": full_name,
                    "object": object_name,
                    "name": field_name,
                    "physicalType": getattr(prop, "physicalType", "") or "",
                    "logicalType": getattr(prop, "logicalType", "") or "",
                    "required": bool(getattr(prop, "required", False)),
                    "description": getattr(prop, "description", "") or "",
                    "businessName": getattr(prop, "businessName", "") or "",
                }
            )
    return entries


def _integration_catalog() -> List[Dict[str, Any]]:
    """Return basic metadata for all stored contracts."""

    catalog: List[Dict[str, Any]] = []
    for cid in sorted(store.list_contracts()):
        try:
            versions = store.list_versions(cid)
        except FileNotFoundError:
            continue
        sorted_versions = _sorted_versions(versions)
        if not sorted_versions:
            continue
        latest_contract: Optional[OpenDataContractStandard] = None
        for version in reversed(sorted_versions):
            try:
                latest_contract = store.get(cid, version)
                break
            except FileNotFoundError:
                continue
        description = ""
        status = ""
        name = ""
        if latest_contract is not None:
            name = getattr(latest_contract, "name", "") or ""
            if getattr(latest_contract, "description", None):
                description = getattr(latest_contract.description, "usage", "") or ""
            status = getattr(latest_contract, "status", "") or ""
        catalog.append(
            {
                "id": cid,
                "name": name or cid,
                "description": description,
                "versions": sorted_versions,
                "latestVersion": sorted_versions[-1],
                "status": status,
            }
        )
    return catalog


@dataclass
class IntegrationContractContext:
    """Container storing contract objects alongside serialized metadata."""

    contract: OpenDataContractStandard
    summary: Dict[str, Any]


async def _load_integration_contract(cid: str, ver: str) -> IntegrationContractContext:
    """Return the contract and summary information for helper endpoints."""

    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    expectations = await _expectation_predicates(contract)
    server_info = _server_details(contract)
    description = ""
    if getattr(contract, "description", None):
        description = getattr(contract.description, "usage", "") or ""
    schema_entries = _flatten_schema_entries(contract)
    summary: Dict[str, Any] = {
        "id": cid,
        "version": ver,
        "name": getattr(contract, "name", "") or cid,
        "description": description,
        "server": jsonable_encoder(server_info) if server_info else None,
        "expectations": expectations,
        "schemaEntries": schema_entries,
        "fieldCount": len(schema_entries),
        "datasetId": (server_info.get("dataset_id") if server_info else contract.id or cid),
    }
    return IntegrationContractContext(contract=contract, summary=summary)


def _normalise_selection(entries: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    """Normalise payload selections into ``contract_id``/``version`` pairs."""

    result: List[Dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        cid = entry.get("contract_id") or entry.get("contractId") or entry.get("id")
        ver = entry.get("version")
        if not cid or not ver:
            raise HTTPException(status_code=422, detail="contract_id and version are required")
        result.append({"contract_id": str(cid), "version": str(ver)})
    return result


_IDENTIFIER_SANITISER = re.compile(r"[^0-9A-Za-z_]")


def _sanitise_identifier(value: str, default: str) -> str:
    """Return a Python identifier derived from ``value``."""

    candidate = _IDENTIFIER_SANITISER.sub("_", value)
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        candidate = default
    if candidate[0].isdigit():
        candidate = f"{default}_{candidate}"
    return candidate.lower()


def _summarise_predicates(expectations: Mapping[str, str]) -> str:
    """Return a human-friendly summary of SQL predicates."""

    if not expectations:
        return ""
    parts = [f"{key}: {value}" for key, value in expectations.items()]
    return textwrap.shorten("; ".join(parts), width=160, placeholder=" …")


def _normalise_read_strategy(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalise the requested read strategy."""

    raw_mode = (payload or {}).get("mode")
    mode = str(raw_mode or "status").lower()
    if mode not in {"status", "strict"}:
        raise HTTPException(status_code=400, detail=f"Unsupported read strategy: {mode}")
    return {"mode": mode}


def _normalise_write_strategy(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalise the requested write strategy."""

    data = dict(payload or {})
    mode_raw = data.get("mode")
    mode = str(mode_raw or "split").lower()
    if mode == "noop":
        return {"mode": "noop"}

    include_valid = bool(data.get("include_valid", True))
    include_reject = bool(data.get("include_reject", True))
    if not include_valid and not include_reject:
        include_valid = True
    if mode == "split":
        return {
            "mode": "split",
            "include_valid": include_valid,
            "include_reject": include_reject,
        }
    if mode == "strict":
        return {
            "mode": "strict",
            "include_valid": include_valid,
            "include_reject": include_reject,
            "fail_on_warnings": bool(data.get("fail_on_warnings", False)),
        }
    raise HTTPException(status_code=400, detail=f"Unsupported write strategy: {mode}")


def _spark_stub_for_selection(
    inputs: List[Dict[str, str]],
    outputs: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    *,
    read_strategy: Mapping[str, Any],
    write_strategy: Mapping[str, Any],
) -> str:
    """Return a Spark pipeline stub tailored to the selected contracts."""

    read_mode = str(read_strategy.get("mode") or "status").lower()
    write_mode = str(write_strategy.get("mode") or "split").lower()

    violation_imports: List[str] = []
    if outputs:
        if write_mode == "noop":
            violation_imports.append("NoOpWriteViolationStrategy")
        else:
            violation_imports.append("SplitWriteViolationStrategy")
            if write_mode == "strict":
                violation_imports.append("StrictWriteViolationStrategy")

    lines: List[str] = [
        "from pyspark.sql import SparkSession",
        "from dc43_integrations.spark.io import read_with_contract, write_with_contract",
    ]
    lines.append(
        "# Contract status guardrails reject draft/deprecated contracts unless the strategies opt in."
    )
    if violation_imports:
        unique_violation_imports = ", ".join(dict.fromkeys(violation_imports))
        lines.append(
            "from dc43_integrations.spark.violation_strategy import "
            + unique_violation_imports
        )
    lines.extend(
        [
            "from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient",
            "from dc43_service_clients.data_quality.client.remote import RemoteDataQualityServiceClient",
            "from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient",
            "",
            "# Generated by the DC43 integration helper",
            'BASE_URL = "http://dc43-services"',
            "",
            "contract_client = RemoteContractServiceClient(base_url=BASE_URL)",
            "dq_client = RemoteDataQualityServiceClient(base_url=BASE_URL)",
            "governance_client = RemoteGovernanceServiceClient(base_url=BASE_URL)",
            "",
            'spark = SparkSession.builder.appName("dc43-pipeline").getOrCreate()',
        ]
    )

    if outputs:
        lines.append("")
        if write_mode == "noop":
            lines.extend(
                [
                    "# NoOpWriteViolationStrategy keeps writes in a single target dataset.",
                    "# Pass allowed_contract_statuses=(\"active\", \"draft\") to allow draft contracts in development.",
                    "write_strategy = NoOpWriteViolationStrategy()",
                ]
            )
        else:
            include_valid = bool(write_strategy.get("include_valid", True))
            include_reject = bool(write_strategy.get("include_reject", True))
            include_valid_flag = "True" if include_valid else "False"
            include_reject_flag = "True" if include_reject else "False"
            lines.extend(
                [
                    "# SplitWriteViolationStrategy routes rows based on the contract predicates.",
                    "split_strategy = SplitWriteViolationStrategy(",
                    '    valid_suffix="valid",',
                    '    reject_suffix="reject",',
                    f"    include_valid={include_valid_flag},",
                    f"    include_reject={include_reject_flag},",
                    ")",
                ]
            )
            if write_mode == "strict":
                fail_on_warnings = bool(write_strategy.get("fail_on_warnings", False))
                fail_flag = "True" if fail_on_warnings else "False"
                lines.extend(
                    [
                        "",
                        "# StrictWriteViolationStrategy escalates contract issues to failures.",
                        "write_strategy = StrictWriteViolationStrategy(",
                        "    base=split_strategy,",
                        f"    fail_on_warnings={fail_flag},",
                        ")",
                    ]
                )
            else:
                lines.extend(["", "write_strategy = split_strategy"])

    input_vars: List[str] = []
    for index, entry in enumerate(inputs, start=1):
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server_raw = summary.get("server") or {}
        server = dict(server_raw) if isinstance(server_raw, Mapping) else {}
        location = server.get("path") or server.get("dataset")
        fmt = server.get("format")
        base_name = _sanitise_identifier(summary["id"], f"input{index}")
        df_var = f"{base_name}_df"
        status_var = f"{base_name}_status"
        input_vars.append(df_var)

        lines.extend(
            [
                "",
                f"# Input: {summary['id']} {summary['version']} ({summary['datasetId']})",
            ]
        )
        if location:
            lines.append(f"#   Location: {location}")
        if fmt:
            lines.append(f"#   Format: {fmt}")
        lines.extend(
            [
                f"{df_var}, {status_var} = read_with_contract(",
                "    spark,",
                f"    contract_id={summary['id']!r},",
                f"    expected_contract_version=\"=={summary['version']}\",",
                "    contract_service=contract_client,",
                "    data_quality_service=dq_client,",
            ]
        )
        if server.get("dataset"):
            lines.append(f"    table={server['dataset']!r},")
        lines.extend(
            [
                "    enforce=True,",
                "    auto_cast=True,",
                "    return_status=True,",
                "    # status_strategy=DefaultReadStatusStrategy(allowed_contract_statuses=(\"active\", \"draft\")),",
                ")",
                "",
            ]
        )
        if read_mode == "strict":
            lines.extend(
                [
                    f"if {status_var} and {status_var}.status != \"ok\":",
                    "    raise RuntimeError(",
                    f"        f\"{summary['id']} status: {{{status_var}.status}} {{{status_var}.reason or ''}}\"",
                    "    )",
                ]
            )
        else:
            lines.extend(
                [
                    f"if {status_var} and {status_var}.status != \"ok\":",
                    f"    print(\"{summary['id']} status:\", {status_var}.status, {status_var}.reason or \"\")",
                ]
            )

    if input_vars:
        primary_df = input_vars[0]
        lines.extend(
            [
                "",
                "# TODO: implement business logic for the loaded dataframes",
            ]
        )
        if len(input_vars) > 1:
            lines.append("# Available inputs: " + ", ".join(input_vars))
        lines.append(f"transformed_df = {primary_df}  # replace with your transformations")
    else:
        lines.extend(
            [
                "",
                "# TODO: create a dataframe that matches the output contract schema",
                "transformed_df = spark.createDataFrame([], schema=None)",
            ]
        )

    for index, entry in enumerate(outputs, start=1):
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server_raw = summary.get("server") or {}
        server = dict(server_raw) if isinstance(server_raw, Mapping) else {}
        fmt = server.get("format")
        base_name = _sanitise_identifier(summary["id"], f"output{index}")
        validation_var = f"{base_name}_validation"
        status_var = f"{base_name}_status"
        location = server.get("path") or server.get("dataset")

        lines.extend(
            [
                "",
                f"# Output: {summary['id']} {summary['version']} ({summary['datasetId']})",
            ]
        )
        if location:
            lines.append(f"#   Location: {location}")
        if fmt:
            lines.append(f"#   Format: {fmt}")
        lines.extend(
            [
                f"{validation_var}, {status_var} = write_with_contract(",
                "    df=transformed_df,  # TODO: replace with dataframe for this output",
                f"    contract_id={summary['id']!r},",
                f"    expected_contract_version=\"=={summary['version']}\",",
                "    contract_service=contract_client,",
                "    data_quality_service=dq_client,",
                "    governance_service=governance_client,",
            ]
        )
        if server.get("dataset"):
            lines.append(f"    table={server['dataset']!r},")
        lines.extend(
            [
                "    violation_strategy=write_strategy,",
                "    return_status=True,",
                ")",
                "",
                f"if {status_var}:",
                f"    print(\"{summary['id']} governance status:\", {status_var}.status)",
                f"print(\"{summary['id']} write validation ok:\", {validation_var}.ok)",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _read_strategy_notes(
    selections: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    strategy: Mapping[str, Any],
) -> List[Dict[str, str]]:
    """Describe how read strategies are applied for the helper UI."""

    mode = str(strategy.get("mode") or "status").lower()
    if mode == "strict":
        intro = (
            "read_with_contract(... return_status=True) enforces schema alignment and the stub "
            "raises a RuntimeError whenever validation verdicts are not OK."
        )
    else:
        intro = (
            "read_with_contract(... return_status=True) enforces schema alignment and logs non-OK "
            "statuses so orchestration can branch on data quality verdicts."
        )
    intro += " Non-active contract statuses raise unless the strategy explicitly allows them."
    notes: List[Dict[str, str]] = [
        {
            "title": "Contract-aware reads",
            "description": intro,
        }
    ]
    for entry in selections:
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server = summary.get("server") or {}
        location = server.get("path") or server.get("dataset")
        location_clause = f"Source location: {location}." if location else ""
        predicate_summary = _summarise_predicates(summary.get("expectations") or {})
        if predicate_summary:
            predicate_clause = f"Valid if {predicate_summary}."
        else:
            predicate_clause = "Valid if the contract schema and recorded rules pass."
        action_clause = (
            "Validation failures raise RuntimeError so the pipeline stops."
            if mode == "strict"
            else "Validation verdicts are logged for orchestration decisions."
        )
        description = " ".join(
            part for part in (location_clause, predicate_clause, action_clause) if part
        )
        notes.append(
            {
                "title": f"{summary['id']} {summary['version']} read",
                "description": description,
            }
        )
    return notes


def _write_strategy_notes(
    selections: List[Dict[str, str]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    strategy: Mapping[str, Any],
) -> List[Dict[str, str]]:
    """Describe write strategies recommended for the helper UI."""

    mode = str(strategy.get("mode") or "split").lower()
    include_valid = bool(strategy.get("include_valid", True))
    include_reject = bool(strategy.get("include_reject", True))
    fail_on_warnings = bool(strategy.get("fail_on_warnings", False))

    notes: List[Dict[str, str]] = [
        {
            "title": "Governance hand-off",
            "description": (
                "write_with_contract(... return_status=True) records validation results and relays "
                "dataset versions to the governance client so each pipeline run is traceable."
                " By default non-active contracts are rejected; extend the contract-status options"
                " on your chosen strategy when drafts should be allowed."
            ),
        }
    ]

    if mode == "noop":
        notes.append(
            {
                "title": "Primary dataset only",
                "description": (
                    "NoOpWriteViolationStrategy keeps all rows in the primary dataset while still "
                    "capturing validation metadata."
                ),
            }
        )
    else:
        if include_valid and include_reject:
            split_desc = (
                "SplitWriteViolationStrategy writes passing rows to '<dataset>::valid' and rejected "
                "rows to '<dataset>::reject', preserving failed samples for triage."
            )
        elif include_valid:
            split_desc = (
                "SplitWriteViolationStrategy emits '<dataset>::valid' while violations stay with the "
                "primary dataset for follow-up."
            )
        elif include_reject:
            split_desc = (
                "SplitWriteViolationStrategy routes violations to '<dataset>::reject' and keeps valid "
                "rows in the primary dataset."
            )
        else:
            split_desc = "SplitWriteViolationStrategy keeps the primary dataset intact."
        description = split_desc
        if mode == "strict":
            strict_clause = " StrictWriteViolationStrategy raises when validation is not OK."
            if fail_on_warnings:
                strict_clause += " Warnings are treated as failures."
            description += strict_clause
        notes.append(
            {
                "title": "Split rejected rows" if mode == "split" else "Split & fail on violations",
                "description": description,
            }
        )
    for entry in selections:
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        dataset = summary.get("datasetId") or summary["id"]
        predicate_summary = _summarise_predicates(summary.get("expectations") or {})
        location = summary.get("server", {}).get("path") or summary.get("server", {}).get("dataset")
        location_clause = f"Target location: {location}." if location else ""
        if predicate_summary:
            predicate_clause = f"Valid if {predicate_summary}."
        else:
            predicate_clause = "Valid if the contract schema passes."
        if mode == "noop":
            routing_clause = f"All rows remain in '{dataset}' while validation metadata is captured."
        else:
            valid_target = f"'{dataset}::valid'" if include_valid else None
            reject_target = f"'{dataset}::reject'" if include_reject else None
            if include_valid and include_reject:
                routing_clause = (
                    f"Rows meeting the predicates flow to {valid_target} while violations route to {reject_target}."
                )
            elif include_valid:
                routing_clause = (
                    f"Rows meeting the predicates flow to {valid_target}; violations stay with '{dataset}'."
                )
            elif include_reject:
                routing_clause = (
                    f"Violations route to {reject_target} while passing rows remain in '{dataset}'."
                )
            else:
                routing_clause = f"Rows remain in '{dataset}'."
        extra_clause = ""
        if mode == "strict":
            if fail_on_warnings:
                extra_clause = " Validation errors or warnings raise RuntimeError so the run stops."
            else:
                extra_clause = " Validation errors raise RuntimeError so the run stops."
        notes.append(
            {
                "title": f"{summary['id']} {summary['version']} write",
                "description": " ".join(
                    part
                    for part in (
                        location_clause,
                        predicate_clause,
                        routing_clause,
                        extra_clause.strip(),
                    )
                    if part
                ),
            }
        )
    return notes

@router.get("/api/contracts")
async def api_contracts() -> List[Dict[str, Any]]:
    return load_contract_meta()


@router.get("/api/contracts/{cid}/{ver}")
async def api_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r.__dict__ for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    expectations = await _expectation_predicates(contract)
    return {
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations,
    }


@router.get("/api/contracts/{cid}/{ver}/preview")
async def api_contract_preview(
    cid: str,
    ver: str,
    dataset_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
    if read_with_contract is None or ContractVersionLocator is None:
        raise HTTPException(status_code=503, detail="pyspark is required for data previews")
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    effective_dataset_id = str(dataset_id or contract.id or cid)
    server = (contract.servers or [None])[0]
    dataset_path_hint = getattr(server, "path", None) if server else None
    version_contract = contract if effective_dataset_id == (contract.id or cid) else None
    scoped_records = [
        record
        for record in load_records()
        if record.contract_id == cid
        and record.contract_version == ver
        and record.dataset_name == effective_dataset_id
    ]
    version_records = _dq_version_records(
        effective_dataset_id,
        contract=version_contract,
        dataset_path=dataset_path_hint if version_contract else None,
        dataset_records=scoped_records,
    )
    known_versions = [entry["version"] for entry in version_records]
    if not known_versions:
        known_versions = ["latest"]
    selected_version = str(dataset_version or known_versions[-1])
    if selected_version not in known_versions:
        known_versions = _sort_versions([*known_versions, selected_version])
    limit = max(1, min(limit, 500))

    try:
        def _load_preview() -> tuple[list[Mapping[str, Any]], list[str]]:
            local_contract_service, local_dq_service, _ = _thread_service_clients()
            spark = _spark_session()
            locator = ContractVersionLocator(
                dataset_version=selected_version,
                dataset_id=effective_dataset_id,
            )
            df = read_with_contract(  # type: ignore[misc]
                spark,
                contract_id=cid,
                contract_service=local_contract_service,
                expected_contract_version=f"=={ver}",
                dataset_locator=locator,
                enforce=False,
                auto_cast=False,
                data_quality_service=local_dq_service,
                return_status=False,
            )
            rows_raw = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
            return rows_raw, list(df.columns)

        rows_raw, columns = await run_in_threadpool(_load_preview)
        rows = jsonable_encoder(rows_raw)
    except Exception as exc:  # pragma: no cover - defensive guard for preview errors
        logger.exception(
            "Failed to load preview for %s@%s dataset %s version %s",
            cid,
            ver,
            effective_dataset_id,
            selected_version,
        )
        raise HTTPException(status_code=500, detail=f"Failed to load preview: {exc}")

    status_payload = _dq_status_payload(effective_dataset_id, selected_version)
    status_value = str(status_payload.get("status", "unknown")) if status_payload else "unknown"
    response = {
        "dataset_id": effective_dataset_id,
        "dataset_version": selected_version,
        "rows": rows,
        "columns": columns,
        "limit": limit,
        "known_versions": known_versions,
        "status": {
            "status": status_value,
            "status_label": status_value.replace("_", " ").title(),
            "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
            "details": status_payload.get("details") if status_payload else None,
        },
    }
    return response


@router.post("/api/contracts/{cid}/{ver}/validate")
async def api_validate_contract(cid: str, ver: str) -> Dict[str, str]:
    return {"status": "active"}


@router.get("/api/datasets")
async def api_datasets() -> List[Dict[str, Any]]:
    records = load_records()
    return [r.__dict__.copy() for r in records]


@router.get("/api/datasets/{dataset_version}")
async def api_dataset_detail(dataset_version: str) -> Dict[str, Any]:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            return {
                "record": r.__dict__,
                "contract": contract_to_dict(contract),
                "expectations": await _expectation_predicates(contract),
            }
    raise HTTPException(status_code=404, detail="Dataset not found")


@router.get("/api/integration-helper/contracts")
async def api_integration_contracts() -> Dict[str, Any]:
    """Return catalog metadata for the integration helper UI."""

    return {"contracts": _integration_catalog()}


@router.get("/api/integration-helper/contracts/{cid}/{ver}")
async def api_integration_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    """Return contract details enriched for the integration helper."""

    context = await _load_integration_contract(cid, ver)
    return {
        "contract": contract_to_dict(context.contract),
        "summary": jsonable_encoder(context.summary),
    }


@router.post("/api/integration-helper/stub")
async def api_integration_stub(request: Request) -> Dict[str, Any]:
    """Return a generated stub and strategy notes for an integration selection."""

    payload = await request.json()
    integration = str(payload.get("integration") or "spark").lower()
    if integration != "spark":
        raise HTTPException(status_code=400, detail=f"Unsupported integration: {integration}")

    inputs = _normalise_selection(payload.get("inputs") or [])
    outputs = _normalise_selection(payload.get("outputs") or [])
    if not inputs:
        raise HTTPException(status_code=422, detail="At least one input contract is required")
    if not outputs:
        raise HTTPException(status_code=422, detail="At least one output contract is required")

    read_strategy = _normalise_read_strategy(payload.get("read_strategy") or {})
    write_strategy = _normalise_write_strategy(payload.get("write_strategy") or {})

    context_map: Dict[Tuple[str, str], IntegrationContractContext] = {}
    for entry in inputs + outputs:
        key = (entry["contract_id"], entry["version"])
        if key not in context_map:
            context_map[key] = await _load_integration_contract(*key)

    stub_text = _spark_stub_for_selection(
        inputs,
        outputs,
        context_map,
        read_strategy=read_strategy,
        write_strategy=write_strategy,
    )
    read_notes = _read_strategy_notes(inputs, context_map, read_strategy)
    write_notes = _write_strategy_notes(outputs, context_map, write_strategy)

    return {
        "integration": integration,
        "stub": stub_text,
        "strategies": {
            "read": read_notes,
            "write": write_notes,
        },
        "selected_strategies": {
            "read": read_strategy,
            "write": write_strategy,
        },
        "contracts": {
            "inputs": [
                jsonable_encoder(context_map[(item["contract_id"], item["version"])].summary)
                for item in inputs
            ],
            "outputs": [
                jsonable_encoder(context_map[(item["contract_id"], item["version"])].summary)
                for item in outputs
            ],
        },
    }


@router.get("/setup", response_class=HTMLResponse)
async def setup_get(request: Request, step: Optional[int] = None, restart: bool = False) -> HTMLResponse:
    """Render the environment setup wizard."""

    if restart:
        state = reset_setup_state()
    else:
        state = load_setup_state()

    context = _build_setup_context(request, state, step=step)
    return templates.TemplateResponse("setup.html", context)


@router.post("/setup", response_class=HTMLResponse)
async def setup_post(request: Request) -> HTMLResponse:
    """Handle setup wizard transitions and persist configuration."""

    form = await request.form()
    action = str(form.get("step") or "1")
    state = load_setup_state()

    if action == "1":
        selections: Dict[str, str] = {}
        errors: List[str] = []
        for module_key, module_meta in SETUP_MODULES.items():
            field_name = f"module__{module_key}"
            value = str(form.get(field_name) or "").strip()
            if not value or value not in module_meta["options"]:
                errors.append(f"Select an option for {module_meta.get('title') or module_key}.")
            else:
                selections[module_key] = value
        if errors:
            temp_state = dict(state)
            temp_state["selected_options"] = selections
            context = _build_setup_context(request, temp_state, step=1, errors=errors)
            return templates.TemplateResponse("setup.html", context, status_code=422)

        configuration = state.get("configuration") if isinstance(state, Mapping) else {}
        new_configuration: Dict[str, Any] = {}
        if isinstance(configuration, Mapping):
            for module_key in selections:
                module_config = configuration.get(module_key, {})
                if isinstance(module_config, Mapping):
                    new_configuration[module_key] = dict(module_config)

        updated_state = dict(state)
        updated_state["selected_options"] = selections
        updated_state["configuration"] = new_configuration
        updated_state["current_step"] = 2
        updated_state["completed"] = False
        save_setup_state(updated_state)
        return RedirectResponse(url="/setup?step=2", status_code=303)

    if action == "2":
        selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
        if not isinstance(selected_options, Mapping) or not selected_options:
            context = _build_setup_context(request, state, step=1, errors=["Choose an implementation for each module first."])
            return templates.TemplateResponse("setup.html", context, status_code=422)

        configuration: Dict[str, Dict[str, Any]] = {}
        errors = []
        for module_key, option_key in selected_options.items():
            module_meta = SETUP_MODULES.get(module_key)
            option_meta = module_meta["options"].get(option_key) if module_meta else None
            if not option_meta:
                continue
            module_config: Dict[str, Any] = {}
            for field_meta in option_meta.get("fields", []):
                field_name = str(field_meta.get("name") or "")
                if not field_name:
                    continue
                form_key = f"config__{module_key}__{field_name}"
                value = str(form.get(form_key) or "").strip()
                if not value and not field_meta.get("optional"):
                    errors.append(f"{module_meta.get('title')}: {field_meta.get('label')} is required.")
                module_config[field_name] = value
            configuration[module_key] = module_config

        if errors:
            temp_state = dict(state)
            temp_state["configuration"] = configuration
            context = _build_setup_context(request, temp_state, step=2, errors=errors)
            return templates.TemplateResponse("setup.html", context, status_code=422)

        updated_state = dict(state)
        updated_state["configuration"] = configuration
        updated_state["current_step"] = 3
        updated_state["completed"] = False
        save_setup_state(updated_state)
        return RedirectResponse(url="/setup?step=3", status_code=303)

    if action == "complete":
        updated_state = dict(state)
        updated_state["completed"] = True
        updated_state["current_step"] = 3
        updated_state["completed_at"] = datetime.utcnow().isoformat() + "Z"
        save_setup_state(updated_state)
        return RedirectResponse(url="/", status_code=303)

    if action == "reset":
        reset_setup_state()
        return RedirectResponse(url="/setup?step=1", status_code=303)

    context = _build_setup_context(request, state)
    return templates.TemplateResponse("setup.html", context)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/integration-helper", response_class=HTMLResponse)
async def integration_helper(request: Request) -> HTMLResponse:
    """Render the contract integration helper interface."""

    context = {
        "request": request,
        "catalog": _integration_catalog(),
        "integration_options": [
            {"value": "spark", "label": "Spark (PySpark / Delta Lake)"},
        ],
    }
    return templates.TemplateResponse("integration_helper.html", context)


@router.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contract_ids = store.list_contracts()
    return templates.TemplateResponse(
        "contracts.html", {"request": request, "contracts": contract_ids}
    )


@router.get("/contracts/new", response_class=HTMLResponse)
async def new_contract_form(request: Request) -> HTMLResponse:
    editor_state = _contract_editor_state()
    editor_state["version"] = editor_state.get("version") or "1.0.0"
    context = _editor_context(request, editor_state=editor_state)
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/new", response_class=HTMLResponse)
async def create_contract(
    request: Request,
    payload: str = Form(...),
) -> HTMLResponse:
    error: Optional[str] = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _contract_editor_state()
    else:
        try:
            _validate_contract_payload(editor_state, editing=False)
            model = _build_contract_from_payload(editor_state)
            store.put(model)
            return RedirectResponse(url="/contracts", status_code=303)
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _editor_context(
        request,
        editor_state=editor_state,
        error=error,
    )
    return templates.TemplateResponse("new_contract.html", context)




@router.get("/contracts/{cid}", response_class=HTMLResponse)
async def list_contract_versions(request: Request, cid: str) -> HTMLResponse:
    versions = store.list_versions(cid)
    if not versions:
        raise HTTPException(status_code=404, detail="Contract not found")
    records_by_version: Dict[str, List[DatasetRecord]] = {}
    for record in load_records():
        if record.contract_id != cid:
            continue
        records_by_version.setdefault(record.contract_version, []).append(record)

    contracts = []
    for ver in versions:
        try:
            contract = store.get(cid, ver)
        except FileNotFoundError:
            continue

        status_raw = getattr(contract, "status", "") or "unknown"
        status_value = str(status_raw).lower()
        status_label = str(status_raw).replace("_", " ").title()
        status_badge = _CONTRACT_STATUS_BADGES.get(status_value, "bg-secondary")

        server_info = _server_details(contract)
        dataset_hint = (
            server_info.get("dataset_id")
            if server_info
            else (contract.id or cid)
        )

        latest_run: Optional[DatasetRecord] = None
        run_entries = records_by_version.get(ver, [])
        if run_entries:
            run_entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
            latest_run = run_entries[-1]

        contracts.append(
            {
                "id": cid,
                "version": ver,
                "status": status_value,
                "status_label": status_label,
                "status_badge": status_badge,
                "server": server_info,
                "dataset_hint": dataset_hint,
                "latest_run": latest_run.__dict__ if latest_run else None,
            }
        )
    context = {"request": request, "contract_id": cid, "contracts": contracts}
    return templates.TemplateResponse("contract_versions.html", context)


@router.get("/contracts/{cid}/{ver}", response_class=HTMLResponse)
async def contract_detail(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    records = load_records()
    datasets = [r for r in records if r.contract_id == cid and r.contract_version == ver]
    product_links = data_products_for_contract(cid, records)
    field_quality = _field_quality_sections(contract)
    dataset_quality = _dataset_quality_sections(contract)
    change_log = _contract_change_log(contract)
    server_info = _server_details(contract)
    dataset_id = server_info.get("dataset_id") if server_info else contract.id or cid
    dataset_path_hint = server_info.get("path") if server_info else None
    version_records = _dq_version_records(
        dataset_id or cid,
        contract=contract,
        dataset_path=dataset_path_hint,
        dataset_records=datasets,
    )
    version_list = [entry["version"] for entry in version_records]
    status_map = {
        entry["version"]: {
            "status": entry["status"],
            "label": entry["status_label"],
            "badge": entry["badge"],
        }
        for entry in version_records
    }
    default_index = len(version_list) - 1 if version_list else None
    context = {
        "request": request,
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": await _expectation_predicates(contract),
        "field_quality": field_quality,
        "dataset_quality": dataset_quality,
        "change_log": change_log,
        "status_badges": _STATUS_BADGES,
        "server_info": server_info,
        "compatibility_versions": version_records,
        "preview_versions": version_list,
        "preview_status_map": status_map,
        "preview_default_index": default_index,
        "preview_dataset_id": dataset_id,
        "data_products": product_links,
    }
    return templates.TemplateResponse("contract_detail.html", context)


def _next_version(ver: str) -> str:
    v = Version(ver)
    return f"{v.major}.{v.minor}.{v.micro + 1}"


_EXPECTATION_KEYS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
    "query",
)


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bool, int, float)):
        return json.dumps(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value)


def _parse_json_value(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list, bool, int, float)):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return raw


def _as_bool(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
    return bool(value)


def _as_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Expected integer value, got {value!r}")


def _custom_properties_state(raw: Any) -> List[Dict[str, str]]:
    state: List[Dict[str, str]] = []
    for item in normalise_custom_properties(raw):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        else:
            key = getattr(item, "property", None)
            value = getattr(item, "value", None)
        if key:
            state.append({"property": str(key), "value": _stringify_value(value)})
    return state


def _quality_state(items: Optional[Iterable[Any]]) -> List[Dict[str, Any]]:
    state: List[Dict[str, Any]] = []
    if not items:
        return state
    for item in items:
        if hasattr(item, "model_dump"):
            raw = item.model_dump(exclude_none=True)
        elif hasattr(item, "dict"):
            raw = item.dict(exclude_none=True)  # type: ignore[attr-defined]
        else:
            raw = {k: v for k, v in vars(item).items() if v is not None}
        expectation = None
        expectation_value = None
        for key in _EXPECTATION_KEYS:
            if key in raw:
                expectation = key
                expectation_value = raw.pop(key)
                break
        for key, value in list(raw.items()):
            if isinstance(value, (list, dict)):
                raw[key] = json.dumps(value, indent=2, sort_keys=True)
        entry: Dict[str, Any] = {k: v for k, v in raw.items() if v is not None}
        if expectation:
            entry["expectation"] = expectation
            if isinstance(expectation_value, list):
                entry["expectationValue"] = ", ".join(str(v) for v in expectation_value)
            elif isinstance(expectation_value, (dict, list)):
                entry["expectationValue"] = json.dumps(expectation_value, indent=2, sort_keys=True)
            elif expectation_value is None:
                entry["expectationValue"] = ""
            else:
                entry["expectationValue"] = str(expectation_value)
        state.append(entry)
    return state


def _schema_property_state(prop: SchemaProperty) -> Dict[str, Any]:
    examples = getattr(prop, "examples", None) or []
    return {
        "name": getattr(prop, "name", "") or "",
        "physicalType": getattr(prop, "physicalType", "") or "",
        "description": getattr(prop, "description", "") or "",
        "businessName": getattr(prop, "businessName", "") or "",
        "logicalType": getattr(prop, "logicalType", "") or "",
        "logicalTypeOptions": _stringify_value(getattr(prop, "logicalTypeOptions", None)),
        "required": bool(getattr(prop, "required", False)),
        "unique": bool(getattr(prop, "unique", False)),
        "partitioned": bool(getattr(prop, "partitioned", False)),
        "primaryKey": bool(getattr(prop, "primaryKey", False)),
        "classification": getattr(prop, "classification", "") or "",
        "examples": "\n".join(str(item) for item in examples),
        "customProperties": _custom_properties_state(getattr(prop, "customProperties", None)),
        "quality": _quality_state(getattr(prop, "quality", None)),
    }


def _schema_object_state(obj: SchemaObject) -> Dict[str, Any]:
    properties = [
        _schema_property_state(prop)
        for prop in getattr(obj, "properties", None) or []
    ]
    return {
        "name": getattr(obj, "name", "") or "",
        "description": getattr(obj, "description", "") or "",
        "businessName": getattr(obj, "businessName", "") or "",
        "logicalType": getattr(obj, "logicalType", "") or "",
        "customProperties": _custom_properties_state(getattr(obj, "customProperties", None)),
        "quality": _quality_state(getattr(obj, "quality", None)),
        "properties": properties,
    }


_SERVER_FIELD_MAP = {
    "description": "description",
    "environment": "environment",
    "format": "format",
    "path": "path",
    "dataset": "dataset",
    "database": "database",
    "schema": "schema_",
    "catalog": "catalog",
    "host": "host",
    "location": "location",
    "endpointUrl": "endpointUrl",
    "project": "project",
    "region": "region",
    "regionName": "regionName",
    "serviceName": "serviceName",
    "warehouse": "warehouse",
    "stagingDir": "stagingDir",
    "account": "account",
}


def _server_state(server: Server) -> Dict[str, Any]:
    state = {
        "server": getattr(server, "server", "") or "",
        "type": getattr(server, "type", "") or "",
        "port": getattr(server, "port", None) or "",
    }
    for field, attr in _SERVER_FIELD_MAP.items():
        state[field] = getattr(server, attr, "") or ""
    versioning_value: Any | None = None
    path_pattern_value: Any | None = None
    custom_entries: List[Dict[str, str]] = []
    for item in normalise_custom_properties(getattr(server, "customProperties", None)):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        else:
            key = getattr(item, "property", None)
            value = getattr(item, "value", None)
        if not key:
            continue
        if str(key) == "dc43.core.versioning":
            versioning_value = value
            continue
        if str(key) == "dc43.pathPattern":
            path_pattern_value = value
            continue
        custom_entries.append({"property": str(key), "value": _stringify_value(value)})
    if versioning_value is not None:
        parsed = versioning_value
        if isinstance(parsed, str):
            parsed = _parse_json_value(parsed)
        state["versioningConfig"] = parsed if isinstance(parsed, Mapping) else None
    if path_pattern_value not in (None, ""):
        state["pathPattern"] = str(path_pattern_value)
    state["customProperties"] = custom_entries
    return state


def _support_state(items: Optional[Iterable[Support]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    if not items:
        return result
    for entry in items:
        payload: Dict[str, Any] = {}
        for field in ("channel", "url", "description", "tool", "scope", "invitationUrl"):
            value = getattr(entry, field, None)
            if value:
                payload[field] = value
        if payload:
            result.append(payload)
    return result


def _sla_state(items: Optional[Iterable[ServiceLevelAgreementProperty]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    if not items:
        return result
    for entry in items:
        payload: Dict[str, Any] = {}
        for field in ("property", "value", "valueExt", "unit", "element", "driver"):
            value = getattr(entry, field, None)
            if value is None:
                continue
            if field in {"value", "valueExt"}:
                payload[field] = _stringify_value(value)
            else:
                payload[field] = value
        if payload:
            result.append(payload)
    return result


def _contract_editor_state(contract: Optional[OpenDataContractStandard] = None) -> Dict[str, Any]:
    if contract is None:
        return {
            "id": "",
            "version": "",
            "kind": "DataContract",
            "apiVersion": "3.0.2",
            "name": "",
            "description": "",
            "status": "",
            "domain": "",
            "dataProduct": "",
            "tenant": "",
            "tags": [],
            "customProperties": [],
            "servers": [],
            "schemaObjects": [
                {
                    "name": "",
                    "description": "",
                    "businessName": "",
                    "logicalType": "",
                    "customProperties": [],
                    "quality": [],
                    "properties": [],
                }
            ],
            "support": [],
            "slaProperties": [],
        }
    description = getattr(contract.description, "usage", "") if getattr(contract, "description", None) else ""
    state = {
        "id": getattr(contract, "id", "") or "",
        "version": getattr(contract, "version", "") or "",
        "kind": getattr(contract, "kind", "DataContract") or "DataContract",
        "apiVersion": getattr(contract, "apiVersion", "3.0.2") or "3.0.2",
        "name": getattr(contract, "name", "") or "",
        "description": description,
        "status": getattr(contract, "status", "") or "",
        "domain": getattr(contract, "domain", "") or "",
        "dataProduct": getattr(contract, "dataProduct", "") or "",
        "tenant": getattr(contract, "tenant", "") or "",
        "tags": list(getattr(contract, "tags", []) or []),
        "customProperties": _custom_properties_state(getattr(contract, "customProperties", None)),
        "servers": [_server_state(server) for server in getattr(contract, "servers", []) or []],
        "schemaObjects": [
            _schema_object_state(obj) for obj in getattr(contract, "schema_", None) or []
        ],
        "support": _support_state(getattr(contract, "support", None)),
        "slaProperties": _sla_state(getattr(contract, "slaProperties", None)),
    }
    if not state["schemaObjects"]:
        state["schemaObjects"] = _contract_editor_state(None)["schemaObjects"]
    return state


def _sorted_versions(values: Iterable[str]) -> List[str]:
    parsed: List[Tuple[Version, str]] = []
    invalid: List[str] = []
    for value in values:
        if not value:
            continue
        try:
            parsed.append((Version(str(value)), str(value)))
        except InvalidVersion:
            invalid.append(str(value))
    parsed.sort(key=lambda entry: entry[0])
    return [ver for _, ver in parsed] + sorted(invalid)


def _build_editor_meta(
    *,
    editor_state: Mapping[str, Any],
    editing: bool,
    original_version: Optional[str],
    baseline_state: Optional[Mapping[str, Any]],
    baseline_contract: Optional[OpenDataContractStandard],
) -> Dict[str, Any]:
    existing_contracts = sorted(store.list_contracts())
    version_map: Dict[str, List[str]] = {}
    for contract_id in existing_contracts:
        try:
            versions = store.list_versions(contract_id)
        except FileNotFoundError:
            versions = []
        version_map[contract_id] = _sorted_versions(versions)
    meta: Dict[str, Any] = {
        "existingContracts": existing_contracts,
        "existingVersions": version_map,
        "editing": editing,
        "originalVersion": original_version,
        "contractId": str(editor_state.get("id", "")) or (
            getattr(baseline_contract, "id", "") if baseline_contract else ""
        ),
    }
    if original_version:
        meta["baseVersion"] = original_version
    if baseline_state is None and baseline_contract is not None:
        baseline_state = _contract_editor_state(baseline_contract)
    if baseline_state is not None:
        # ensure baseline is JSON serializable
        meta["baselineState"] = jsonable_encoder(baseline_state)
    if baseline_contract is not None:
        meta["baseContract"] = contract_to_dict(baseline_contract)
    return meta


def _editor_context(
    request: Request,
    *,
    editor_state: Dict[str, Any],
    editing: bool = False,
    original_version: Optional[str] = None,
    baseline_state: Optional[Mapping[str, Any]] = None,
    baseline_contract: Optional[OpenDataContractStandard] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    context = {
        "request": request,
        "editing": editing,
        "editor_state": editor_state,
        "status_options": _STATUS_OPTIONS,
        "versioning_modes": _VERSIONING_MODES,
        "editor_meta": _build_editor_meta(
            editor_state=editor_state,
            editing=editing,
            original_version=original_version,
            baseline_state=baseline_state,
            baseline_contract=baseline_contract,
        ),
    }
    if original_version:
        context["original_version"] = original_version
    if error:
        context["error"] = error
    return context


def _custom_properties_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[CustomProperty] | None:
    result: List[CustomProperty] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = (str(item.get("property", ""))).strip()
        if not key:
            continue
        value = _parse_json_value(item.get("value"))
        result.append(CustomProperty(property=key, value=value))
    return result or None


def _validate_contract_payload(
    payload: Mapping[str, Any],
    *,
    editing: bool,
    base_contract_id: Optional[str] = None,
    base_version: Optional[str] = None,
) -> None:
    contract_id = (str(payload.get("id", ""))).strip()
    if not contract_id:
        raise ValueError("Contract ID is required")
    version = (str(payload.get("version", ""))).strip()
    if not version:
        raise ValueError("Version is required")
    try:
        new_version = SemVer.parse(version)
    except ValueError as exc:
        raise ValueError(f"Invalid semantic version: {exc}") from exc
    existing_contracts = set(store.list_contracts())
    existing_versions = (
        set(store.list_versions(contract_id)) if contract_id in existing_contracts else set()
    )
    if editing:
        if base_contract_id and contract_id != base_contract_id:
            raise ValueError("Contract ID cannot be changed while editing")
        if base_version:
            try:
                prior = SemVer.parse(base_version)
            except ValueError:
                prior = None
            if prior and (
                (new_version.major, new_version.minor, new_version.patch)
                <= (prior.major, prior.minor, prior.patch)
            ):
                raise ValueError(
                    f"Version {version} must be greater than {base_version}"
                )
        if version in existing_versions:
            raise ValueError(
                f"Version {version} is already stored for contract {contract_id}"
            )
    else:
        if contract_id in existing_contracts and version in existing_versions:
            raise ValueError(
                f"Contract {contract_id} already has a version {version}."
            )


def _support_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[Support] | None:
    result: List[Support] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        channel = (str(item.get("channel", ""))).strip()
        if not channel:
            continue
        payload: Dict[str, Any] = {"channel": channel}
        for field in ("url", "description", "tool", "scope", "invitationUrl"):
            value = item.get(field)
            if value:
                payload[field] = value
        result.append(Support(**payload))
    return result or None


def _sla_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[ServiceLevelAgreementProperty] | None:
    result: List[ServiceLevelAgreementProperty] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = (str(item.get("property", ""))).strip()
        if not key:
            continue
        payload: Dict[str, Any] = {"property": key}
        for field in ("unit", "element", "driver"):
            value = item.get(field)
            if value:
                payload[field] = value
        value = item.get("value")
        if value not in (None, ""):
            payload["value"] = _parse_json_value(value)
        value_ext = item.get("valueExt")
        if value_ext not in (None, ""):
            payload["valueExt"] = _parse_json_value(value_ext)
        result.append(ServiceLevelAgreementProperty(**payload))
    return result or None


def _parse_expectation_value(expectation: str, value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (list, dict, bool, int, float)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if expectation in {"mustBeBetween", "mustNotBeBetween"}:
        separators = [",", ";"]
        for sep in separators:
            if sep in text:
                parts = [p.strip() for p in text.split(sep) if p.strip()]
                break
        else:
            parts = [p.strip() for p in text.split() if p.strip()]
        if len(parts) < 2:
            raise ValueError("Data quality range requires two numeric values")
        try:
            return [float(parts[0]), float(parts[1])]
        except ValueError as exc:
            raise ValueError("Data quality range must be numeric") from exc
    if expectation in {
        "mustBeGreaterThan",
        "mustBeGreaterOrEqualTo",
        "mustBeLessThan",
        "mustBeLessOrEqualTo",
    }:
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(f"Expectation {expectation} requires a numeric value") from exc
    if expectation in {"mustBe", "mustNotBe"}:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def _quality_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[DataQuality] | None:
    result: List[DataQuality] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        payload: Dict[str, Any] = {}
        for field in (
            "name",
            "type",
            "rule",
            "description",
            "dimension",
            "severity",
            "unit",
            "schedule",
            "scheduler",
            "businessImpact",
            "method",
        ):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        tags_value = item.get("tags")
        if isinstance(tags_value, str):
            tags = [t.strip() for t in tags_value.split(",") if t.strip()]
            if tags:
                payload["tags"] = tags
        elif isinstance(tags_value, Iterable):
            tags = [str(t).strip() for t in tags_value if str(t).strip()]
            if tags:
                payload["tags"] = tags
        expectation = item.get("expectation")
        if expectation:
            payload[expectation] = _parse_expectation_value(expectation, item.get("expectationValue"))
        implementation = item.get("implementation")
        if implementation not in (None, ""):
            payload["implementation"] = _parse_json_value(implementation)
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        if payload:
            result.append(DataQuality(**payload))
    return result or None


def _schema_properties_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[SchemaProperty]:
    result: List[SchemaProperty] = []
    if not items:
        return result
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        if not name:
            continue
        payload: Dict[str, Any] = {"name": name}
        physical_type = item.get("physicalType")
        if physical_type:
            payload["physicalType"] = physical_type
        for field in (
            "description",
            "businessName",
            "classification",
            "logicalType",
        ):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        logical_type_options = item.get("logicalTypeOptions")
        if logical_type_options not in (None, ""):
            payload["logicalTypeOptions"] = _parse_json_value(logical_type_options)
        for boolean_field in ("required", "unique", "partitioned", "primaryKey"):
            value = _as_bool(item.get(boolean_field))
            if value is not None:
                payload[boolean_field] = value
        examples = item.get("examples")
        if isinstance(examples, str):
            values = [ex.strip() for ex in examples.splitlines() if ex.strip()]
            if values:
                payload["examples"] = values
        elif isinstance(examples, Iterable):
            values = [str(ex).strip() for ex in examples if str(ex).strip()]
            if values:
                payload["examples"] = values
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        quality = _quality_models(item.get("quality"))
        if quality:
            payload["quality"] = quality
        result.append(SchemaProperty(**payload))
    return result


def _schema_objects_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[SchemaObject]:
    result: List[SchemaObject] = []
    if not items:
        return result
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        payload: Dict[str, Any] = {}
        if name:
            payload["name"] = name
        for field in ("description", "businessName", "logicalType"):
            value = item.get(field)
            if value not in (None, ""):
                payload[field] = value
        custom_props = _custom_properties_models(item.get("customProperties"))
        if custom_props:
            payload["customProperties"] = custom_props
        quality = _quality_models(item.get("quality"))
        if quality:
            payload["quality"] = quality
        properties = _schema_properties_models(item.get("properties"))
        if properties:
            name_counts = Counter(
                prop.name for prop in properties if getattr(prop, "name", None)
            )
            duplicates = [name for name, count in name_counts.items() if count > 1]
            if duplicates:
                object_name = payload.get("name") or "schema object"
                dup_list = ", ".join(sorted(duplicates))
                raise ValueError(
                    f"Duplicate field name(s) {dup_list} in {object_name}"
                )
        payload["properties"] = properties
        result.append(SchemaObject(**payload))
    return result


def _server_models(items: Optional[Iterable[Mapping[str, Any]]]) -> List[Server] | None:
    result: List[Server] = []
    if not items:
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        server_name = (str(item.get("server", ""))).strip()
        server_type = (str(item.get("type", ""))).strip()
        if not server_name or not server_type:
            continue
        payload: Dict[str, Any] = {"server": server_name, "type": server_type}
        for field, attr in _SERVER_FIELD_MAP.items():
            value = item.get(field)
            if value not in (None, ""):
                payload[attr] = value
        port_value = item.get("port")
        if port_value not in (None, ""):
            payload["port"] = _as_int(port_value)
        custom_props: List[CustomProperty] = []
        base_custom = _custom_properties_models(item.get("customProperties"))
        if base_custom:
            custom_props.extend(base_custom)
        versioning_config = item.get("versioningConfig")
        if versioning_config not in (None, "", {}):
            parsed_versioning = (
                versioning_config
                if isinstance(versioning_config, Mapping)
                else _parse_json_value(versioning_config)
            )
            if not isinstance(parsed_versioning, Mapping):
                raise ValueError("dc43.core.versioning must be provided as an object")
            custom_props.append(
                CustomProperty(property="dc43.core.versioning", value=parsed_versioning)
            )
        path_pattern = item.get("pathPattern")
        if path_pattern not in (None, ""):
            custom_props.append(
                CustomProperty(property="dc43.pathPattern", value=str(path_pattern))
            )
        if custom_props:
            payload["customProperties"] = custom_props
        result.append(Server(**payload))
    return result or None


def _normalise_tags(value: Any) -> List[str] | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        tags = [item.strip() for item in value.split(",") if item.strip()]
        return tags or None
    if isinstance(value, Iterable):
        tags = [str(item).strip() for item in value if str(item).strip()]
        return tags or None
    return None


def _build_contract_from_payload(payload: Mapping[str, Any]) -> OpenDataContractStandard:
    contract_id = (str(payload.get("id", ""))).strip()
    if not contract_id:
        raise ValueError("Contract ID is required")
    version = (str(payload.get("version", ""))).strip()
    if not version:
        raise ValueError("Version is required")
    name = (str(payload.get("name", "")) or contract_id).strip()
    description = str(payload.get("description", ""))
    kind = (str(payload.get("kind", "DataContract")) or "DataContract").strip()
    api_version = (str(payload.get("apiVersion", "3.0.2")) or "3.0.2").strip()
    status = str(payload.get("status", "")).strip() or None
    domain = str(payload.get("domain", "")).strip() or None
    data_product = str(payload.get("dataProduct", "")).strip() or None
    tenant = str(payload.get("tenant", "")).strip() or None
    tags = _normalise_tags(payload.get("tags"))
    custom_props = _custom_properties_models(payload.get("customProperties"))
    servers = _server_models(payload.get("servers"))
    schema_objects = _schema_objects_models(payload.get("schemaObjects"))
    if not schema_objects:
        raise ValueError("At least one schema object with fields is required")
    # Ensure each schema object has properties
    for obj in schema_objects:
        if not obj.properties:
            raise ValueError("Each schema object must define at least one field")
    support_entries = _support_models(payload.get("support"))
    sla_properties = _sla_models(payload.get("slaProperties"))
    return OpenDataContractStandard(
        version=version,
        kind=kind,
        apiVersion=api_version,
        id=contract_id,
        name=name,
        description=None if not description else Description(usage=description),
        status=status,
        domain=domain,
        dataProduct=data_product,
        tenant=tenant,
        tags=tags,
        customProperties=custom_props,
        servers=servers,
        schema=schema_objects,  # type: ignore[arg-type]
        support=support_entries,
        slaProperties=sla_properties,
    )


@router.get("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def edit_contract_form(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    new_ver = _next_version(ver)
    editor_state = _contract_editor_state(contract)
    baseline_state = json.loads(json.dumps(editor_state))
    editor_state["version"] = new_ver
    context = _editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=ver,
        baseline_state=baseline_state,
        baseline_contract=contract,
    )
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def save_contract_edits(
    request: Request,
    cid: str,
    ver: str,
    payload: str = Form(...),
    original_version: str = Form(""),
) -> HTMLResponse:
    editor_state: Dict[str, Any]
    baseline_contract: Optional[OpenDataContractStandard] = None
    baseline_state: Optional[Dict[str, Any]] = None
    base_version = original_version or ver
    try:
        baseline_contract = store.get(cid, base_version)
        baseline_state = json.loads(json.dumps(_contract_editor_state(baseline_contract)))
    except FileNotFoundError:
        baseline_contract = None
        baseline_state = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _contract_editor_state()
        editor_state["id"] = cid
        editor_state["version"] = _next_version(ver)
    else:
        try:
            _validate_contract_payload(
                editor_state,
                editing=True,
                base_contract_id=cid,
                base_version=base_version,
            )
            model = _build_contract_from_payload(editor_state)
            store.put(model)
            return RedirectResponse(
                url=f"/contracts/{model.id}/{model.version}", status_code=303
            )
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=base_version,
        baseline_state=baseline_state,
        baseline_contract=baseline_contract,
        error=error,
    )
    return templates.TemplateResponse("new_contract.html", context)


@router.post("/contracts/{cid}/{ver}/validate")
async def html_validate_contract(cid: str, ver: str) -> HTMLResponse:
    return RedirectResponse(url=f"/contracts/{cid}/{ver}", status_code=303)


@router.get("/datasets", response_class=HTMLResponse)
async def list_datasets(request: Request) -> HTMLResponse:
    records = load_records()
    catalog = dataset_catalog(records)
    context = {"request": request, "datasets": catalog}
    return templates.TemplateResponse("datasets.html", context)


@router.get("/datasets/{dataset_name}", response_class=HTMLResponse)
async def dataset_versions(request: Request, dataset_name: str) -> HTMLResponse:
    records = [r.__dict__.copy() for r in load_records() if r.dataset_name == dataset_name]
    context = {"request": request, "dataset_name": dataset_name, "records": records}
    return templates.TemplateResponse("dataset_versions.html", context)


@router.get("/data-products", response_class=HTMLResponse)
async def list_data_products(request: Request) -> HTMLResponse:
    records = load_records()
    catalog = data_product_catalog(records)
    context = {"request": request, "products": catalog}
    return templates.TemplateResponse("data_products.html", context)


@router.get("/data-products/{product_id}", response_class=HTMLResponse)
async def data_product_detail_view(request: Request, product_id: str) -> HTMLResponse:
    records = load_records()
    details = describe_data_product(product_id, records)
    if details is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    context = {"request": request, "product": details}
    return templates.TemplateResponse("data_product_detail.html", context)


def _dataset_path(contract: OpenDataContractStandard | None, dataset_name: str, dataset_version: str) -> Path:
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(DATA_DIR).parent
    base = Path(getattr(server, "path", "")) if server else data_root
    if base.suffix:
        base = base.parent
    if not base.is_absolute():
        base = data_root / base
    if base.name == dataset_name:
        return base / dataset_version
    return base / dataset_name / dataset_version


def _dataset_preview(contract: OpenDataContractStandard | None, dataset_name: str, dataset_version: str) -> str:
    ds_path = _dataset_path(contract, dataset_name, dataset_version)
    server = (contract.servers or [None])[0] if contract else None
    fmt = getattr(server, "format", None)
    try:
        if fmt == "parquet":
            from pyspark.sql import SparkSession  # type: ignore
            spark = SparkSession.builder.master("local[1]").appName("preview").getOrCreate()
            df = spark.read.parquet(str(ds_path))
            return "\n".join(str(r.asDict()) for r in df.limit(10).collect())[:1000]
        if fmt == "json":
            target = ds_path if ds_path.is_file() else next(ds_path.glob("*.json"), None)
            if target:
                return target.read_text()[:1000]
        if ds_path.is_file():
            return ds_path.read_text()[:1000]
        if ds_path.is_dir():
            target = next((p for p in ds_path.iterdir() if p.is_file()), None)
            if target:
                return target.read_text()[:1000]
    except Exception:
        return ""
    return ""




def _data_products_payload() -> List[Mapping[str, Any]]:
    try:
        raw = json.loads(DATA_PRODUCTS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, Mapping)]
    return []


def load_data_products() -> List[OpenDataProductStandard]:
    documents: List[OpenDataProductStandard] = []
    for payload in _data_products_payload():
        try:
            documents.append(OpenDataProductStandard.from_dict(payload))
        except Exception:  # pragma: no cover - defensive
            continue
    return documents


def _port_custom_map(port: Any) -> Dict[str, Any]:
    props = getattr(port, "custom_properties", []) or []
    mapping: Dict[str, Any] = {}
    for entry in props:
        if isinstance(entry, Mapping):
            key = entry.get("property")
            if key:
                mapping[str(key)] = entry.get("value")
    return mapping


def _records_by_data_product(records: Iterable[DatasetRecord]) -> Dict[str, List[DatasetRecord]]:
    grouped: Dict[str, List[DatasetRecord]] = {}
    for record in records:
        if record.data_product_id:
            grouped.setdefault(record.data_product_id, []).append(record)
    for entries in grouped.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    return grouped


def data_product_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    grouped = _records_by_data_product(records)
    summaries: List[Dict[str, Any]] = []
    for product in load_data_products():
        product_records = list(grouped.get(product.id, []))
        latest_record = product_records[-1] if product_records else None
        inputs: List[Dict[str, Any]] = []
        for port in product.input_ports:
            props = _port_custom_map(port)
            inputs.append(
                {
                    "name": port.name,
                    "contract_id": port.contract_id,
                    "version": port.version,
                    "source_data_product": props.get("dc43.input.source_data_product"),
                    "source_output_port": props.get("dc43.input.source_output_port"),
                    "custom_properties": props,
                }
            )
        outputs: List[Dict[str, Any]] = []
        for port in product.output_ports:
            props = _port_custom_map(port)
            outputs.append(
                {
                    "name": port.name,
                    "contract_id": port.contract_id,
                    "version": port.version,
                    "dataset_id": props.get("dc43.dataset.id") or props.get("dc43.contract.ref"),
                    "stage_contract": props.get("dc43.stage.contract"),
                    "custom_properties": props,
                }
            )
        summaries.append(
            {
                "id": product.id,
                "name": product.name or product.id,
                "status": product.status,
                "version": product.version or "",
                "description": product.description or {},
                "tags": list(product.tags or []),
                "inputs": inputs,
                "outputs": outputs,
                "run_count": len(product_records),
                "latest_run": latest_record,
            }
        )
    summaries.sort(key=lambda item: item["id"])
    return summaries


def describe_data_product(
    product_id: str,
    records: Iterable[DatasetRecord],
) -> Dict[str, Any] | None:
    products = {doc.id: doc for doc in load_data_products()}
    product = products.get(product_id)
    if product is None:
        return None
    grouped = _records_by_data_product(records)
    product_records = list(grouped.get(product.id, []))
    product_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    input_ports: List[Dict[str, Any]] = []
    for port in product.input_ports:
        props = _port_custom_map(port)
        input_ports.append(
            {
                "name": port.name,
                "contract_id": port.contract_id,
                "version": port.version,
                "custom_properties": props,
                "source_data_product": props.get("dc43.input.source_data_product"),
                "source_output_port": props.get("dc43.input.source_output_port"),
            }
        )
    output_ports: List[Dict[str, Any]] = []
    for port in product.output_ports:
        props = _port_custom_map(port)
        dataset_id = props.get("dc43.dataset.id") or props.get("dc43.contract.ref")
        related_records = [
            rec
            for rec in product_records
            if rec.data_product_port == port.name
            or rec.dataset_name == dataset_id
            or rec.contract_id == port.contract_id
        ]
        output_ports.append(
            {
                "name": port.name,
                "contract_id": port.contract_id,
                "version": port.version,
                "dataset_id": dataset_id,
                "stage_contract": props.get("dc43.stage.contract"),
                "custom_properties": props,
                "records": related_records,
            }
        )
    return {
        "id": product.id,
        "name": product.name or product.id,
        "status": product.status,
        "version": product.version or "",
        "description": product.description or {},
        "custom_properties": product.custom_properties or [],
        "tags": list(product.tags or []),
        "inputs": input_ports,
        "outputs": output_ports,
        "records": product_records,
    }


def data_products_for_contract(contract_id: str, records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    grouped_records = _records_by_data_product(records)
    for product in load_data_products():
        for port in list(product.input_ports) + list(product.output_ports):
            if port.contract_id != contract_id:
                continue
            props = _port_custom_map(port)
            product_records = grouped_records.get(product.id, [])
            matches.append(
                {
                    "product_id": product.id,
                    "product_name": product.name or product.id,
                    "port_name": port.name,
                    "port_version": port.version,
                    "direction": "input" if port in product.input_ports else "output",
                    "records": product_records,
                    "custom_properties": props,
                }
            )
    matches.sort(key=lambda item: (item["product_id"], item["port_name"]))
    return matches


def data_products_for_dataset(dataset_name: str, records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    associations: List[Dict[str, Any]] = []
    for record in records:
        if record.dataset_name != dataset_name or not record.data_product_id:
            continue
        associations.append(
            {
                "product_id": record.data_product_id,
                "port_name": record.data_product_port,
                "role": record.data_product_role,
                "status": record.status,
                "dataset_version": record.dataset_version,
            }
        )
    associations.sort(key=lambda item: _version_sort_key(item.get("dataset_version") or ""))
    return associations



def dataset_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    """Summarise known datasets and associated contract information."""

    grouped: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if not record.dataset_name:
            continue
        bucket = grouped.setdefault(
            record.dataset_name,
            {"dataset_name": record.dataset_name, "records": []},
        )
        bucket["records"].append(record)

    for contract_id in store.list_contracts():
        for version in store.list_versions(contract_id):
            try:
                contract = store.get(contract_id, version)
            except FileNotFoundError:
                continue
            server_info = _server_details(contract) or {}
            dataset_id = (
                server_info.get("dataset_id")
                or server_info.get("dataset")
                or contract.id
                or contract_id
            )
            bucket = grouped.setdefault(
                dataset_id,
                {"dataset_name": dataset_id, "records": []},
            )
            contracts_map = bucket.setdefault("contracts_by_id", {})
            contracts = contracts_map.setdefault(contract_id, [])
            contracts.append(
                {
                    "version": version,
                    "status": getattr(contract, "status", ""),
                    "server": server_info,
                }
            )

    catalog: List[Dict[str, Any]] = []
    for dataset_name, payload in grouped.items():
        dataset_records: List[DatasetRecord] = list(payload.get("records", []))
        dataset_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
        latest_record: Optional[DatasetRecord] = dataset_records[-1] if dataset_records else None

        product_associations = data_products_for_dataset(dataset_name, dataset_records)
        product_summary_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        for association in product_associations:
            if not isinstance(association, Mapping):
                continue
            product_id = str(association.get("product_id") or "")
            port_name = str(association.get("port_name") or "")
            product_summary_map[(product_id, port_name)] = dict(association)
        product_summaries: List[Dict[str, Any]] = []
        for summary in product_summary_map.values():
            product_summaries.append(
                {
                    "product_id": summary.get("product_id"),
                    "port_name": summary.get("port_name"),
                    "role": summary.get("role"),
                    "latest_status": summary.get("status"),
                    "latest_dataset_version": summary.get("dataset_version"),
                }
            )
        product_summaries.sort(
            key=lambda item: (
                str(item.get("product_id") or ""),
                str(item.get("port_name") or ""),
            )
        )

        latest_status_value: Optional[str] = None
        latest_status_label: str = ""
        latest_status_badge: str = ""
        latest_reason: Optional[str] = None
        if latest_record:
            status_raw = str(latest_record.status or "unknown")
            latest_status_value = status_raw.lower()
            latest_status_label = status_raw.replace("_", " ").title()
            latest_status_badge = _DQ_STATUS_BADGES.get(latest_status_value, "bg-secondary")
            latest_reason = latest_record.reason or None

        run_drafts = sorted(
            {rec.draft_contract_version for rec in dataset_records if rec.draft_contract_version},
            key=_version_sort_key,
        )
        contracts_summary: List[Dict[str, Any]] = []
        contracts_map: Dict[str, List[Dict[str, Any]]] = payload.get("contracts_by_id", {})
        for contract_id, versions in contracts_map.items():
            versions.sort(key=lambda item: _version_sort_key(item["version"]))
            other_versions = [item["version"] for item in versions[:-1]]
            latest_contract = versions[-1] if versions else {"version": "", "status": ""}
            status_raw = str(latest_contract.get("status") or "unknown")
            status_value = status_raw.lower()
            status_label = status_raw.replace("_", " ").title()
            draft_versions = [
                item for item in versions if str(item.get("status", "")).lower() == "draft"
            ]
            latest_draft = draft_versions[-1]["version"] if draft_versions else None
            contracts_summary.append(
                {
                    "id": contract_id,
                    "latest_version": latest_contract.get("version", ""),
                    "latest_status": status_value,
                    "latest_status_label": status_label,
                    "other_versions": other_versions,
                    "drafts_count": len(draft_versions),
                    "latest_draft_version": latest_draft,
                }
            )

        contracts_summary.sort(key=lambda item: item["id"])

        catalog.append(
            {
                "dataset_name": dataset_name,
                "latest_version": latest_record.dataset_version if latest_record else "",
                "latest_status": latest_status_value,
                "latest_status_label": latest_status_label,
                "latest_status_badge": latest_status_badge,
                "latest_record_reason": latest_reason,
                "contract_summaries": contracts_summary,
                "data_products": product_summaries,
                "run_drafts_count": len(run_drafts),
                "run_latest_draft_version": run_drafts[-1] if run_drafts else None,
            }
        )

    catalog.sort(key=lambda item: item["dataset_name"])
    return catalog


@router.get("/datasets/{dataset_name}/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_name: str, dataset_version: str) -> HTMLResponse:
    records = load_records()
    associations = data_products_for_dataset(dataset_name, records)
    for r in records:
        if r.dataset_name == dataset_name and r.dataset_version == dataset_version:
            contract_obj: OpenDataContractStandard | None = None
            if r.contract_id and r.contract_version:
                try:
                    contract_obj = store.get(r.contract_id, r.contract_version)
                except FileNotFoundError:
                    contract_obj = None
            preview = _dataset_preview(contract_obj, dataset_name, dataset_version)
            context = {
                "request": request,
                "record": r,
                "contract": contract_to_dict(contract_obj) if contract_obj else None,
                "data_preview": preview,
                "data_products": associations,
            }
            return templates.TemplateResponse("dataset_detail.html", context)
    raise HTTPException(status_code=404, detail="Dataset not found")


def create_app() -> FastAPI:
    """Return a FastAPI application serving contract and dataset views."""

    application = FastAPI(title="DC43 Contracts App")

    @application.middleware("http")
    async def setup_guard(request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        exempt_paths = {"/openapi.json"}
        exempt_prefixes = ("/setup", "/static", "/docs", "/redoc")
        if path in exempt_paths or any(path.startswith(prefix) for prefix in exempt_prefixes):
            request.state.setup_required = not is_setup_complete()
            return await call_next(request)

        setup_done = is_setup_complete()
        request.state.setup_required = not setup_done
        if not setup_done and path == "/":
            return RedirectResponse(url="/setup", status_code=307)
        return await call_next(request)

    application.mount(
        "/static",
        StaticFiles(directory=str(BASE_DIR / "static"), check_dir=False),
        name="static",
    )
    application.include_router(router)
    return application


app = create_app()


def run(config_path: str | os.PathLike[str] | None = None) -> None:  # pragma: no cover - convenience runner
    """Run the demo UI and spawn a dedicated backend server."""

    import uvicorn

    config = configure_from_config(load_config(config_path))
    backend_cfg = config.backend
    process_cfg = backend_cfg.process
    backend_host = process_cfg.host
    backend_port = process_cfg.port
    backend_url = backend_cfg.base_url or process_cfg.url()

    env = os.environ.copy()
    env.setdefault("DC43_CONTRACT_STORE", str(CONTRACT_DIR))
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "dc43_service_backends.webapp:app",
        "--host",
        backend_host,
        "--port",
        str(backend_port),
    ]
    log_level = process_cfg.log_level
    if log_level:
        cmd.extend(["--log-level", log_level])

    process = subprocess.Popen(cmd, env=env)

    try:
        _wait_for_backend(backend_url)
    except Exception:
        process.terminate()
        process.wait(timeout=5)
        raise

    try:
        configure_backend(base_url=backend_url)
        uvicorn.run("dc43_contracts_app.server:app", host="0.0.0.0", port=8000)
    finally:
        process.terminate()
        with contextlib.suppress(Exception):
            process.wait(timeout=5)
        configure_from_config(config)
