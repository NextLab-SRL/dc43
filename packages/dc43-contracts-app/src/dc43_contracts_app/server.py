from __future__ import annotations

"""Contracts application built on FastAPI.

This UI surfaces contract and data product metadata through the configured
service backends. Run it locally with::

    uvicorn dc43_contracts_app.server:app --reload

Optional dependencies needed: ``fastapi``, ``uvicorn`` and ``jinja2``. Data
preview features additionally rely on ``pyspark``.
"""

import asyncio
import io
import contextlib
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from functools import lru_cache, partial
from pathlib import Path
from typing import (
    List,
    Dict,
    Any,
    Tuple,
    Mapping,
    Optional,
    Iterable,
    Callable,
    Set,
    Sequence,
)
from uuid import uuid4
from threading import Lock
import json
import importlib.metadata as importlib_metadata
import os
import re
import shutil
import tempfile
import textwrap
from datetime import datetime, timezone
from collections import Counter
from decimal import Decimal
import zipfile

import httpx
from fastapi import APIRouter, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from fastapi.concurrency import run_in_threadpool

from dc43_service_backends.config import (
    AuthConfig as BackendAuthConfig,
    ContractStoreConfig as BackendContractStoreConfig,
    DataProductStoreConfig as BackendDataProductStoreConfig,
    DataQualityBackendConfig as BackendDataQualityConfig,
    GovernanceConfig as BackendGovernanceConfig,
    GovernanceStoreConfig as BackendGovernanceStoreConfig,
    ServiceBackendsConfig,
    UnityCatalogConfig as BackendUnityCatalogConfig,
    dumps as dump_service_backends_config,
)
from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    ODPS_REQUIRED,
    OpenDataProductStandard,
)
from dc43_service_clients.data_quality.models import ValidationResult
from dc43_service_clients.data_quality.transport import decode_validation_result
from dc43_service_clients.governance.models import DatasetContractStatus
from ._odcs import custom_properties_dict, normalise_custom_properties
from ._versioning import SemVer
from .config import (
    BackendConfig,
    BackendProcessConfig,
    ContractsAppConfig,
    DocsChatConfig,
    WorkspaceConfig,
    dumps as dump_contracts_app_config,
    load_config,
    mapping_to_toml,
)
from .services import (
    configure_backend,
    contract_service_client,
    contract_versions,
    data_product_service_client,
    data_product_versions,
    data_quality_service_client,
    dataset_pipeline_activity,
    dataset_status_matrix,
    dataset_validation_status,
    get_contract,
    get_data_product,
    governance_service_client,
    latest_contract,
    latest_data_product,
    list_contract_ids,
    list_data_product_ids,
    list_dataset_ids,
    put_contract,
    put_data_product,
    service_backends_config,
    thread_service_clients,
)
from .hints import get_workspace_hints
from . import docs_chat
from .setup_bundle import PipelineExample, render_pipeline_stub
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
    from dc43_integrations.spark.io import (
        ContractVersionLocator,
        GovernanceSparkReadRequest,
        read_with_governance,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - safety net for CI
    if exc.name != "pyspark":
        raise
    ContractVersionLocator = None  # type: ignore[assignment]
    GovernanceSparkReadRequest = None  # type: ignore[assignment]
    read_with_governance = None  # type: ignore[assignment]

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
REPO_ROOT = BASE_DIR.parents[4]
TERRAFORM_TEMPLATE_ROOT = REPO_ROOT / "deploy" / "terraform"

_CONFIG_LOCK = Lock()
_ACTIVE_CONFIG: ContractsAppConfig | None = None


class _ServiceFacade:
    """Lazy wrapper exposing service client methods via attribute access."""

    def __init__(self, supplier: Callable[[], object | None], *, name: str, optional: bool = False) -> None:
        self._supplier = supplier
        self._name = name
        self._optional = optional

    def _client(self) -> object | None:
        client = self._supplier()
        if client is None and not self._optional:
            raise RuntimeError(f"{self._name} client is not configured")
        return client

    def __getattr__(self, attribute: str) -> Any:  # pragma: no cover - thin proxy
        client = self._client()
        if client is None:
            raise AttributeError(f"{self._name} client is not available")
        return getattr(client, attribute)

    def __bool__(self) -> bool:  # pragma: no cover - convenience for callers
        if self._optional:
            return self._supplier() is not None
        return True


contract_service = _ServiceFacade(contract_service_client, name="contract service")
data_product_service = _ServiceFacade(
    data_product_service_client,
    name="data product service",
    optional=True,
)
dq_service = _ServiceFacade(
    data_quality_service_client,
    name="data quality service",
    optional=True,
)
governance_service = _ServiceFacade(
    governance_service_client,
    name="governance service",
    optional=True,
)


def _thread_service_clients() -> tuple[Any, Any, Any]:
    """Compatibility alias used by legacy tests and integrations."""

    return thread_service_clients()


def _workspace_hint(key: str, fallback: str = "") -> str:
    """Return the configured workspace hint for ``key`` if available."""

    hints = get_workspace_hints()
    if hints:
        value = hints.get(key)
        if value:
            return str(value)

    if key == "root":
        override = os.getenv("DC43_CONTRACTS_APP_WORK_DIR") or os.getenv(
            "DC43_DEMO_WORK_DIR"
        )
        if override:
            return str(Path(override).expanduser())

    return fallback


def _workspace_hint_map() -> Mapping[str, str]:
    """Return all registered workspace hints as a mapping."""

    hints = get_workspace_hints()
    if hints:
        return dict(hints)
    return {}


def _workspace_default(key: str, fallback: str = "") -> Callable[[], str]:
    """Return a ``default_factory`` that resolves ``key`` via workspace hints."""

    def _factory() -> str:
        return _workspace_hint(key, fallback)

    return _factory


def _state_root(config: ContractsAppConfig | None = None) -> Path:
    active = config or _current_config()
    override = os.getenv("DC43_CONTRACTS_APP_STATE_DIR")
    if override:
        return Path(override).expanduser()
    root = active.workspace.root
    if root:
        return Path(root).expanduser()
    return Path.home() / ".dc43-contracts-app"


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


def _parse_iso_datetime(value: object) -> datetime | None:
    """Return a :class:`datetime` parsed from ``value`` when possible."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _format_recorded_at(value: str) -> str:
    """Return a human-friendly timestamp label for ``value``."""

    dt = _parse_iso_datetime(value)
    if dt is None:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z").strip()


def _decode_metric_value(value: object | None) -> object | None:
    """Return ``value`` with JSON-wrapped payloads decoded when possible."""

    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return ""
    if text[0] in "{[\"" and text[-1] in "}]\"":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value


def _coerce_numeric(value: object | None) -> float | None:
    """Return ``value`` as a :class:`float` when it resembles a number."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _format_metric_value(numeric: object | None, raw: object | None) -> str:
    """Return the preferred display string for a metric value."""

    if raw is not None:
        if raw == "":
            return "—"
        if isinstance(raw, (dict, list)):
            try:
                return json.dumps(raw, sort_keys=True)
            except (TypeError, ValueError):  # pragma: no cover - defensive
                return str(raw)
        if isinstance(raw, float):
            return format(raw, "g")
        return str(raw)
    if numeric is not None:
        if isinstance(numeric, float):
            return format(numeric, "g")
        return str(numeric)
    return "—"


def _metric_group_sort_key(item: Mapping[str, Any]) -> tuple[object, ...]:
    sort_key = item.get("_sort_key")
    recorded_at = str(item.get("recorded_at") or "")
    dataset_version = str(item.get("dataset_version") or "")
    contract_id = str(item.get("contract_id") or "")
    contract_version = str(item.get("contract_version") or "")
    if isinstance(sort_key, (int, float)):
        return (0, -float(sort_key), recorded_at, dataset_version, contract_id, contract_version)
    return (1, recorded_at, dataset_version, contract_id, contract_version)


def _empty_metrics_summary() -> Dict[str, Any]:
    """Return a metrics summary structure with no observations."""

    return {
        "latest": None,
        "previous": [],
        "history": [],
        "metric_keys": [],
        "numeric_metric_keys": [],
        "chronological_history": [],
        "contract_filters": [],
    }


def _contract_filter_fallbacks(
    records: Iterable[Mapping[str, Any]]
) -> dict[str, dict[str, set[str]]]:
    """Return contract/version fallbacks derived from dataset history rows."""

    fallbacks: dict[str, dict[str, set[str]]] = {}
    for record in records:
        contract_id = str(record.get("contract_id") or "").strip()
        if not contract_id:
            continue
        entry = fallbacks.setdefault(
            contract_id,
            {"contract_versions": set(), "dataset_versions": set()},
        )
        contract_version = str(record.get("contract_version") or "").strip()
        dataset_version = str(record.get("dataset_version") or "").strip()
        if contract_version:
            entry["contract_versions"].add(contract_version)
        if dataset_version:
            entry["dataset_versions"].add(dataset_version)
    return fallbacks


def _summarise_metrics(
    entries: Sequence[Mapping[str, object]],
    *,
    fallback_contract_versions: Mapping[str, Mapping[str, Iterable[str]]] | None = None,
) -> Dict[str, Any]:
    """Group raw metric rows into timestamped snapshots for the templates."""

    if not entries:
        return _empty_metrics_summary()

    grouped: dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    keys: set[str] = set()
    numeric_keys: set[str] = set()
    contract_versions: dict[str, set[str]] = {}
    contract_dataset_versions: dict[str, set[str]] = {}
    for entry in entries:
        recorded_at = str(entry.get("status_recorded_at") or "")
        dataset_version = str(entry.get("dataset_version") or "")
        contract_id = str(entry.get("contract_id") or "")
        contract_version = str(entry.get("contract_version") or "")
        group_key = (recorded_at, dataset_version, contract_id, contract_version)
        group = grouped.get(group_key)
        if group is None:
            dt = _parse_iso_datetime(recorded_at)
            group = {
                "recorded_at": recorded_at,
                "recorded_label": _format_recorded_at(recorded_at) if recorded_at else "",
                "dataset_version": dataset_version,
                "contract_id": contract_id,
                "contract_version": contract_version,
                "metrics": [],
                "_sort_key": dt.timestamp() if dt is not None else None,
            }
            grouped[group_key] = group
        metric_key = str(entry.get("metric_key") or "")
        if metric_key:
            keys.add(metric_key)
        metric_value = _decode_metric_value(entry.get("metric_value"))
        numeric_value = entry.get("metric_numeric_value")
        coerced_numeric = _coerce_numeric(numeric_value)
        if coerced_numeric is None:
            coerced_numeric = _coerce_numeric(metric_value)
        if metric_key and coerced_numeric is not None:
            numeric_keys.add(metric_key)
        group["metrics"].append(
            {
                "key": metric_key,
                "value": _format_metric_value(coerced_numeric, metric_value),
                "raw_value": metric_value,
                "numeric_value": coerced_numeric,
            }
        )
        if contract_id:
            versions = contract_versions.setdefault(contract_id, set())
            if contract_version:
                versions.add(contract_version)
            dataset_versions = contract_dataset_versions.setdefault(
                contract_id, set()
            )
            if dataset_version:
                dataset_versions.add(dataset_version)

    snapshots = sorted(grouped.values(), key=_metric_group_sort_key)
    for snapshot in snapshots:
        snapshot.pop("_sort_key", None)
        snapshot["metrics"].sort(key=lambda item: item.get("key", ""))

    latest = snapshots[0] if snapshots else None
    previous = snapshots[1:] if len(snapshots) > 1 else []
    fallbacks = fallback_contract_versions or {}
    filters = []
    contract_ids = sorted(set(contract_versions) | set(fallbacks))
    for contract_id in contract_ids:
        versions = contract_versions.get(contract_id)
        dataset_version_candidates = contract_dataset_versions.get(contract_id, set())
        fallback_entry = fallbacks.get(contract_id) or {}
        fallback_contract_versions = {
            str(value)
            for value in fallback_entry.get("contract_versions", [])
            if str(value)
        }
        fallback_dataset_versions = {
            str(value)
            for value in fallback_entry.get("dataset_versions", [])
            if str(value)
        }
        explicit_versions = sorted({str(value) for value in (versions or set()) if str(value)})
        version_source = "contract"
        if not explicit_versions and fallback_contract_versions:
            explicit_versions = sorted(fallback_contract_versions)
        dataset_versions = dataset_version_candidates or fallback_dataset_versions
        dataset_version_values = sorted({str(value) for value in dataset_versions if str(value)})
        if not explicit_versions and dataset_version_values:
            explicit_versions = dataset_version_values
            version_source = "dataset"
        if not explicit_versions:
            continue
        filters.append(
            {
                "contract_id": contract_id,
                "label": contract_id,
                "versions": explicit_versions,
                "version_source": version_source,
            }
        )

    return {
        "latest": latest,
        "previous": previous,
        "history": snapshots,
        "chronological_history": list(reversed(snapshots)),
        "metric_keys": sorted(keys),
        "numeric_metric_keys": sorted(numeric_keys),
        "contract_filters": filters,
    }


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

def _setup_state_path() -> Path:
    """Return the path that stores onboarding progress information."""

    return _state_root() / "setup_state.json"


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
    temp_path: Optional[str] = None
    fd = -1
    try:
        fd, temp_path = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(serialisable, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        fd = -1  # file descriptor ownership moved to context manager
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if fd != -1:
            os.close(fd)
        if temp_path:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path)


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
    configure_backend(config=config.backend)
    base_dir = config.workspace.root
    docs_chat.configure(config.docs_chat, base_dir=base_dir)

    def _log_warmup(detail: str) -> None:
        logger.info("Docs chat warm-up: %s", detail)

    docs_chat.warm_up(progress=_log_warmup)
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
    service = data_quality_service_client()
    if service is None:
        return {}
    plan = await asyncio.to_thread(service.describe_expectations, contract=contract)
    mapping: Dict[str, str] = {}
    for item in plan:
        key = item.get("key") if isinstance(item, Mapping) else None
        predicate = item.get("predicate") if isinstance(item, Mapping) else None
        if isinstance(key, str) and isinstance(predicate, str):
            mapping[key] = predicate
    return mapping

router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/docs-chat", response_class=HTMLResponse)
async def docs_chat_view(request: Request) -> HTMLResponse:
    status_payload = docs_chat.status()
    context = {
        "request": request,
        "docs_chat_status": status_payload,
        "gradio_path": docs_chat.GRADIO_MOUNT_PATH,
    }
    return templates.TemplateResponse("docs_chat.html", context)


@router.post("/api/docs-chat/messages")
async def docs_chat_message(payload: dict[str, Any]) -> JSONResponse:
    message = payload.get("message") if isinstance(payload, Mapping) else None
    if not isinstance(message, str) or not message.strip():
        raise HTTPException(status_code=422, detail="Provide a question so the assistant can help.")

    history_raw = payload.get("history") if isinstance(payload, Mapping) else None
    history: list[Any]
    if isinstance(history_raw, list):
        history = history_raw
    else:
        history = []

    status_payload = docs_chat.status()
    if not status_payload.enabled:
        detail = status_payload.message or "Docs chat is disabled in the current configuration."
        raise HTTPException(status_code=400, detail=detail)
    if not status_payload.ready:
        detail = status_payload.message or "Docs chat is not ready yet."
        raise HTTPException(status_code=400, detail=detail)

    try:
        reply = await run_in_threadpool(docs_chat.generate_reply, message, history)
    except docs_chat.DocsChatError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse({"message": reply.answer, "sources": reply.sources, "steps": reply.steps})


SETUP_MODULES: Dict[str, Dict[str, Any]] = {
    "contracts_backend": {
        "title": "Contracts storage backend",
        "summary": "Choose where contract definitions and their history are persisted.",
        "options": {
            "filesystem": {
                "label": "Local filesystem",
                "description": "Store JSON/YAML contract assets on a mounted volume that the UI can read and write.",
                "installation": [
                    "Create a persistent volume (for example `./volumes/contracts`) and mount it under the working directory.",
                    "Ensure backup or version control for the mounted folder so contract changes are traceable.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_APP_BACKEND_MODE=embedded` so the UI boots the bundled backend.",
                    "Expose the directories below through `DC43_CONTRACTS_APP_WORK_DIR` when running inside Docker.",
                ],
                "fields": [
                    {
                        "name": "work_dir",
                        "label": "Workspace root",
                        "placeholder": "/workspace",
                        "help": "Root directory that will be bound to `DC43_CONTRACTS_APP_WORK_DIR`.",
                        "default_factory": _workspace_default("root"),
                    },
                    {
                        "name": "contracts_dir",
                        "label": "Contracts directory",
                        "placeholder": "/workspace/contracts",
                        "help": "Path where contract files will be created (must be inside the workspace).",
                        "default_factory": _workspace_default("contracts_dir"),
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_filesystem",
                            "label": "Contracts workspace (filesystem)",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_filesystem",
                                    "label": "Stores contracts",
                                }
                            ],
                        }
                    ]
                },
            },
            "collibra": {
                "label": "Collibra governance backend",
                "description": "Persist contracts as assets inside a Collibra Data Governance domain.",
                "installation": [
                    "Provision a Collibra Cloud site with the Data Quality & Observability package enabled.",
                    "Create a service account with write access to the domain that will hold contract assets.",
                ],
                "configuration_notes": [
                    "Export credentials via `COLLIBRA_CLIENT_ID` and `COLLIBRA_CLIENT_SECRET` for the automation user.",
                    "Set `DC43_CONTRACTS_BACKEND=collibra` (or matching Helm/Docker overrides) to activate this connector.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Site URL",
                        "placeholder": "https://acme.collibra.com",
                        "help": "Fully qualified URL for the Collibra environment.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "collibra-service-client",
                        "help": "OAuth client identifier for the integration user.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "OAuth client secret stored in your secrets manager.",
                    },
                    {
                        "name": "domain_id",
                        "label": "Target domain",
                        "placeholder": "DATA_CONTRACTS",
                        "help": "Collibra domain that will contain the contract assets.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_collibra",
                            "label": "Collibra contracts domain",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_collibra",
                                    "label": "Syncs assets",
                                }
                            ],
                        }
                    ]
                },
            },
            "sql": {
                "label": "SQL database",
                "description": "Use a relational database for contract metadata with transactional guarantees.",
                "installation": [
                    "Provision the database (PostgreSQL, SQL Server, or compatible) and create a dedicated schema.",
                    "Apply migration scripts from `dc43-service-backends` to prepare the contracts tables.",
                ],
                "configuration_notes": [
                    "Populate `DC43_CONTRACTS_SQL_URL` with an application role and SSL enforced connection string.",
                    "Set `DC43_CONTRACTS_BACKEND=sql` so the services load the SQLAlchemy implementation.",
                ],
                "fields": [
                    {
                        "name": "connection_uri",
                        "label": "Connection URI",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/contracts",
                        "help": "SQLAlchemy compatible URI including credentials and database name.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Database schema where contract tables will be created.",
                    },
                    {
                        "name": "ssl_mode",
                        "label": "SSL mode",
                        "placeholder": "require",
                        "help": "SSL requirement flag passed to the driver (e.g. `require`, `verify-full`).",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_sql",
                            "label": "Contracts SQL database",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_sql",
                                    "label": "Persists metadata",
                                }
                            ],
                        }
                    ]
                },
            },
            "delta_lake": {
                "label": "Delta Lake (SQL-on-lake)",
                "description": "Back contracts with Delta tables so history is queryable via Spark or SQL endpoints.",
                "installation": [
                    "Create a Delta table (Unity Catalog, Hive metastore, or lakehouse) dedicated to contract revisions.",
                    "Grant the governance and product services read/write privileges on the catalog/schema pair.",
                ],
                "configuration_notes": [
                    "Set `DC43_CONTRACTS_BACKEND=delta` to enable the Delta implementation.",
                    "Provide either the Unity Catalog table name or an external Delta storage location; leave the storage location blank when using managed tables.",
                    "Populate `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (or a Databricks CLI profile) so the service can negotiate with Unity Catalog.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Delta storage location",
                        "placeholder": "s3://contracts-lake/contracts",
                        "help": "External Delta location (S3/ABFS/etc.). Leave blank when referencing a Unity-managed table below.",
                        "optional": True,
                    },
                    {
                        "name": "table_name",
                        "label": "Unity Catalog table (optional)",
                        "placeholder": "main.contracts.contract_store",
                        "help": "Fully qualified Unity Catalog table name when using a managed table instead of a storage path.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_url",
                        "label": "Databricks workspace URL (optional)",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL of the Databricks workspace that hosts the Delta catalog.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "unity-admin",
                        "help": "Profile name from databricks.cfg when using profile-based auth instead of a static token.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_token",
                        "label": "Workspace personal access token (optional)",
                        "placeholder": "dapi...",
                        "help": "PAT stored as `DATABRICKS_TOKEN` for Spark sessions that connect to Unity Catalog.",
                        "optional": True,
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog (optional)",
                        "placeholder": "main",
                        "help": "Unity Catalog or metastore catalog name if using managed tables.",
                        "optional": True,
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Schema or database that groups the contract Delta tables.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "contracts_backend_delta",
                            "label": "Contracts Delta Lake",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "contracts_backend",
                                    "to": "contracts_backend_delta",
                                    "label": "ACID table",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "products_backend": {
        "title": "Data products backend",
        "summary": "Decide where published product manifests are stored alongside operational metadata.",
        "options": {
            "filesystem": {
                "label": "Local filesystem",
                "description": "Reuse the mounted workspace to materialise product descriptors next to contracts.",
                "installation": [
                    "Mount a persistent directory that is shared between the producer pipelines and the UI.",
                    "Seed the folder with existing product definitions if you are migrating from another platform.",
                ],
                "configuration_notes": [
                    "Set `DC43_PRODUCTS_BACKEND=filesystem` so the services co-locate products with contracts.",
                    "Keep the directory consistent with the contracts workspace to ease local development.",
                ],
                "fields": [
                    {
                        "name": "products_dir",
                        "label": "Products directory",
                        "placeholder": "/workspace/products",
                        "help": "Folder that stores published product descriptors (JSON/YAML).",
                        "default_factory": _workspace_default("products_dir"),
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_filesystem",
                            "label": "Products workspace (filesystem)",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_filesystem",
                                    "label": "Stores products",
                                }
                            ],
                        }
                    ]
                },
            },
            "collibra": {
                "label": "Collibra domain",
                "description": "Manage product lifecycles directly in Collibra next to ownership metadata.",
                "installation": [
                    "Reuse the Collibra site used for contracts or provision a dedicated community for products.",
                    "Grant the automation account stewardship permissions for the target domain.",
                ],
                "configuration_notes": [
                    "Share the same credentials as the contracts backend or supply overrides below.",
                    "Export `DC43_PRODUCTS_BACKEND=collibra` when deploying the governance workflows.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Site URL",
                        "placeholder": "https://acme.collibra.com",
                        "help": "URL of the Collibra instance hosting product assets.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "collibra-products-client",
                        "help": "OAuth client for the product synchronisation job.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "Store this secret securely – it is required for API authentication.",
                    },
                    {
                        "name": "domain_id",
                        "label": "Products domain",
                        "placeholder": "DATA_PRODUCTS",
                        "help": "Collibra domain identifier where product assets are stored.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_collibra",
                            "label": "Collibra products domain",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_collibra",
                                    "label": "Syncs assets",
                                }
                            ],
                        }
                    ]
                },
            },
            "sql": {
                "label": "SQL database",
                "description": "Persist product manifests and release checkpoints in a relational database.",
                "installation": [
                    "Create the schema that will store product metadata and grant DDL/DML to the automation role.",
                    "Execute the database migrations from the product service package if available for your engine.",
                ],
                "configuration_notes": [
                    "Populate `DC43_PRODUCTS_SQL_URL` (or Helm secret) with an SSL-enabled connection string.",
                    "Enable the connector with `DC43_PRODUCTS_BACKEND=sql` to align with the contracts backend.",
                ],
                "fields": [
                    {
                        "name": "connection_uri",
                        "label": "Connection URI",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/products",
                        "help": "SQLAlchemy compatible URI for the product metadata database.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "products",
                        "help": "Schema that contains the product tables and views.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_sql",
                            "label": "Products SQL database",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_sql",
                                    "label": "Persists metadata",
                                }
                            ],
                        }
                    ]
                },
            },
            "delta_lake": {
                "label": "Delta Lake",
                "description": "Use Delta tables (local lakehouse or Unity Catalog) to track product releases.",
                "installation": [
                    "Provision an external location or managed catalog for the product Delta tables.",
                    "Grant the publishing pipelines and governance hook read/write access to the location.",
                ],
                "configuration_notes": [
                    "Set `DC43_PRODUCTS_BACKEND=delta` to keep the configuration consistent with contracts.",
                    "Provide either the Unity Catalog table name or an external Delta storage location; leave the storage location blank when using managed tables.",
                    "Provide `DATABRICKS_HOST`/`DATABRICKS_TOKEN` or a CLI profile so publishing jobs can reach the workspace.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Delta storage location",
                        "placeholder": "s3://contracts-lake/products",
                        "help": "External Delta location (S3/ABFS/etc.). Leave blank when referencing a Unity-managed table below.",
                        "optional": True,
                    },
                    {
                        "name": "table_name",
                        "label": "Unity Catalog table (optional)",
                        "placeholder": "main.products.catalogue",
                        "help": "Fully qualified Unity Catalog table name when publishing to managed tables.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_url",
                        "label": "Databricks workspace URL (optional)",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL of the Databricks workspace backing the Delta tables.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "unity-admin",
                        "help": "Profile configured in databricks.cfg when avoiding inline PATs.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_token",
                        "label": "Workspace personal access token (optional)",
                        "placeholder": "dapi...",
                        "help": "Token exported as `DATABRICKS_TOKEN` for Spark or REST clients.",
                        "optional": True,
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog",
                        "placeholder": "main",
                        "help": "Unity Catalog or Hive metastore catalog containing the product Delta tables.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "products",
                        "help": "Schema or database that holds the product Delta tables.",
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "products_backend_delta",
                            "label": "Products Delta Lake",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "products_backend",
                                    "to": "products_backend_delta",
                                    "label": "ACID table",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "data_quality": {
        "title": "Data quality service",
        "summary": "Select the engine that evaluates expectations against your datasets.",
        "default_option": "embedded_engine",
        "options": {
            "embedded_engine": {
                "label": "Embedded local engine",
                "description": "Run the bundled expectation runner alongside the contracts UI (default).",
                "installation": [
                    "Mount a directory for expectation suites and validation results inside the workspace.",
                    "Install any additional libraries required by your custom expectations (Great Expectations, soda-core, …).",
                ],
                "configuration_notes": [
                    "Set `DC43_DQ_ENGINE=local` to confirm the UI should manage execution locally.",
                    "Point the directories below to locations writable by the container user.",
                ],
                "fields": [
                    {
                        "name": "expectations_path",
                        "label": "Expectations directory",
                        "placeholder": "/workspace/expectations",
                        "help": "Where `.yml`/`.json` suites defining rules are stored.",
                        "default_factory": _workspace_default("expectations_dir"),
                    },
                    {
                        "name": "results_path",
                        "label": "Results directory",
                        "placeholder": "/workspace/expectations/results",
                        "help": "Optional folder where validation run outputs are persisted.",
                        "optional": True,
                    },
                ],
            },
            "remote_http": {
                "label": "Remote data-quality API",
                "description": "Delegate expectation evaluation to an external observability service that persists validation results in your chosen storage backend.",
                "installation": [
                    "Deploy the dc43 data-quality backend (or a compatible API) near the storage system that should hold validation outcomes.",
                    "Expose the HTTPS endpoint to the contracts UI container or automation environment.",
                ],
                "configuration_notes": [
                    "Set `DC43_DATA_QUALITY_BACKEND_TYPE=http` so backends use the HTTP delegate.",
                    "Provide the service URL and credentials via `DC43_DATA_QUALITY_BACKEND_URL` and `DC43_DATA_QUALITY_BACKEND_TOKEN` (plus optional header/scheme overrides).",
                    "Include any static headers your observability platform requires (for example organisation or workspace identifiers).",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Service base URL",
                        "placeholder": "https://quality.example.com",
                        "help": "HTTPS endpoint for the remote data-quality API.",
                    },
                    {
                        "name": "api_token",
                        "label": "API token (optional)",
                        "placeholder": "quality-token",
                        "help": "Bearer or PAT credential presented to the remote data-quality service.",
                        "optional": True,
                    },
                    {
                        "name": "token_header",
                        "label": "Token header (optional)",
                        "placeholder": "Authorization",
                        "help": "Override the HTTP header used to pass the token (defaults to Authorization).",
                        "optional": True,
                    },
                    {
                        "name": "token_scheme",
                        "label": "Token scheme (optional)",
                        "placeholder": "Bearer",
                        "help": "Override the scheme/prefix that precedes the token value.",
                        "optional": True,
                    },
                    {
                        "name": "default_engine",
                        "label": "Default expectation engine (optional)",
                        "placeholder": "soda",
                        "help": "Engine identifier requested when contracts omit an explicit engine.",
                        "optional": True,
                    },
                    {
                        "name": "extra_headers",
                        "label": "Additional headers (optional)",
                        "placeholder": "X-Org=governance,X-Team=data-quality",
                        "help": "Comma or newline separated key=value pairs appended to every request.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "governance_store": {
        "title": "Validation results storage",
        "summary": "Select the persistence layer for validation statuses, dataset links, and pipeline activity.",
        "default_option": "embedded_memory",
        "options": {
            "embedded_memory": {
                "label": "In-memory cache (demo)",
                "description": "Keep validation results inside the application process. Recommended only for local demos.",
                "installation": [
                    "No additional dependencies required – data is discarded when the process restarts.",
                ],
                "configuration_notes": [
                    "Leave `governance_store.type` unset (defaults to `memory`).",
                    "Suitable for proof-of-concept environments where durability is not required.",
                ],
                "fields": [],
            },
            "filesystem": {
                "label": "Filesystem archive",
                "description": "Persist validation history as JSON files on a mounted volume that pipelines can inspect.",
                "installation": [
                    "Provision a durable volume (for example an NFS export or cloud file share).",
                    "Mount the volume into the container running the governance services.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_STORE_TYPE=filesystem` to activate the JSON archive.",
                    "Point the directory below at a shared location accessible to operators and pipelines.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Governance storage directory",
                        "placeholder": "/workspace/governance",
                        "help": "Root folder that will contain status, link, and pipeline activity subdirectories.",
                        "default_factory": _workspace_default("governance_dir"),
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "governance_store_filesystem",
                            "label": "Validation archive (filesystem)",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "governance_store",
                                    "to": "governance_store_filesystem",
                                    "label": "Persists history",
                                }
                            ],
                        }
                    ]
                },
            },
            "sql": {
                "label": "SQL database",
                "description": "Write validation outcomes and pipeline activity to relational tables via SQLAlchemy.",
                "installation": [
                    "Provision a PostgreSQL, MySQL, or compatible database reachable from the governance services.",
                    "Create tables or grant schema ownership so the connector can manage them automatically.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_STORE_TYPE=sql` and export the DSN via `DC43_GOVERNANCE_STORE_DSN`.",
                    "Override table names if the default `dq_status`, `dq_activity`, and link tables already exist.",
                ],
                "fields": [
                    {
                        "name": "connection_uri",
                        "label": "Database connection URI",
                        "placeholder": "postgresql+psycopg://governance:secret@db.example.com/dc43",
                        "help": "SQLAlchemy-compatible DSN used to create the governance tables.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema (optional)",
                        "placeholder": "governance",
                        "help": "Schema or database namespace used for the governance tables.",
                        "optional": True,
                    },
                    {
                        "name": "status_table",
                        "label": "Status table (optional)",
                        "placeholder": "dq_status",
                        "help": "Table that stores the latest validation status for each dataset version.",
                        "optional": True,
                    },
                    {
                        "name": "activity_table",
                        "label": "Activity table (optional)",
                        "placeholder": "dq_activity",
                        "help": "Table that records pipeline activity and validation history.",
                        "optional": True,
                    },
                    {
                        "name": "link_table",
                        "label": "Dataset link table (optional)",
                        "placeholder": "dq_dataset_contract_links",
                        "help": "Table that maps dataset versions to governed contract versions.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "governance_store_sql",
                            "label": "Governance SQL schema",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "governance_store",
                                    "to": "governance_store_sql",
                                    "label": "Writes validation results",
                                }
                            ],
                        }
                    ]
                },
            },
            "delta_lake": {
                "label": "Delta Lake",
                "description": "Persist validation artefacts to Delta tables so Unity Catalog and lakehouse tooling can query them.",
                "installation": [
                    "Ensure the deployment environment has access to the target Delta Lake or Unity Catalog workspace.",
                    "Grant the service principal permission to write to the Delta location or managed tables.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_STORE_TYPE=delta` so the governance services use the Spark connector.",
                    "Provide either a base storage path for external tables or the fully qualified Unity table names below.",
                    "Populate `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (or profile) to allow Spark to reach Unity Catalog.",
                ],
                "fields": [
                    {
                        "name": "storage_path",
                        "label": "Delta storage location (optional)",
                        "placeholder": "s3://governance/validation",
                        "help": "Base folder for Delta tables when not using managed Unity Catalog tables.",
                        "optional": True,
                    },
                    {
                        "name": "status_table",
                        "label": "Status table name (optional)",
                        "placeholder": "main.governance.dq_status",
                        "help": "Fully qualified Unity Catalog table for validation status records.",
                        "optional": True,
                    },
                    {
                        "name": "activity_table",
                        "label": "Activity table name (optional)",
                        "placeholder": "main.governance.dq_activity",
                        "help": "Unity Catalog table that captures pipeline activity events.",
                        "optional": True,
                    },
                    {
                        "name": "link_table",
                        "label": "Dataset link table (optional)",
                        "placeholder": "main.governance.dq_dataset_contract_links",
                        "help": "Unity Catalog table mapping dataset versions to contract versions.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_url",
                        "label": "Databricks workspace URL (optional)",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL of the Databricks workspace hosting the Delta catalog.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "unity-admin",
                        "help": "Profile from databricks.cfg when authenticating without inline PATs.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_token",
                        "label": "Workspace personal access token (optional)",
                        "placeholder": "dapi...",
                        "help": "PAT stored as `DATABRICKS_TOKEN` so Spark can authenticate against Unity Catalog.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "governance_store_delta",
                            "label": "Validation Delta tables",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "governance_store",
                                    "to": "governance_store_delta",
                                    "label": "Stores outcomes",
                                }
                            ],
                        }
                    ]
                },
            },
            "remote_http": {
                "label": "Remote governance API",
                "description": "Proxy validation storage to an external observability service over HTTPS.",
                "installation": [
                    "Deploy a dc43-compatible governance API or integrate with an existing observability platform.",
                    "Expose the HTTPS endpoint and credentials to the contracts application environment.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_STORE_TYPE=http` to activate the HTTP persistence delegate.",
                    "Provide the base URL, token, and any additional headers required by the remote API.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Service base URL",
                        "placeholder": "https://governance.example.com",
                        "help": "HTTPS endpoint for the remote governance persistence API.",
                    },
                    {
                        "name": "api_token",
                        "label": "API token (optional)",
                        "placeholder": "governance-token",
                        "help": "Bearer or PAT credential presented to the remote governance service.",
                        "optional": True,
                    },
                    {
                        "name": "token_header",
                        "label": "Token header (optional)",
                        "placeholder": "Authorization",
                        "help": "Override the HTTP header that carries the authentication token.",
                        "optional": True,
                    },
                    {
                        "name": "token_scheme",
                        "label": "Token scheme (optional)",
                        "placeholder": "Bearer",
                        "help": "Override the scheme/prefix applied to the token value.",
                        "optional": True,
                    },
                    {
                        "name": "timeout",
                        "label": "Request timeout (seconds, optional)",
                        "placeholder": "10",
                        "help": "Override the HTTP client timeout when the remote service has higher latency.",
                        "optional": True,
                    },
                    {
                        "name": "extra_headers",
                        "label": "Additional headers (optional)",
                        "placeholder": "X-Org=governance,X-Team=quality",
                        "help": "Comma or newline separated key=value pairs appended to every request.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "governance_store_remote",
                            "label": "External governance API",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "governance_store",
                                    "to": "governance_store_remote",
                                    "label": "Delegates persistence",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "pipeline_integration": {
        "title": "Pipeline integration",
        "summary": "Document the orchestration technology that will load dc43 backends and drive contract-aware pipelines.",
        "options": {
            "spark": {
                "label": "Apache Spark",
                "description": "Run notebooks or jobs that manage Spark sessions and call dc43 integrations from PySpark.",
                "installation": [
                    "Install `pyspark` and `dc43-integrations[spark]` alongside the dc43 service clients.",
                    "Ensure the runtime can reach the same storage locations selected in the storage foundations step.",
                ],
                "configuration_notes": [
                    "Point the script below at the exported TOML so Spark jobs reuse the configured backends.",
                    "Set `DATABRICKS_HOST`/`DATABRICKS_TOKEN` or configure the CLI profile when targeting Databricks clusters.",
                ],
                "fields": [
                    {
                        "name": "runtime",
                        "label": "Execution environment (optional)",
                        "placeholder": "local[*], yarn, databricks job",
                        "help": "Describe where Spark runs so operators provision the matching cluster or session.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_url",
                        "label": "Databricks workspace URL (optional)",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL for Databricks jobs that execute the Spark pipelines.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "pipelines",
                        "help": "CLI/SDK profile used by Spark jobs when authenticating without inline PATs.",
                        "optional": True,
                    },
                    {
                        "name": "cluster_reference",
                        "label": "Cluster or job identifier (optional)",
                        "placeholder": "job:dc43-governance",
                        "help": "Record the cluster name, job ID, or workspace path that will execute Spark pipelines.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "pipeline_integration_spark",
                            "label": "Spark runtime",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "pipeline_integration",
                                    "to": "pipeline_integration_spark",
                                    "label": "Runs contracts pipelines",
                                }
                            ],
                        }
                    ]
                },
            },
            "dlt": {
                "label": "Databricks Delta Live Tables",
                "description": "Drive contract-aware workloads through managed DLT pipelines that call the dc43 clients.",
                "installation": [
                    "Enable the Delta Live Tables workspace features for the recorded environment.",
                    "Grant the service principal permission to manage the target DLT pipeline and destination schema.",
                ],
                "configuration_notes": [
                    "Use the exported script to seed notebooks with the correct client wiring.",
                    "Store access tokens in Databricks secret scopes and reference them via environment variables.",
                ],
                "fields": [
                    {
                        "name": "workspace_url",
                        "label": "Databricks workspace URL",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Workspace that hosts the DLT pipeline.",
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "dlt-admin",
                        "help": "CLI/SDK profile used when authenticating without inline PATs.",
                        "optional": True,
                    },
                    {
                        "name": "pipeline_name",
                        "label": "DLT pipeline name",
                        "placeholder": "dc43-contract-governance",
                        "help": "Human-friendly name for the DLT pipeline that will orchestrate contracts.",
                    },
                    {
                        "name": "notebook_path",
                        "label": "Notebook path (optional)",
                        "placeholder": "/Repos/team/contracts/dc43_pipeline",
                        "help": "Workspace notebook that loads the generated helper and defines DLT tables.",
                        "optional": True,
                    },
                    {
                        "name": "target_schema",
                        "label": "Target schema (optional)",
                        "placeholder": "main.governance",
                        "help": "Unity Catalog target schema configured for the DLT pipeline output.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "pipeline_integration_dlt",
                            "label": "DLT managed pipeline",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "pipeline_integration",
                                    "to": "pipeline_integration_dlt",
                                    "label": "Schedules tables",
                                }
                            ],
                        }
                    ]
                },
            },
        },
    },
    "governance_service": {
        "title": "Governance interface",
        "summary": "Decide whether orchestration runs in-process, via the bundled web service, or through a remote API.",
        "options": {
            "embedded_monolith": {
                "label": "Embedded web service (server.py)",
                "description": "Keep the all-in-one FastAPI application that exposes UI and service endpoints together.",
                "installation": [
                    "Expose the container port (default 8000) through Docker Compose or your orchestrator.",
                    "Mount the same workspace volume used by the contracts and product backends.",
                ],
                "configuration_notes": [
                    "No additional configuration is required beyond the workspace paths defined above.",
                    "Use `uvicorn dc43_contracts_app.server:app --reload` for local development mode.",
                ],
                "fields": [],
            },
            "direct_runtime": {
                "label": "Direct Python orchestration",
                "description": "Call governance, contracts, and product services in-process without exposing HTTP endpoints.",
                "installation": [
                    "Install the dc43 service backends (`pip install dc43-service-backends[contracts,products,dq]`) inside your orchestrator runtime.",
                    "Ensure the runtime has filesystem or network access to the storage targets configured above.",
                ],
                "configuration_notes": [
                    "Import the backend factories (for example `dc43_service_backends.governance`) directly from your pipelines or notebooks.",
                    "Reuse the same environment variables or generated configuration files from previous sections – no web server is required.",
                ],
                "fields": [],
            },
            "remote_api": {
                "label": "Remote governance API",
                "description": "Run governance orchestration as a standalone service and let the UI connect over HTTPS.",
                "installation": [
                    "Deploy the governance service package (for example `dc43-service-backends`) to your preferred platform.",
                    "Enable networking between the UI container and the remote governance endpoint.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_MODE=remote` (or Helm override) to disable the embedded orchestrator.",
                    "Provide the base URL and optional credentials below via environment variables or secrets.",
                ],
                "fields": [
                    {
                        "name": "base_url",
                        "label": "Governance API URL",
                        "placeholder": "https://governance.example.com",
                        "help": "HTTPS endpoint exposing the governance service.",
                    },
                    {
                        "name": "api_token",
                        "label": "API token",
                        "placeholder": "Optional bearer token",
                        "help": "Authentication token presented to the remote governance API.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "governance_deployment": {
        "title": "Governance service deployment",
        "summary": "Capture how the governance backends are hosted so the wizard can emit scripts or Terraform stubs per environment.",
        "depends_on": "governance_service",
        "visible_when": {
            "embedded_monolith": ["local_python", "local_docker"],
            "remote_api": [
                "local_python",
                "local_docker",
                "aws_terraform",
                "azure_terraform",
            ],
            "direct_runtime": ["not_required"],
        },
        "hide_when": ["direct_runtime"],
        "default_for": {
            "embedded_monolith": "local_python",
            "remote_api": "local_docker",
            "direct_runtime": "not_required",
        },
        "default_option": "local_python",
        "options": {
            "local_python": {
                "label": "Local Python process",
                "description": "Start the bundled FastAPI server directly with `uvicorn` for interactive development.",
                "installation": [
                    "Install the contracts app package into your virtual environment (for example `pip install -e .`).",
                    "Run `uvicorn dc43_contracts_app.server:app --reload --port 8000` from the workspace root.",
                ],
                "configuration_notes": [
                    "Reuse the TOML files exported by the wizard or environment variables to configure backends.",
                    "Ensure the process runs with access to the same workspace paths defined in the storage step.",
                ],
                "fields": [
                    {
                        "name": "command",
                        "label": "Launch command",
                        "placeholder": "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                        "help": "Command used by developers to start the governance service locally.",
                        "optional": True,
                        "default_factory": lambda: "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                    },
                ],
            },
            "local_docker": {
                "label": "Local Docker or Compose",
                "description": "Keep the governance service on a laptop or developer workstation using Docker Compose and the generated helper scripts.",
                "installation": [
                    "Install Docker Desktop or an equivalent container runtime.",
                    "Use `scripts/run_local_stack.py` from the exported bundle to start the governance APIs alongside the UI.",
                ],
                "configuration_notes": [
                    "Reuse the TOML files exported by the wizard to keep local environments consistent.",
                    "Commit the generated configuration into your infrastructure repo once you are happy with the defaults.",
                ],
                "fields": [],
            },
            "aws_terraform": {
                "label": "AWS (Terraform)",
                "description": "Provision the governance and data-quality APIs on AWS Fargate using the provided Terraform module skeleton.",
                "installation": [
                    "Clone the Terraform module under `deploy/terraform/aws-service-backend` into your infrastructure repository.",
                    "Ensure the AWS provider is authenticated (for example via `aws configure` or CI/CD secrets).",
                    "Run `terraform init` and `terraform apply` after reviewing the generated `terraform.tfvars` file.",
                ],
                "configuration_notes": [
                    "The wizard will emit a `terraform.tfvars` stub aligned with your selections – fill in any missing secrets before applying.",
                    "Set `contract_store_mode` to `sql` when the contracts backend uses a relational database, otherwise keep `filesystem` for the default volume-backed store.",
                ],
                "fields": [
                    {
                        "name": "aws_region",
                        "label": "AWS region",
                        "placeholder": "us-east-1",
                        "help": "Region where the ECS service will be created.",
                    },
                    {
                        "name": "cluster_name",
                        "label": "ECS cluster name",
                        "placeholder": "dc43-governance",
                        "help": "Existing ECS/Fargate cluster that will host the tasks.",
                    },
                    {
                        "name": "ecr_image_uri",
                        "label": "ECR image URI",
                        "placeholder": "123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-backends:latest",
                        "help": "Container image containing the dc43 service backends.",
                    },
                    {
                        "name": "backend_token",
                        "label": "Backend bearer token",
                        "placeholder": "Optional shared secret",
                        "help": "Optional token enforced by the HTTP API – keep in sync with the exported TOML.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_mode",
                        "label": "Contract store mode",
                        "placeholder": "filesystem",
                        "help": "`filesystem` for EFS backed storage or `sql` when using the relational implementation.",
                        "optional": True,
                    },
                    {
                        "name": "contract_filesystem",
                        "label": "EFS filesystem ID",
                        "placeholder": "fs-0123456789abcdef0",
                        "help": "File system identifier used when `contract_store_mode` is `filesystem`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_storage_path",
                        "label": "Container mount path",
                        "placeholder": "/contracts",
                        "help": "Directory inside the container exposing the contracts volume.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn",
                        "label": "SQL DSN",
                        "placeholder": "postgresql+psycopg://user:pass@host:5432/contracts",
                        "help": "Connection string used when `contract_store_mode` is `sql`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn_secret_arn",
                        "label": "DSN secret ARN",
                        "placeholder": "arn:aws:secretsmanager:...",
                        "help": "Secrets Manager or SSM parameter storing the DSN (alternative to plaintext).",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_table",
                        "label": "Contracts table name",
                        "placeholder": "contracts",
                        "help": "Override the default contracts table for the SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_schema",
                        "label": "Contracts schema",
                        "placeholder": "governance",
                        "help": "Optional schema/namespace for the contracts SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "private_subnet_ids",
                        "label": "Private subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs used by the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_subnet_ids",
                        "label": "Load balancer subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs for the public load balancer.",
                    },
                    {
                        "name": "service_security_group_id",
                        "label": "Service security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_security_group_id",
                        "label": "Load balancer security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the Application Load Balancer.",
                    },
                    {
                        "name": "certificate_arn",
                        "label": "ACM certificate ARN",
                        "placeholder": "arn:aws:acm:...",
                        "help": "Certificate used for HTTPS ingress.",
                    },
                    {
                        "name": "vpc_id",
                        "label": "VPC ID",
                        "placeholder": "vpc-0123456789abcdef0",
                        "help": "Virtual private cloud hosting the deployment.",
                    },
                    {
                        "name": "task_cpu",
                        "label": "Task CPU units",
                        "placeholder": "512",
                        "help": "Fargate CPU allocation for the service backends task.",
                        "optional": True,
                    },
                    {
                        "name": "task_memory",
                        "label": "Task memory (MiB)",
                        "placeholder": "1024",
                        "help": "Fargate memory allocation for the service backends task.",
                        "optional": True,
                    },
                    {
                        "name": "container_port",
                        "label": "Container port",
                        "placeholder": "8001",
                        "help": "Port exposed by the container.",
                        "optional": True,
                    },
                    {
                        "name": "desired_count",
                        "label": "Desired task count",
                        "placeholder": "2",
                        "help": "Number of task replicas to run.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_path",
                        "label": "Health check path",
                        "placeholder": "/health",
                        "help": "HTTP path polled by the load balancer.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_interval",
                        "label": "Health check interval",
                        "placeholder": "30",
                        "help": "Seconds between health checks.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_timeout",
                        "label": "Health check timeout",
                        "placeholder": "5",
                        "help": "Seconds before a health check is considered failed.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_healthy_threshold",
                        "label": "Healthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive successes before the target is considered healthy.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_unhealthy_threshold",
                        "label": "Unhealthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive failures before the target is considered unhealthy.",
                        "optional": True,
                    },
                    {
                        "name": "log_retention_days",
                        "label": "Log retention (days)",
                        "placeholder": "30",
                        "help": "CloudWatch log retention period.",
                        "optional": True,
                    },
                ],
            },
            "azure_terraform": {
                "label": "Azure (Terraform)",
                "description": "Deploy the governance APIs to Azure Container Apps using the bundled Terraform template.",
                "installation": [
                    "Copy the Terraform module under `deploy/terraform/azure-service-backend` into your infrastructure repository.",
                    "Authenticate the Azure CLI or provide service principal credentials to Terraform.",
                    "Review the generated `terraform.tfvars` and run `terraform apply` to provision the resources.",
                ],
                "configuration_notes": [
                    "Point the configuration at your container registry and decide whether the contracts store uses Azure Files or SQL.",
                    "Ensure the exported TOML configuration is mounted or baked into the container image.",
                ],
                "fields": [
                    {
                        "name": "subscription_id",
                        "label": "Subscription ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Azure subscription hosting the deployment.",
                    },
                    {
                        "name": "resource_group_name",
                        "label": "Resource group",
                        "placeholder": "rg-dc43-governance",
                        "help": "Resource group where the Container App and storage resources will be created.",
                    },
                    {
                        "name": "location",
                        "label": "Location",
                        "placeholder": "westeurope",
                        "help": "Azure region for the resources.",
                    },
                    {
                        "name": "container_registry",
                        "label": "Container registry",
                        "placeholder": "dc43.azurecr.io",
                        "help": "Azure Container Registry host serving the backend image.",
                    },
                    {
                        "name": "container_registry_username",
                        "label": "Registry username",
                        "placeholder": "dc43",
                        "help": "Username with pull permissions on the registry.",
                    },
                    {
                        "name": "container_registry_password",
                        "label": "Registry password",
                        "placeholder": "••••••",
                        "help": "Password or access key for the container registry.",
                    },
                    {
                        "name": "image_tag",
                        "label": "Image tag",
                        "placeholder": "dc43-backends:latest",
                        "help": "Full reference (repository:tag) for the container image.",
                    },
                    {
                        "name": "backend_token",
                        "label": "Backend bearer token",
                        "placeholder": "Optional shared secret",
                        "help": "Optional token enforced by the HTTP API – align with the exported TOML.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_mode",
                        "label": "Contract store mode",
                        "placeholder": "filesystem",
                        "help": "`filesystem` for Azure Files, `sql` for Azure SQL deployments.",
                        "optional": True,
                    },
                    {
                        "name": "contract_storage",
                        "label": "Container mount path",
                        "placeholder": "/contracts",
                        "help": "Directory inside the container exposing the Azure Files share.",
                        "optional": True,
                    },
                    {
                        "name": "contract_share_name",
                        "label": "Azure Files share name",
                        "placeholder": "contracts",
                        "help": "Name of the file share created for the filesystem-backed contracts store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_share_quota_gb",
                        "label": "Share quota (GiB)",
                        "placeholder": "100",
                        "help": "Optional quota for the Azure Files share in gibibytes.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_dsn",
                        "label": "SQL DSN",
                        "placeholder": "sqlserver://user:pass@host:1433;database=contracts",
                        "help": "Connection string used when `contract_store_mode` is `sql`.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_table",
                        "label": "Contracts table name",
                        "placeholder": "contracts",
                        "help": "Override the default contracts table for the SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "contract_store_schema",
                        "label": "Contracts schema",
                        "placeholder": "governance",
                        "help": "Optional schema/namespace for the contracts SQL store.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_environment_name",
                        "label": "Container Apps environment",
                        "placeholder": "dc43-env",
                        "help": "Name assigned to the Container Apps environment.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_name",
                        "label": "Container App name",
                        "placeholder": "dc43-service-backends",
                        "help": "Name of the Container App resource created by Terraform.",
                        "optional": True,
                    },
                    {
                        "name": "ingress_port",
                        "label": "Ingress port",
                        "placeholder": "8001",
                        "help": "Port exposed publicly by the Container App.",
                        "optional": True,
                    },
                    {
                        "name": "min_replicas",
                        "label": "Minimum replicas",
                        "placeholder": "1",
                        "help": "Lower bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "max_replicas",
                        "label": "Maximum replicas",
                        "placeholder": "3",
                        "help": "Upper bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "container_cpu",
                        "label": "Container CPU",
                        "placeholder": "0.5",
                        "help": "vCPU allocation per replica (for example 0.5, 1.0).",
                        "optional": True,
                    },
                    {
                        "name": "container_memory",
                        "label": "Container memory",
                        "placeholder": "1.0Gi",
                        "help": "Memory allocation per replica (GiB).",
                        "optional": True,
                    },
                    {
                        "name": "tags",
                        "label": "Resource tags",
                        "placeholder": "env=dev, owner=data-platform",
                        "help": "Comma-separated key=value pairs applied to Azure resources.",
                        "optional": True,
                    },
                ],
            },
            "not_required": {
                "label": "Not required (direct runtime)",
                "description": "Skip dedicated deployment automation when governance orchestration stays embedded in Python workloads.",
                "installation": [
                    "No standalone service is provisioned – pipelines import and execute governance code directly.",
                ],
                "configuration_notes": [
                    "Ensure orchestrators install `dc43-service-backends` and reference the generated TOML files.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
        },
    },
    "governance_extensions": {
        "title": "Governance hooks",
        "summary": "Extend the governance service with additional integrations such as Unity Catalog tagging.",
        "options": {
            "none": {
                "label": "No additional hooks",
                "description": "Skip optional extensions – contracts and products remain self-contained.",
                "installation": [],
                "configuration_notes": [
                    "Use this option when you do not need to synchronise Unity Catalog or other downstream systems yet.",
                ],
                "fields": [],
            },
            "unity_catalog": {
                "label": "Unity Catalog synchronisation",
                "description": "Tag Delta tables and views in Unity Catalog with contract and product metadata.",
                "installation": [
                    "Install the `dc43-integrations` wheel on the Databricks cluster executing the hook.",
                    "Grant the service principal data steward permissions on the target catalog and schema.",
                ],
                "configuration_notes": [
                    "Provide Databricks host/token pairs via `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.",
                    "Set `DC43_GOVERNANCE_HOOK=unity_catalog` so the governance job loads the extension module.",
                ],
                "fields": [
                    {
                        "name": "dataset_prefix",
                        "label": "Dataset prefix",
                        "placeholder": "table:",
                        "help": "Prefix added to contract dataset identifiers before tagging Unity tables.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_profile",
                        "label": "Databricks CLI profile (optional)",
                        "placeholder": "unity-admin",
                        "help": "Profile name from databricks.cfg when using profile-based authentication instead of host/token.",
                        "optional": True,
                    },
                    {
                        "name": "workspace_url",
                        "label": "Workspace URL",
                        "placeholder": "https://adb-1234567890123456.7.azuredatabricks.net",
                        "help": "Base URL of the Databricks workspace hosting Unity Catalog.",
                    },
                    {
                        "name": "catalog",
                        "label": "Catalog",
                        "placeholder": "main",
                        "help": "Unity Catalog containing the managed tables to tag.",
                    },
                    {
                        "name": "schema",
                        "label": "Schema",
                        "placeholder": "contracts",
                        "help": "Schema that will receive Unity Catalog tags.",
                    },
                    {
                        "name": "token",
                        "label": "Personal access token",
                        "placeholder": "dapi...",
                        "help": "Databricks PAT used by the governance hook.",
                    },
                    {
                        "name": "static_properties",
                        "label": "Static table properties (optional)",
                        "placeholder": "owner=governance-team,environment=prod",
                        "help": "Comma or newline separated key=value pairs applied to every Unity table update.",
                        "optional": True,
                    },
                ],
                "diagram": {
                    "nodes": [
                        {
                            "id": "unity_catalog_tables",
                            "label": "Unity Catalog tables",
                            "class": "external",
                            "edges": [
                                {
                                    "from": "governance_extensions",
                                    "to": "unity_catalog_tables",
                                    "label": "Applies tags",
                                }
                            ],
                        }
                    ]
                },
            },
            "custom_module": {
                "label": "Custom Python module",
                "description": "Load your own governance hook module for bespoke synchronisation steps.",
                "installation": [
                    "Package the Python module with your deployment or mount it into the container image.",
                    "Document the callable entrypoints (`register`, `on_publish`, …) expected by the governance runtime.",
                ],
                "configuration_notes": [
                    "Set `DC43_GOVERNANCE_HOOK=python` and provide the module path below.",
                    "Use virtualenv or Poetry extras to install dependencies required by the hook.",
                ],
                "fields": [
                    {
                        "name": "module_path",
                        "label": "Module import path",
                        "placeholder": "acme.governance.hooks",
                        "help": "Python import path that exposes the hook entrypoints.",
                    },
                    {
                        "name": "config_path",
                        "label": "Hook configuration file",
                        "placeholder": "/workspace/config/governance_hook.yml",
                        "help": "Optional YAML/JSON file consumed by the custom hook.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "user_interface": {
        "title": "User interface",
        "summary": "Choose how operators will interact with the governance workflows.",
        "options": {
            "local_web": {
                "label": "Bundled web application",
                "description": "Serve the FastAPI + Bootstrap UI directly from the same container as the services.",
                "installation": [
                    "Expose the web port (default 8000) and secure it behind your ingress/proxy of choice.",
                    "Mount the workspace volume so UI actions persist to the same storage as the services.",
                ],
                "configuration_notes": [
                    "Use environment variables from previous sections – no additional settings are required.",
                    "Set `DC43_UI_MODE=local` when packaging the UI with Docker Compose.",
                ],
                "fields": [],
            },
            "remote_portal": {
                "label": "Hosted portal",
                "description": "Point users to an externally hosted UI that talks to the governance API.",
                "installation": [
                    "Deploy the UI assets (for example via static hosting + backend-for-frontend layer).",
                    "Configure networking so the hosted portal can reach the remote governance service.",
                ],
                "configuration_notes": [
                    "Set `DC43_UI_MODE=remote` to disable the embedded templates.",
                    "Provide the portal base URL below so deep links are generated correctly.",
                ],
                "fields": [
                    {
                        "name": "portal_url",
                        "label": "Portal base URL",
                        "placeholder": "https://contracts.acme.com",
                        "help": "URL users will visit to access the hosted UI.",
                    },
                ],
            },
        },
    },
    "docs_assistant": {
        "title": "Documentation assistant",
        "summary": "Enable the bundled docs chat so operators can query dc43 guides without leaving the app.",
        "default_option": "disabled",
        "options": {
            "disabled": {
                "label": "Disabled",
                "description": "Skip the documentation chat experience for now.",
                "installation": [
                    "Leave the docs assistant turned off until credentials and dependencies are available.",
                ],
                "configuration_notes": [
                    "Re-run the wizard later to capture docs chat settings once the assistant should be exposed.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
            "openai_embedded": {
                "label": "Gradio assistant (OpenAI)",
                "description": "Use the LangChain + Gradio powered docs assistant backed by OpenAI models.",
                "installation": [
                    "Install the docs-chat extra: `pip install --no-cache-dir -e \".[demo]\"` (or `pip install \"dc43-contracts-app[docs-chat]\"`).",
                    "Do not chain both commands in the same environment—pip will report conflicting requirements when the local editable and wheel installs target the same package.",
                    "Expose the configured API key environment variable before starting the UI.",
                ],
                "configuration_notes": [
                    "The assistant indexes Markdown under `docs/` by default and persists a FAISS index alongside the workspace.",
                    "Override paths when bundling custom documentation or sharing an index across environments.",
                ],
                "fields": [
                    {
                        "name": "provider",
                        "label": "Provider ID",
                        "placeholder": "openai",
                        "default": "openai",
                    },
                    {
                        "name": "model",
                        "label": "Chat model",
                        "placeholder": "gpt-4o-mini",
                        "default": "gpt-4o-mini",
                    },
                    {
                        "name": "embedding_model",
                        "label": "Embedding model",
                        "placeholder": "text-embedding-3-small",
                        "default": "text-embedding-3-small",
                    },
                    {
                        "name": "api_key_env",
                        "label": "API key environment variable",
                        "placeholder": "OPENAI_API_KEY",
                        "default": "OPENAI_API_KEY",
                    },
                    {
                        "name": "docs_path",
                        "label": "Documentation directory override",
                        "placeholder": "~/dc43/docs",
                        "optional": True,
                    },
                    {
                        "name": "index_path",
                        "label": "Vector index directory override",
                        "placeholder": "~/dc43/docs-index",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "ui_deployment": {
        "title": "User interface deployment",
        "summary": "Document how the contracts UI is hosted so deployment scripts and Terraform variables can be generated per environment.",
        "depends_on": "user_interface",
        "visible_when": {
            "local_web": ["skip_hosting"],
            "local_python": ["local_python"],
            "local_docker": ["local_docker"],
            "remote_portal": ["skip_hosting", "aws_terraform", "azure_terraform"],
            "aws_terraform": ["aws_terraform"],
            "azure_terraform": ["azure_terraform"],
            "skip_hosting": ["skip_hosting"],
        },
        "hide_when": ["local_web", "skip_hosting"],
        "default_for": {
            "local_web": "skip_hosting",
            "local_python": "local_python",
            "local_docker": "local_docker",
            "remote_portal": "skip_hosting",
            "aws_terraform": "aws_terraform",
            "azure_terraform": "azure_terraform",
            "skip_hosting": "skip_hosting",
        },
        "default_option": "skip_hosting",
        "options": {
            "local_python": {
                "label": "Local Python process",
                "description": "Run the UI with the bundled FastAPI server directly from your shell (no containers).",
                "installation": [
                    "Install the contracts app package and its extras into a virtual environment.",
                    "Start the UI with `uvicorn dc43_contracts_app.server:app --reload --port 8000` and keep the terminal session running.",
                ],
                "configuration_notes": [
                    "Load the generated TOML files via `DC43_CONTRACTS_APP_CONFIG` or environment variables.",
                    "Ensure the workspace folders selected earlier are accessible on the host machine.",
                ],
                "fields": [
                    {
                        "name": "command",
                        "label": "Launch command",
                        "placeholder": "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                        "help": "Command your operators should run to bring up the UI locally.",
                        "optional": True,
                        "default_factory": lambda: "uvicorn dc43_contracts_app.server:app --reload --port 8000",
                    },
                ],
            },
            "local_docker": {
                "label": "Local Docker or Compose",
                "description": "Run the UI from the same container that serves governance by using the helper scripts provided by the wizard.",
                "installation": [
                    "Install Docker Desktop or an equivalent container runtime.",
                    "Launch the stack with `scripts/run_local_stack.py` or integrate the container into your Compose file.",
                ],
                "configuration_notes": [
                    "Point the UI at the exported TOML configuration or environment variables from previous sections.",
                    "Reuse the same workspace volume as the governance service so edits persist.",
                ],
                "fields": [],
            },
            "skip_hosting": {
                "label": "No dedicated hosting",
                "description": "Document that the UI will not be deployed – operators rely on APIs or another portal.",
                "installation": [
                    "No services are provisioned by this wizard selection.",
                ],
                "configuration_notes": [
                    "Ensure governance APIs are reachable for automation even without the bundled UI.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
            "aws_terraform": {
                "label": "AWS (Terraform scaffold)",
                "description": "Capture AWS hosting details so you can feed them into your infrastructure-as-code module for the UI.",
                "installation": [
                    "Clone or adapt your existing Terraform stacks for ECS/ALB frontends.",
                    "Copy the generated `terraform.tfvars` stub into that repository to keep values in sync with the wizard.",
                ],
                "configuration_notes": [
                    "Provide the container image that serves the UI (for example the monolith image with `DC43_UI_MODE=local`).",
                    "Ensure the security groups and subnets align with those used by the governance service when colocated.",
                ],
                "fields": [
                    {
                        "name": "aws_region",
                        "label": "AWS region",
                        "placeholder": "us-east-1",
                        "help": "Region hosting the UI frontend.",
                    },
                    {
                        "name": "cluster_name",
                        "label": "ECS cluster name",
                        "placeholder": "dc43-ui",
                        "help": "Existing ECS/Fargate cluster that will run the UI tasks.",
                    },
                    {
                        "name": "service_name",
                        "label": "ECS service name",
                        "placeholder": "dc43-contracts-ui",
                        "help": "Name of the ECS service that will expose the UI.",
                        "optional": True,
                    },
                    {
                        "name": "ecr_image_uri",
                        "label": "ECR image URI",
                        "placeholder": "123456789012.dkr.ecr.us-east-1.amazonaws.com/dc43-contracts:latest",
                        "help": "Container image containing the contracts UI application.",
                    },
                    {
                        "name": "private_subnet_ids",
                        "label": "Private subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs used by the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_subnet_ids",
                        "label": "Load balancer subnet IDs",
                        "placeholder": "subnet-1a2b3c4d, subnet-4d3c2b1a",
                        "help": "Comma separated list of subnet IDs for the public load balancer.",
                    },
                    {
                        "name": "service_security_group_id",
                        "label": "Service security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the ECS tasks.",
                    },
                    {
                        "name": "load_balancer_security_group_id",
                        "label": "Load balancer security group",
                        "placeholder": "sg-0123456789abcdef0",
                        "help": "Security group attached to the Application Load Balancer.",
                    },
                    {
                        "name": "certificate_arn",
                        "label": "ACM certificate ARN",
                        "placeholder": "arn:aws:acm:...",
                        "help": "Certificate used for HTTPS ingress.",
                    },
                    {
                        "name": "vpc_id",
                        "label": "VPC ID",
                        "placeholder": "vpc-0123456789abcdef0",
                        "help": "Virtual private cloud hosting the deployment.",
                    },
                    {
                        "name": "container_port",
                        "label": "Container port",
                        "placeholder": "8000",
                        "help": "Port exposed by the UI container.",
                        "optional": True,
                    },
                    {
                        "name": "desired_count",
                        "label": "Desired task count",
                        "placeholder": "2",
                        "help": "Number of task replicas to run.",
                        "optional": True,
                    },
                    {
                        "name": "task_cpu",
                        "label": "Task CPU units",
                        "placeholder": "512",
                        "help": "Fargate CPU allocation for the UI task.",
                        "optional": True,
                    },
                    {
                        "name": "task_memory",
                        "label": "Task memory (MiB)",
                        "placeholder": "1024",
                        "help": "Fargate memory allocation for the UI task.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_path",
                        "label": "Health check path",
                        "placeholder": "/health",
                        "help": "HTTP path polled by the load balancer.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_interval",
                        "label": "Health check interval",
                        "placeholder": "30",
                        "help": "Seconds between health checks.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_timeout",
                        "label": "Health check timeout",
                        "placeholder": "5",
                        "help": "Seconds before a health check is considered failed.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_healthy_threshold",
                        "label": "Healthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive successes before the target is considered healthy.",
                        "optional": True,
                    },
                    {
                        "name": "health_check_unhealthy_threshold",
                        "label": "Unhealthy threshold",
                        "placeholder": "2",
                        "help": "Number of consecutive failures before the target is considered unhealthy.",
                        "optional": True,
                    },
                    {
                        "name": "log_retention_days",
                        "label": "Log retention (days)",
                        "placeholder": "30",
                        "help": "CloudWatch log retention period.",
                        "optional": True,
                    },
                ],
            },
            "azure_terraform": {
                "label": "Azure (Terraform scaffold)",
                "description": "Collect Azure Container Apps parameters to drive your UI deployment templates.",
                "installation": [
                    "Integrate the generated variables into your Container Apps Terraform module or Bicep deployment.",
                    "Store sensitive values (like registry passwords) in your preferred secret manager before applying.",
                ],
                "configuration_notes": [
                    "Provide the registry location for the UI image and adjust autoscaling bounds as required.",
                    "Align the tags with your governance service deployment for easier inventory tracking.",
                ],
                "fields": [
                    {
                        "name": "subscription_id",
                        "label": "Subscription ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Azure subscription hosting the UI deployment.",
                    },
                    {
                        "name": "resource_group_name",
                        "label": "Resource group",
                        "placeholder": "rg-dc43-ui",
                        "help": "Resource group where the Container App will be created.",
                    },
                    {
                        "name": "location",
                        "label": "Location",
                        "placeholder": "westeurope",
                        "help": "Azure region for the resources.",
                    },
                    {
                        "name": "container_registry",
                        "label": "Container registry",
                        "placeholder": "dc43.azurecr.io",
                        "help": "Azure Container Registry host serving the UI image.",
                    },
                    {
                        "name": "container_registry_username",
                        "label": "Registry username",
                        "placeholder": "dc43",
                        "help": "Username with pull permissions on the registry.",
                    },
                    {
                        "name": "container_registry_password",
                        "label": "Registry password",
                        "placeholder": "••••••",
                        "help": "Password or access key for the container registry.",
                    },
                    {
                        "name": "image_tag",
                        "label": "Image tag",
                        "placeholder": "dc43-contracts:latest",
                        "help": "Full reference (repository:tag) for the UI container image.",
                    },
                    {
                        "name": "container_app_environment_name",
                        "label": "Container Apps environment",
                        "placeholder": "dc43-ui-env",
                        "help": "Name assigned to the Container Apps environment.",
                        "optional": True,
                    },
                    {
                        "name": "container_app_name",
                        "label": "Container App name",
                        "placeholder": "dc43-contracts-ui",
                        "help": "Name of the Container App resource created by Terraform.",
                        "optional": True,
                    },
                    {
                        "name": "ingress_port",
                        "label": "Ingress port",
                        "placeholder": "8000",
                        "help": "Port exposed publicly by the Container App.",
                        "optional": True,
                    },
                    {
                        "name": "min_replicas",
                        "label": "Minimum replicas",
                        "placeholder": "1",
                        "help": "Lower bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "max_replicas",
                        "label": "Maximum replicas",
                        "placeholder": "3",
                        "help": "Upper bound for Container Apps autoscaling.",
                        "optional": True,
                    },
                    {
                        "name": "container_cpu",
                        "label": "Container CPU",
                        "placeholder": "0.5",
                        "help": "vCPU allocation per replica (for example 0.5, 1.0).",
                        "optional": True,
                    },
                    {
                        "name": "container_memory",
                        "label": "Container memory",
                        "placeholder": "1.0Gi",
                        "help": "Memory allocation per replica (GiB).",
                        "optional": True,
                    },
                    {
                        "name": "tags",
                        "label": "Resource tags",
                        "placeholder": "env=dev, owner=data-platform",
                        "help": "Comma-separated key=value pairs applied to Azure resources.",
                        "optional": True,
                    },
                ],
            },
        },
    },
    "demo_automation": {
        "title": "Demo automation",
        "summary": "Optionally launch the sample stack so teams can explore dc43 quickly.",
        "default_option": "skip_demo",
        "options": {
            "skip_demo": {
                "label": "Do not launch the demo",
                "description": "Skip demo orchestration and proceed with production workflows only.",
                "installation": [
                    "No demo-specific scripts or datasets will be provisioned.",
                ],
                "configuration_notes": [
                    "You can always re-run the wizard and export demo helpers later if needed.",
                ],
                "fields": [],
                "skip_configuration": True,
            },
            "local_python": {
                "label": "Run demo locally (Python)",
                "description": "Execute the generated helper script to populate sample contracts and run orchestrated pipelines on your laptop.",
                "installation": [
                    "Install the repository in editable mode (`pip install -e .[demo]`) so demo modules are importable.",
                    "Execute `python scripts/run_demo.py` from the exported bundle to seed sample data and launch demo flows.",
                ],
                "configuration_notes": [
                    "Ensure the workspace directories selected in storage steps exist and are writable.",
                    "Review the generated README for credentials and follow-up teardown guidance.",
                ],
                "fields": [],
            },
            "local_docker": {
                "label": "Run demo locally (Docker)",
                "description": "Leverage Docker Compose via the helper script to start the demo UI, governance APIs, and datasets.",
                "installation": [
                    "Install Docker Desktop (or equivalent) with access to the repository workspace volume.",
                    "Run `./run_local_stack.py --with-demo` from the exported bundle to launch the demo stack.",
                ],
                "configuration_notes": [
                    "The helper reuses the TOML configuration emitted by the wizard for consistency.",
                    "Stop the containers before running infrastructure automation to avoid port conflicts.",
                ],
                "fields": [],
            },
        },
    },
    "authentication": {
        "title": "Authentication & access",
        "summary": "Configure how users authenticate with the UI and downstream services.",
        "options": {
            "none": {
                "label": "No authentication",
                "description": "Rely on network controls only – all endpoints remain unauthenticated.",
                "installation": [],
                "configuration_notes": [
                    "Use this only in isolated demo environments.",
                    "Consider enabling at least basic auth before exposing the UI broadly.",
                ],
                "fields": [],
            },
            "basic": {
                "label": "HTTP basic auth",
                "description": "Protect the UI with a simple username/password pair managed via environment variables.",
                "installation": [
                    "Generate credentials and inject them as secrets in your container orchestrator.",
                    "Enable HTTPS termination on your ingress/load balancer.",
                ],
                "configuration_notes": [
                    "Set `DC43_AUTH_MODE=basic` and provide the credentials below.",
                    "Rotate passwords frequently and avoid reusing them across environments.",
                ],
                "fields": [
                    {
                        "name": "username",
                        "label": "Username",
                        "placeholder": "governance-admin",
                        "help": "Login used to access the UI.",
                    },
                    {
                        "name": "password",
                        "label": "Password",
                        "placeholder": "••••••",
                        "help": "Strong password stored as a secret.",
                    },
                ],
            },
            "oauth_oidc": {
                "label": "OAuth / OIDC",
                "description": "Delegate authentication to your identity provider using OAuth 2.0 / OpenID Connect.",
                "installation": [
                    "Register the application with your IdP and generate client credentials.",
                    "Configure redirect URIs that match the portal hostname or localhost for development.",
                ],
                "configuration_notes": [
                    "Set `DC43_AUTH_MODE=oauth` and configure the client settings below.",
                    "Sync group or role mappings in the governance service to enforce authorisation policies.",
                ],
                "fields": [
                    {
                        "name": "issuer_url",
                        "label": "Issuer URL",
                        "placeholder": "https://login.microsoftonline.com/<tenant>/v2.0",
                        "help": "Discovery endpoint for the identity provider.",
                    },
                    {
                        "name": "client_id",
                        "label": "Client ID",
                        "placeholder": "00000000-0000-0000-0000-000000000000",
                        "help": "Application (client) identifier registered with the IdP.",
                    },
                    {
                        "name": "client_secret",
                        "label": "Client secret",
                        "placeholder": "••••••",
                        "help": "Client secret or certificate thumbprint used for token exchange.",
                    },
                    {
                        "name": "redirect_uri",
                        "label": "Redirect URI",
                        "placeholder": "https://contracts.acme.com/oauth/callback",
                        "help": "URL the IdP will redirect to after authentication.",
                    },
                ],
            },
        },
    },
}


SETUP_MODULE_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "storage_foundations",
        "title": "Storage foundations",
        "summary": "Decide how contracts, product metadata, and validation results are persisted before the orchestration layer is wired in.",
        "modules": ["contracts_backend", "products_backend", "governance_store"],
    },
    {
        "key": "pipeline_runtime",
        "title": "Pipeline runtime",
        "summary": "Wire the integration layer, orchestration services, and quality hooks that local pipelines rely on.",
        "modules": ["pipeline_integration", "governance_service", "data_quality", "governance_extensions"],
    },
    {
        "key": "service_hosting",
        "title": "Service hosting",
        "summary": "Capture how the governance services are deployed so automation scripts can be generated per environment.",
        "modules": ["governance_deployment"],
    },
    {
        "key": "user_experience",
        "title": "User experience",
        "summary": "Choose how operators reach the contracts UI and how that interface is hosted or automated.",
        "modules": ["user_interface", "docs_assistant", "ui_deployment"],
    },
    {
        "key": "access_security",
        "title": "Access & security",
        "summary": "Specify how users authenticate with the UI and downstream services.",
        "modules": ["authentication"],
    },
    {
        "key": "accelerators",
        "title": "Accelerators",
        "summary": "Optionally launch the bundled demo so teams can explore the platform end-to-end.",
        "modules": ["demo_automation"],
    },
]


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


def _module_dependency_value(module_key: str, selected: Mapping[str, str]) -> str | None:
    """Return the dependency value for ``module_key`` if configured."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return None
    depends_on = module_meta.get("depends_on")
    if not depends_on:
        return None
    value = selected.get(depends_on)
    return str(value) if value is not None else None


def _module_visible_options(module_key: str, selected: Mapping[str, str]) -> List[str]:
    """Return option keys that should be shown for ``module_key``."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return []
    options = module_meta.get("options", {})
    depends_on = module_meta.get("depends_on")
    visible_map = module_meta.get("visible_when") or {}
    if not depends_on or not visible_map:
        return [str(key) for key in options.keys()]
    dependency_value = _module_dependency_value(module_key, selected)
    if dependency_value is None:
        allowed = visible_map.get("__default__", [])
    else:
        allowed = visible_map.get(dependency_value)
        if allowed is None:
            allowed = visible_map.get("__default__", [])
    if not isinstance(allowed, Iterable):
        return []
    return [key for key in allowed if key in options]


def _module_should_hide(module_key: str, selected: Mapping[str, str]) -> bool:
    """Return ``True`` when a module should be hidden based on dependencies."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return False
    hide_when = module_meta.get("hide_when") or []
    if not hide_when:
        return False
    dependency_value = _module_dependency_value(module_key, selected)
    return dependency_value in hide_when


def _module_default_option(module_key: str, selected: Mapping[str, str]) -> str | None:
    """Return the default option for ``module_key`` given current selections."""

    module_meta = SETUP_MODULES.get(module_key)
    if not module_meta:
        return None
    default_map = module_meta.get("default_for") or {}
    dependency_value = _module_dependency_value(module_key, selected)
    if dependency_value is not None and dependency_value in default_map:
        return str(default_map[dependency_value])
    default_option = module_meta.get("default_option")
    if isinstance(default_option, str) and default_option in module_meta.get("options", {}):
        return default_option
    visible_options = _module_visible_options(module_key, selected)
    if len(visible_options) == 1:
        return visible_options[0]
    return None


def _serialise_field(
    module_key: str,
    field_meta: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any],
    workspace: Mapping[str, str] | None,
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
                    value = str(default_factory())
                except TypeError:
                    if workspace is not None:
                        try:
                            value = str(default_factory(workspace))
                        except Exception:  # pragma: no cover - defensive defaults
                            value = ""
                    else:
                        value = ""
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


def _serialise_diagram(diagram_meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a JSON-safe payload describing diagram nodes/edges."""

    nodes: List[Dict[str, Any]] = []
    for node_meta in diagram_meta.get("nodes", []):
        if not isinstance(node_meta, Mapping):
            continue
        node_id = str(node_meta.get("id") or "").strip()
        if not node_id:
            continue
        node_payload: Dict[str, Any] = {
            "id": node_id,
            "label": node_meta.get("label") or node_id,
        }
        class_name = (
            node_meta.get("class")
            or node_meta.get("class_name")
            or node_meta.get("className")
        )
        if class_name:
            node_payload["className"] = str(class_name)

        edges: List[Dict[str, Any]] = []
        for edge_meta in node_meta.get("edges", []):
            if not isinstance(edge_meta, Mapping):
                continue
            edge_from = str(edge_meta.get("from") or "").strip()
            edge_to = str(edge_meta.get("to") or "").strip()
            if not edge_from or not edge_to:
                continue
            edge_payload: Dict[str, Any] = {
                "from": edge_from,
                "to": edge_to,
            }
            if edge_meta.get("label"):
                edge_payload["label"] = str(edge_meta.get("label"))
            edges.append(edge_payload)

        if edges:
            node_payload["edges"] = edges

        nodes.append(node_payload)

    payload: Dict[str, Any] = {}
    if nodes:
        payload["nodes"] = nodes
    return payload


def _setup_export_payload(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a serialisable payload that captures the wizard selections."""

    selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_options, Mapping):
        selected_options = {}
    if not isinstance(configuration, Mapping):
        configuration = {}

    modules: List[Dict[str, Any]] = []
    for module_key, option_key in selected_options.items():
        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue
        module_config = configuration.get(module_key, {})
        if not isinstance(module_config, Mapping):
            module_config = {}

        settings: Dict[str, Any] = {}
        for field_meta in option_meta.get("fields", []):
            field_name = str(field_meta.get("name") or "")
            if not field_name:
                continue
            raw_value = module_config.get(field_name)
            if isinstance(raw_value, str):
                value = raw_value.strip()
            else:
                value = raw_value
            if value in {None, ""}:
                continue
            settings[field_name] = value

        modules.append(
            {
                "key": module_key,
                "title": module_meta.get("title"),
                "summary": module_meta.get("summary"),
                "option": option_key,
                "option_label": option_meta.get("label"),
                "installation": list(option_meta.get("installation", [])),
                "configuration_notes": list(option_meta.get("configuration_notes", [])),
                "settings": settings,
            }
        )

    configuration_payload: Dict[str, Dict[str, Any]] = {}
    for module_key, module_values in configuration.items():
        if isinstance(module_values, Mapping):
            configuration_payload[module_key] = {
                key: value
                for key, value in module_values.items()
                if not isinstance(value, Mapping)
            }

    payload: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "completed": bool(state.get("completed")),
        "modules": modules,
        "selected_options": dict(selected_options),
        "configuration": configuration_payload,
    }
    completed_at = state.get("completed_at") if isinstance(state, Mapping) else None
    if isinstance(completed_at, str) and completed_at:
        payload["completed_at"] = completed_at
    return payload


def _clean_str(value: Any) -> str | None:
    """Return ``value`` as a trimmed string when non-empty."""

    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
    return None


def _toml_string(value: str) -> str:
    """Serialise ``value`` using TOML string quoting rules."""

    return json.dumps(value)


def _clean_number(value: Any) -> int | float | None:
    """Return ``value`` as a number when possible."""

    if isinstance(value, (int, float)):
        return value
    text = _clean_str(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return None


def _split_csv(value: Any) -> List[str]:
    """Return a list parsed from comma/newline separated ``value``."""

    text = _clean_str(value)
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[,\n]+", text) if part.strip()]
    return parts


def _parse_key_value_pairs(value: Any) -> Dict[str, str]:
    """Return a map parsed from ``key=value`` pairs separated by commas/newlines."""

    text = _clean_str(value)
    if not text:
        return {}
    pairs: Dict[str, str] = {}
    for part in re.split(r"[,\n]+", text):
        if not part.strip():
            continue
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        key = key.strip()
        if not key:
            continue
        pairs[key] = raw_value.strip()
    return pairs


def _terraform_identifier(key: str) -> str:
    """Return a Terraform-compatible identifier for ``key``."""

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        return key
    return json.dumps(key)


def _terraform_literal(value: Any) -> str:
    """Return ``value`` formatted as a Terraform literal."""

    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, Mapping):
        items = []
        for key, item_value in value.items():
            ident = _terraform_identifier(str(key))
            items.append(f"  {ident} = {_terraform_literal(item_value)}")
        if not items:
            return "{}"
        return "{\n" + "\n".join(items) + "\n}"
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        items = [f"  {_terraform_literal(item)}," for item in value]
        if not items:
            return "[]"
        return "[\n" + "\n".join(items) + "\n]"
    return json.dumps(str(value))


def _render_terraform_tfvars(
    entries: List[Tuple[str, Any]],
    missing: Iterable[str],
) -> str:
    """Render ``entries`` as a Terraform ``.tfvars`` file."""

    lines = [
        "# terraform.tfvars generated by the dc43 setup wizard",
        "# Update any TODO values before running `terraform apply`.",
        "",
    ]
    missing_list = [item for item in missing if item]
    if missing_list:
        lines.append("# The following values are still required:")
        for item in missing_list:
            lines.append(f"# - {item}")
        lines.append("")
    for key, value in entries:
        lines.append(f"{key} = {_terraform_literal(value)}")
    lines.append("")
    return "\n".join(lines)


def _collect_tfvars_entries(
    data: Mapping[str, Any],
    *,
    field_map: Mapping[str, str],
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    list_fields: Iterable[str] = (),
    numeric_fields: Iterable[str] = (),
    map_fields: Iterable[str] = (),
    list_parser: Callable[[Any], Iterable[Any]] | None = None,
    map_parser: Callable[[Any], Mapping[str, Any]] | None = None,
) -> Tuple[List[Tuple[str, Any]], List[str]]:
    """Return Terraform variable entries and missing labels for ``field_map``."""

    entries: List[Tuple[str, Any]] = []
    missing_labels: List[str] = []
    required_set = {str(name) for name in required_fields}
    list_fields = {str(name) for name in list_fields}
    numeric_fields = {str(name) for name in numeric_fields}
    map_fields = {str(name) for name in map_fields}

    for field_name, var_name in field_map.items():
        field_name = str(field_name)
        if field_name in map_fields:
            parser = map_parser or (lambda value: _parse_key_value_pairs(value))
            mapping = parser(data.get(field_name))
            if mapping:
                entries.append((var_name, mapping))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        elif field_name in list_fields:
            parser = list_parser or (lambda value: _split_csv(value))
            values = [item for item in parser(data.get(field_name)) if item]
            if values:
                entries.append((var_name, values))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        elif field_name in numeric_fields:
            number = _clean_number(data.get(field_name))
            if number is not None:
                entries.append((var_name, number))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))
        else:
            text = _clean_str(data.get(field_name))
            if text:
                entries.append((var_name, text))
            elif field_name in required_set:
                missing_labels.append(field_labels.get(field_name, field_name))

    return entries, missing_labels


def _terraform_template_files(
    provider: str,
    *,
    target: str,
    template_dir: Path | None = None,
) -> List[Tuple[str, str]]:
    """Return template files bundled with the repository for ``provider`` and ``target``."""

    files: List[Tuple[str, str]] = []
    base = template_dir or (TERRAFORM_TEMPLATE_ROOT / f"{provider}-service-backend")
    for filename in ("main.tf", "variables.tf", "README.md"):
        path = base / filename
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except OSError:
            continue
        files.append((f"dc43-setup/terraform/{target}/{provider}/{filename}", content))
    return files


def _aws_governance_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered AWS ``terraform.tfvars`` content and missing labels for governance deployments."""

    data = {str(key): value for key, value in module_config.items()}
    contract_backend = selected.get("contracts_backend", "").strip().lower()
    if not _clean_str(data.get("contract_store_mode")):
        data["contract_store_mode"] = "sql" if contract_backend == "sql" else "filesystem"

    field_map = {
        "aws_region": "aws_region",
        "cluster_name": "cluster_name",
        "ecr_image_uri": "ecr_image_uri",
        "backend_token": "backend_token",
        "contract_store_mode": "contract_store_mode",
        "contract_filesystem": "contract_filesystem",
        "contract_storage_path": "contract_storage_path",
        "contract_store_dsn": "contract_store_dsn",
        "contract_store_dsn_secret_arn": "contract_store_dsn_secret_arn",
        "contract_store_table": "contract_store_table",
        "contract_store_schema": "contract_store_schema",
        "private_subnet_ids": "private_subnet_ids",
        "load_balancer_subnet_ids": "load_balancer_subnet_ids",
        "service_security_group_id": "service_security_group_id",
        "load_balancer_security_group_id": "load_balancer_security_group_id",
        "certificate_arn": "certificate_arn",
        "vpc_id": "vpc_id",
        "desired_count": "desired_count",
        "task_cpu": "task_cpu",
        "task_memory": "task_memory",
        "health_check_path": "health_check_path",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        list_fields={"private_subnet_ids", "load_balancer_subnet_ids"},
        numeric_fields={"desired_count"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels

def _aws_ui_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered AWS ``terraform.tfvars`` content for UI deployments."""

    data = {str(key): value for key, value in module_config.items()}
    field_map = {
        "aws_region": "aws_region",
        "cluster_name": "cluster_name",
        "service_name": "service_name",
        "ecr_image_uri": "ecr_image_uri",
        "private_subnet_ids": "private_subnet_ids",
        "load_balancer_subnet_ids": "load_balancer_subnet_ids",
        "service_security_group_id": "service_security_group_id",
        "load_balancer_security_group_id": "load_balancer_security_group_id",
        "certificate_arn": "certificate_arn",
        "vpc_id": "vpc_id",
        "container_port": "container_port",
        "desired_count": "desired_count",
        "task_cpu": "task_cpu",
        "task_memory": "task_memory",
        "health_check_path": "health_check_path",
        "health_check_interval": "health_check_interval",
        "health_check_timeout": "health_check_timeout",
        "health_check_healthy_threshold": "health_check_healthy_threshold",
        "health_check_unhealthy_threshold": "health_check_unhealthy_threshold",
        "log_retention_days": "log_retention_days",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        list_fields={"private_subnet_ids", "load_balancer_subnet_ids"},
        numeric_fields={
            "container_port",
            "desired_count",
            "health_check_interval",
            "health_check_timeout",
            "health_check_healthy_threshold",
            "health_check_unhealthy_threshold",
            "log_retention_days",
        },
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _azure_ui_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered Azure ``terraform.tfvars`` content for UI deployments."""

    data = {str(key): value for key, value in module_config.items()}
    field_map = {
        "subscription_id": "subscription_id",
        "resource_group_name": "resource_group_name",
        "location": "location",
        "container_registry": "container_registry",
        "container_registry_username": "container_registry_username",
        "container_registry_password": "container_registry_password",
        "image_tag": "image_tag",
        "container_app_environment_name": "container_app_environment_name",
        "container_app_name": "container_app_name",
        "ingress_port": "ingress_port",
        "min_replicas": "min_replicas",
        "max_replicas": "max_replicas",
        "container_cpu": "container_cpu",
        "container_memory": "container_memory",
        "tags": "tags",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        numeric_fields={"ingress_port", "min_replicas", "max_replicas"},
        map_fields={"tags"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _azure_governance_tfvars(
    module_config: Mapping[str, Any],
    *,
    field_labels: Mapping[str, str],
    required_fields: Iterable[str],
    selected: Mapping[str, str],
) -> Tuple[str | None, List[str]]:
    """Return rendered Azure ``terraform.tfvars`` content and missing labels for governance deployments."""

    data = {str(key): value for key, value in module_config.items()}
    contract_backend = selected.get("contracts_backend", "").strip().lower()
    if not _clean_str(data.get("contract_store_mode")):
        data["contract_store_mode"] = "sql" if contract_backend == "sql" else "filesystem"

    field_map = {
        "subscription_id": "subscription_id",
        "resource_group_name": "resource_group_name",
        "location": "location",
        "container_registry": "container_registry",
        "container_registry_username": "container_registry_username",
        "container_registry_password": "container_registry_password",
        "image_tag": "image_tag",
        "backend_token": "backend_token",
        "contract_store_mode": "contract_store_mode",
        "contract_storage": "contract_storage",
        "contract_share_name": "contract_share_name",
        "contract_share_quota_gb": "contract_share_quota_gb",
        "contract_store_dsn": "contract_store_dsn",
        "contract_store_table": "contract_store_table",
        "contract_store_schema": "contract_store_schema",
        "container_app_environment_name": "container_app_environment_name",
        "container_app_name": "container_app_name",
        "ingress_port": "ingress_port",
        "min_replicas": "min_replicas",
        "max_replicas": "max_replicas",
        "container_cpu": "container_cpu",
        "container_memory": "container_memory",
        "tags": "tags",
    }
    entries, missing_labels = _collect_tfvars_entries(
        data,
        field_map=field_map,
        field_labels=field_labels,
        required_fields=required_fields,
        numeric_fields={"ingress_port", "min_replicas", "max_replicas", "contract_share_quota_gb"},
        map_fields={"tags"},
    )
    if not entries and not missing_labels:
        return None, []
    return _render_terraform_tfvars(entries, missing_labels), missing_labels


def _terraform_bundle_files(state: Mapping[str, Any]) -> List[Tuple[str, str]]:
    """Return additional Terraform files for the setup bundle."""

    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_raw, Mapping) or not isinstance(configuration_raw, Mapping):
        return []

    selected = {str(key): str(value) for key, value in selected_raw.items()}
    configuration: Dict[str, Mapping[str, Any]] = {
        str(key): value
        for key, value in configuration_raw.items()
        if isinstance(value, Mapping)
    }

    bundle_plan = {
        "governance_deployment": {
            "slug": "governance",
            "options": {
                "aws_terraform": {
                    "provider": "aws",
                    "builder": _aws_governance_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "aws-service-backend",
                },
                "azure_terraform": {
                    "provider": "azure",
                    "builder": _azure_governance_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "azure-service-backend",
                },
            },
        },
        "ui_deployment": {
            "slug": "ui",
            "options": {
                "aws_terraform": {
                    "provider": "aws",
                    "builder": _aws_ui_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "aws-contracts-app",
                },
                "azure_terraform": {
                    "provider": "azure",
                    "builder": _azure_ui_tfvars,
                    "template_dir": TERRAFORM_TEMPLATE_ROOT / "azure-contracts-app",
                },
            },
        },
    }

    files: List[Tuple[str, str]] = []

    for module_key, module_plan in bundle_plan.items():
        option_key = selected.get(module_key)
        if not option_key:
            continue
        option_plan = module_plan["options"].get(option_key)
        if not option_plan:
            continue

        module_meta = SETUP_MODULES.get(module_key)
        if not module_meta:
            continue
        option_meta = module_meta["options"].get(option_key)
        if not option_meta:
            continue

        field_labels: Dict[str, str] = {}
        required_fields: List[str] = []
        for field_meta in option_meta.get("fields", []):
            name = str(field_meta.get("name") or "")
            if not name:
                continue
            field_labels[name] = str(field_meta.get("label") or name)
            if not field_meta.get("optional"):
                required_fields.append(name)

        module_config = configuration.get(module_key, {})
        if not isinstance(module_config, Mapping):
            module_config = {}

        builder = option_plan.get("builder")
        provider = option_plan.get("provider")
        template_dir = option_plan.get("template_dir")
        if not provider or not builder:
            continue

        tfvars_text, missing = builder(
            module_config,
            field_labels=field_labels,
            required_fields=required_fields,
            selected=selected,
        )

        template_files = _terraform_template_files(
            provider,
            target=module_plan.get("slug", module_key),
            template_dir=template_dir if isinstance(template_dir, Path) else None,
        )
        files.extend(template_files)

        prefix = f"dc43-setup/terraform/{module_plan.get('slug', module_key)}/{provider}"
        if tfvars_text:
            files.append((f"{prefix}/terraform.tfvars", tfvars_text))
        elif missing:
            stub = _render_terraform_tfvars([], missing)
            files.append((f"{prefix}/terraform.tfvars", stub))

    return files


def _service_backends_config_from_state(
    state: Mapping[str, Any],
) -> ServiceBackendsConfig | None:
    """Return a :class:`ServiceBackendsConfig` built from the wizard state."""

    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(selected_raw, Mapping):
        return None

    selected = {str(key): str(value) for key, value in selected_raw.items()}
    configuration: Dict[str, Mapping[str, Any]] = {}
    if isinstance(configuration_raw, Mapping):
        configuration = {
            str(key): value
            for key, value in configuration_raw.items()
            if isinstance(value, Mapping)
        }

    if not selected:
        return None

    def module_config(name: str) -> Mapping[str, Any]:
        module = configuration.get(name, {})
        return module if isinstance(module, Mapping) else {}

    def path_from(value: Any) -> Path | None:
        text = _clean_str(value)
        if not text:
            return None
        try:
            return Path(text)
        except (TypeError, ValueError):  # pragma: no cover - safety
            return None

    def parse_static_properties(value: Any) -> dict[str, str]:
        properties: dict[str, str] = {}
        if isinstance(value, Mapping):
            for key, raw in value.items():
                name = _clean_str(key)
                if not name:
                    continue
                properties[name] = _clean_str(raw) or ""
            return properties

        text = _clean_str(value)
        if not text:
            return properties

        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, Mapping):
            for key, raw in parsed.items():
                name = _clean_str(key)
                if not name:
                    continue
                properties[name] = _clean_str(raw) or ""
            if properties:
                return properties

        for token in re.split(r"[\n,]", text):
            candidate = token.strip()
            if not candidate:
                continue
            separator: str | None = None
            for sep in ("=", ":"):
                if sep in candidate:
                    separator = sep
                    break
            if not separator:
                continue
            key_part, value_part = candidate.split(separator, 1)
            name = key_part.strip()
            if not name:
                continue
            properties[name] = value_part.strip()

        return properties

    contract_option = selected.get("contracts_backend")
    databricks_hosts: list[str] = []
    databricks_profiles: list[str] = []
    databricks_tokens: list[str] = []
    contract_cfg = BackendContractStoreConfig()
    if contract_option:
        option = contract_option.strip().lower()
        module = module_config("contracts_backend")
        if option == "filesystem":
            contract_cfg.type = "filesystem"
            root = (
                module.get("contracts_dir")
                or module.get("work_dir")
                or module.get("storage_path")
            )
            contract_cfg.root = path_from(root)
        elif option == "sql":
            contract_cfg.type = "sql"
            contract_cfg.dsn = _clean_str(module.get("connection_uri"))
            contract_cfg.schema = _clean_str(module.get("schema"))
        elif option == "delta_lake":
            contract_cfg.type = "delta"
            contract_cfg.base_path = path_from(module.get("storage_path"))
            contract_cfg.schema = _clean_str(module.get("schema"))
            table_name = _clean_str(module.get("table_name"))
            if table_name:
                contract_cfg.table = table_name
            host_value = _clean_str(module.get("workspace_url"))
            if host_value:
                databricks_hosts.append(host_value)
            profile_value = _clean_str(module.get("workspace_profile"))
            if profile_value:
                databricks_profiles.append(profile_value)
            token_value = _clean_str(module.get("workspace_token"))
            if token_value:
                databricks_tokens.append(token_value)
        elif option == "collibra":
            contract_cfg.type = "collibra_http"
            contract_cfg.base_url = _clean_str(module.get("base_url"))

    product_option = selected.get("products_backend")
    product_cfg = BackendDataProductStoreConfig()
    if product_option:
        option = product_option.strip().lower()
        module = module_config("products_backend")
        if option == "filesystem":
            product_cfg.type = "filesystem"
            root = module.get("products_dir") or module.get("storage_path")
            product_cfg.root = path_from(root)
        elif option == "sql":
            product_cfg.type = "sql"
            product_cfg.dsn = _clean_str(module.get("connection_uri"))
            product_cfg.schema = _clean_str(module.get("schema"))
        elif option == "delta_lake":
            product_cfg.type = "delta"
            product_cfg.base_path = path_from(module.get("storage_path"))
            product_cfg.schema = _clean_str(module.get("schema"))
            product_cfg.catalog = _clean_str(module.get("catalog"))
            table_name = _clean_str(module.get("table_name"))
            if table_name:
                product_cfg.table = table_name
            host_value = _clean_str(module.get("workspace_url"))
            if host_value:
                databricks_hosts.append(host_value)
            profile_value = _clean_str(module.get("workspace_profile"))
            if profile_value:
                databricks_profiles.append(profile_value)
            token_value = _clean_str(module.get("workspace_token"))
            if token_value:
                databricks_tokens.append(token_value)
        elif option == "collibra":
            product_cfg.type = "collibra_stub"
            product_cfg.base_url = _clean_str(module.get("base_url"))

    dq_option = selected.get("data_quality")
    dq_cfg = BackendDataQualityConfig()
    if dq_option:
        option = dq_option.strip().lower()
        module = module_config("data_quality")
        if option == "embedded_engine":
            dq_cfg.type = "local"
            default_engine = _clean_str(module.get("default_engine"))
            if default_engine:
                dq_cfg.default_engine = default_engine
        elif option == "remote_http":
            dq_cfg.type = "http"
            dq_cfg.base_url = _clean_str(module.get("base_url"))
            dq_cfg.token = _clean_str(module.get("api_token"))
            header_value = _clean_str(module.get("token_header"))
            if header_value:
                dq_cfg.token_header = header_value
            scheme_value = _clean_str(module.get("token_scheme"))
            if scheme_value:
                dq_cfg.token_scheme = scheme_value
            default_engine = _clean_str(module.get("default_engine"))
            if default_engine:
                dq_cfg.default_engine = default_engine
            headers_value = _parse_key_value_pairs(module.get("extra_headers"))
            if headers_value:
                dq_cfg.headers = headers_value

    store_option = selected.get("governance_store")
    governance_store_cfg = BackendGovernanceStoreConfig()
    if store_option:
        option = store_option.strip().lower()
        module = module_config("governance_store")
        if option in {"embedded_memory", "memory", "local"}:
            governance_store_cfg.type = "memory"
        elif option == "filesystem":
            governance_store_cfg.type = "filesystem"
            governance_store_cfg.root = path_from(
                module.get("storage_path") or module.get("base_path")
            )
        elif option == "sql":
            governance_store_cfg.type = "sql"
            governance_store_cfg.dsn = _clean_str(module.get("connection_uri"))
            governance_store_cfg.schema = _clean_str(module.get("schema"))
            governance_store_cfg.status_table = _clean_str(module.get("status_table"))
            governance_store_cfg.activity_table = _clean_str(
                module.get("activity_table")
            )
            governance_store_cfg.link_table = _clean_str(module.get("link_table"))
        elif option == "delta_lake":
            governance_store_cfg.type = "delta"
            governance_store_cfg.base_path = path_from(module.get("storage_path"))
            governance_store_cfg.status_table = _clean_str(module.get("status_table"))
            governance_store_cfg.activity_table = _clean_str(
                module.get("activity_table")
            )
            governance_store_cfg.link_table = _clean_str(module.get("link_table"))
            host_value = _clean_str(module.get("workspace_url"))
            if host_value:
                databricks_hosts.append(host_value)
            profile_value = _clean_str(module.get("workspace_profile"))
            if profile_value:
                databricks_profiles.append(profile_value)
            token_value = _clean_str(module.get("workspace_token"))
            if token_value:
                databricks_tokens.append(token_value)
        elif option == "remote_http":
            governance_store_cfg.type = "http"
            governance_store_cfg.base_url = _clean_str(module.get("base_url"))
            governance_store_cfg.token = _clean_str(module.get("api_token"))
            header_value = _clean_str(module.get("token_header"))
            if header_value:
                governance_store_cfg.token_header = header_value
            scheme_value = _clean_str(module.get("token_scheme"))
            if scheme_value:
                governance_store_cfg.token_scheme = scheme_value
            timeout_value = _clean_number(module.get("timeout"))
            if timeout_value is not None:
                try:
                    governance_store_cfg.timeout = float(timeout_value)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    pass
            headers_value = _parse_key_value_pairs(module.get("extra_headers"))
            if headers_value:
                governance_store_cfg.headers = headers_value

    governance_option = selected.get("governance_extensions")
    unity_cfg = BackendUnityCatalogConfig()
    governance_builders: tuple[str, ...] = ()
    if governance_option == "unity_catalog":
        module = module_config("governance_extensions")
        unity_cfg.enabled = True
        prefix_value = _clean_str(module.get("dataset_prefix"))
        if prefix_value:
            unity_cfg.dataset_prefix = prefix_value
        unity_cfg.workspace_profile = _clean_str(module.get("workspace_profile"))
        unity_cfg.workspace_url = _clean_str(module.get("workspace_url"))
        unity_cfg.workspace_token = _clean_str(module.get("token"))
        catalog_value = _clean_str(module.get("catalog"))
        schema_value = _clean_str(module.get("schema"))
        notes = parse_static_properties(module.get("static_properties"))
        if catalog_value and "catalog" not in notes:
            notes["catalog"] = catalog_value
        if schema_value and "schema" not in notes:
            notes["schema"] = schema_value
        if notes:
            unity_cfg.static_properties = notes
    elif governance_option == "custom_module":
        module = module_config("governance_extensions")
        module_path = _clean_str(module.get("module_path"))
        if module_path:
            governance_builders = (module_path,)

    if not unity_cfg.workspace_url:
        for host in databricks_hosts:
            if host:
                unity_cfg.workspace_url = host
                break
    if not unity_cfg.workspace_profile:
        for profile in databricks_profiles:
            if profile:
                unity_cfg.workspace_profile = profile
                break
    if not unity_cfg.workspace_token:
        for token in databricks_tokens:
            if token:
                unity_cfg.workspace_token = token
                break

    auth_cfg = BackendAuthConfig()
    if selected.get("governance_service") == "remote_api":
        module = module_config("governance_service")
        auth_cfg.token = _clean_str(module.get("api_token"))

    config = ServiceBackendsConfig(
        contract_store=contract_cfg,
        data_product_store=product_cfg,
        data_quality=dq_cfg,
        auth=auth_cfg,
        unity_catalog=unity_cfg,
        governance=BackendGovernanceConfig(
            dataset_contract_link_builders=governance_builders,
        ),
        governance_store=governance_store_cfg,
    )

    return config


def _service_backends_toml(state: Mapping[str, Any]) -> str | None:
    """Render the dc43-service-backends TOML using the wizard selections."""

    config = _service_backends_config_from_state(state)
    if config is None:
        return None
    toml_text = dump_service_backends_config(config)
    return toml_text or None


def _contracts_app_config_from_state(
    state: Mapping[str, Any],
) -> ContractsAppConfig | None:
    """Return a :class:`ContractsAppConfig` derived from wizard selections."""

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}

    if not isinstance(configuration_raw, Mapping):
        configuration_raw = {}
    if not isinstance(selected_raw, Mapping):
        selected_raw = {}

    configuration: Dict[str, Mapping[str, Any]] = {
        str(key): value
        for key, value in configuration_raw.items()
        if isinstance(value, Mapping)
    }
    selected = {str(key): str(value) for key, value in selected_raw.items()}

    workspace_module = configuration.get("contracts_backend", {})
    work_dir = _clean_str(workspace_module.get("work_dir"))
    workspace_cfg = WorkspaceConfig(root=Path(work_dir) if work_dir else None)

    backend_mode = "embedded"
    backend_base_url: str | None = None
    governance_config = configuration.get("governance_service", {})
    if selected.get("governance_service") == "remote_api":
        backend_mode = "remote"
        backend_base_url = _clean_str(governance_config.get("base_url"))

    backend_cfg = BackendConfig(
        mode="remote" if backend_mode == "remote" else "embedded",
        base_url=backend_base_url,
        process=BackendProcessConfig(),
    )

    docs_option = selected.get("docs_assistant")
    docs_chat_cfg = DocsChatConfig()
    if docs_option == "openai_embedded":
        docs_module = configuration.get("docs_assistant", {})
        if not isinstance(docs_module, Mapping):
            docs_module = {}

        provider = _clean_str(docs_module.get("provider")) or "openai"
        model = _clean_str(docs_module.get("model")) or "gpt-4o-mini"
        embedding_provider = _clean_str(docs_module.get("embedding_provider")) or "openai"
        embedding_model = _clean_str(docs_module.get("embedding_model")) or "text-embedding-3-small"
        api_key_env = _clean_str(docs_module.get("api_key_env")) or "OPENAI_API_KEY"
        api_key_value = _clean_str(docs_module.get("api_key"))

        docs_path_text = _clean_str(docs_module.get("docs_path"))
        index_path_text = _clean_str(docs_module.get("index_path"))

        docs_path = Path(docs_path_text).expanduser() if docs_path_text else None
        index_path = Path(index_path_text).expanduser() if index_path_text else None

        code_paths_value = docs_module.get("code_paths")
        code_path_entries: list[str] = []
        if isinstance(code_paths_value, (list, tuple, set)):
            for item in code_paths_value:
                if not isinstance(item, str):
                    item = str(item)
                item = item.strip()
                if item:
                    code_path_entries.append(item)
        elif isinstance(code_paths_value, str):
            candidate = code_paths_value.strip()
            if candidate:
                parts = [part.strip() for part in candidate.replace(";", ",").split(",")]
                code_path_entries.extend(part for part in parts if part)

        code_paths = tuple(Path(entry).expanduser() for entry in code_path_entries)
        reasoning_effort = _clean_str(docs_module.get("reasoning_effort")) or None

        docs_chat_cfg = DocsChatConfig(
            enabled=True,
            provider=provider,
            model=model,
            embedding_provider=embedding_provider,
            embedding_model=embedding_model,
            api_key_env=api_key_env,
            api_key=api_key_value,
            docs_path=docs_path,
            index_path=index_path,
            code_paths=code_paths,
            reasoning_effort=reasoning_effort,
        )

    return ContractsAppConfig(
        workspace=workspace_cfg,
        backend=backend_cfg,
        docs_chat=docs_chat_cfg,
    )


def _contracts_app_toml(state: Mapping[str, Any]) -> str | None:
    """Render the dc43-contracts-app TOML derived from the wizard configuration."""

    config = _contracts_app_config_from_state(state)
    if config is None:
        return None
    toml_text = dump_contracts_app_config(config)
    return toml_text or None


def _pipeline_example_assets(state: Mapping[str, Any]) -> PipelineExample:
    """Return integration-aware pipeline example assets."""

    return render_pipeline_stub(state, clean_str=_clean_str)


def _integration_selection(state: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Return the selected pipeline integration and its configuration."""

    selected = state.get("selected_options") if isinstance(state, Mapping) else {}
    if not isinstance(selected, Mapping):
        selected = {}

    configuration = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(configuration, Mapping):
        configuration = {}

    integration_key = str(selected.get("pipeline_integration") or "")
    integration_config_raw = configuration.get("pipeline_integration", {})
    integration_config = (
        dict(integration_config_raw)
        if isinstance(integration_config_raw, Mapping)
        else {}
    )
    return integration_key, integration_config


def _pipeline_bootstrap_script(state: Mapping[str, Any]) -> str:
    """Return the helper script that loads the generated configuration."""

    integration_key, integration_config = _integration_selection(state)

    spark_runtime = _clean_str(integration_config.get("runtime"))
    spark_workspace_url = _clean_str(integration_config.get("workspace_url"))
    spark_workspace_profile = _clean_str(integration_config.get("workspace_profile"))
    spark_cluster = _clean_str(integration_config.get("cluster_reference"))

    dlt_workspace_url = _clean_str(integration_config.get("workspace_url"))
    dlt_workspace_profile = _clean_str(integration_config.get("workspace_profile"))
    dlt_pipeline_name = _clean_str(integration_config.get("pipeline_name"))
    dlt_notebook_path = _clean_str(integration_config.get("notebook_path"))
    dlt_target_schema = _clean_str(integration_config.get("target_schema"))

    lines: List[str] = [
        "#!/usr/bin/env python3",
        "'''Bootstrap dc43 service backends for pipeline orchestration.",
        "",
        "This script loads the generated dc43-service-backends TOML file and",
        "exposes helpers tailored to the integration runtime you captured in",
        "the setup wizard.",
        "'''",
        "",
        "import os",
        "from pathlib import Path",
        "",
        "from dc43_service_backends.bootstrap import build_backends",
        "from dc43_service_backends.config import load_config",
        "",
        "",
        "def load_backends():",
        "    \"\"\"Load the dc43 backends declared in the exported TOML file.\"\"\"",
        "    bundle_root = Path(__file__).resolve().parent.parent",
        "    config_path = bundle_root / \"config\" / \"dc43-service-backends.toml\"",
        "    config = load_config(config_path)",
        "    return build_backends(config)",
    ]

    if integration_key == "spark":
        lines.extend(
            [
                "",
                "def build_spark_context(app_name=\"dc43-pipeline\"):",
                "    \"\"\"Return Spark session and dc43 backends for PySpark jobs.\"\"\"",
                "    suite = load_backends()",
                "    try:",
                "        from pyspark.sql import SparkSession",
                "    except ModuleNotFoundError as exc:",
                "        raise RuntimeError(\"Install pyspark to run the Spark integration.\") from exc",
                "    spark = SparkSession.builder.appName(app_name).getOrCreate()",
                "    return {",
                "        \"spark\": spark,",
                "        \"contract_backend\": suite.contract,",
                "        \"data_product_backend\": suite.data_product,",
                "        \"data_quality_backend\": suite.data_quality,",
                "        \"governance_store\": suite.governance_store,",
                "    }",
            ]
        )

    if integration_key == "dlt":
        host_literal = repr(dlt_workspace_url)
        profile_literal = repr(dlt_workspace_profile)
        pipeline_literal = repr(dlt_pipeline_name)
        notebook_literal = repr(dlt_notebook_path)
        target_literal = repr(dlt_target_schema)
        lines.extend(
            [
                "",
                "def build_dlt_context():",
                "    \"\"\"Return Databricks workspace handles and dc43 backends for DLT.\"\"\"",
                "    suite = load_backends()",
                "    try:",
                "        from databricks.sdk import WorkspaceClient",
                "    except ModuleNotFoundError as exc:",
                "        raise RuntimeError(\"Install databricks-sdk to drive Delta Live Tables.\") from exc",
                "    client_kwargs = {}",
                f"    host = {host_literal}",
                "    if host:",
                "        client_kwargs['host'] = host",
                f"    profile = {profile_literal}",
                "    if profile:",
                "        client_kwargs['config_profile'] = profile",
                "    token = os.getenv(\"DATABRICKS_TOKEN\")",
                "    if token:",
                "        client_kwargs.setdefault('token', token)",
                "    workspace = WorkspaceClient(**client_kwargs)",
                "    return {",
                "        \"workspace\": workspace,",
                "        \"contract_backend\": suite.contract,",
                "        \"data_product_backend\": suite.data_product,",
                "        \"data_quality_backend\": suite.data_quality,",
                "        \"governance_store\": suite.governance_store,",
                f"        \"pipeline_name\": {pipeline_literal}",
                f"        \"notebook_path\": {notebook_literal}",
                f"        \"target_schema\": {target_literal}",
                "    }",
            ]
        )

    spark_runtime_literal = repr(spark_runtime)
    spark_workspace_literal = repr(spark_workspace_url)
    spark_profile_literal = repr(spark_workspace_profile)
    spark_cluster_literal = repr(spark_cluster)
    lines.extend(
        [
            "",
            "def main() -> None:",
            "    suite = load_backends()",
            "    print(\"Contract backend initialised:\", suite.contract.__class__.__name__)",
            "    print(\"Data product backend initialised:\", suite.data_product.__class__.__name__)",
            "    print(\"Data-quality backend initialised:\", suite.data_quality.__class__.__name__)",
            "    integration = \"" + integration_key + "\"",
            "    if integration == 'spark':",
            "        context = build_spark_context()",
            f"        runtime_hint = {spark_runtime_literal}",
            "        if runtime_hint:",
            "            print(\"Spark runtime hint:\", runtime_hint)",
            f"        workspace_hint = {spark_workspace_literal}",
            "        if workspace_hint:",
            "            print(\"Databricks workspace:\", workspace_hint)",
            f"        profile_hint = {spark_profile_literal}",
            "        if profile_hint:",
            "            print(\"Databricks CLI profile:\", profile_hint)",
            f"        cluster_hint = {spark_cluster_literal}",
            "        if cluster_hint:",
            "            print(\"Spark cluster reference:\", cluster_hint)",
            "        print(\"Spark session ready:\", context['spark'])",
            "    elif integration == 'dlt':",
            "        context = build_dlt_context()",
            "        workspace = context.get('workspace')",
            "        host = None",
            "        if isinstance(workspace, dict):",
            "            config = workspace.get('config')",
            "            if isinstance(config, dict):",
            "                host = config.get('host')",
            "        elif workspace is not None:",
            "            try:",
            "                config = workspace.config  # type: ignore[attr-defined]",
            "            except AttributeError:",
            "                config = None",
            "            if config is not None:",
            "                try:",
            "                    host = config.host  # type: ignore[attr-defined]",
            "                except AttributeError:",
            "                    host = None",
            "        if host:",
            "            print(\"DLT workspace host:\", host)",
            "        pipeline_name = context.get('pipeline_name')",
            "        if pipeline_name:",
            "            print(\"DLT pipeline name:\", pipeline_name)",
            "        notebook_path = context.get('notebook_path')",
            "        if notebook_path:",
            "            print(\"DLT notebook path:\", notebook_path)",
            "        target_schema = context.get('target_schema')",
            "        if target_schema:",
            "            print(\"DLT target schema:\", target_schema)",
            "    else:",
            "        print(\"No pipeline integration selected in the setup wizard.\")",
            "",
            "",
            "if __name__ == '__main__':",
            "    main()",
            "",
        ]
    )

    return "\n".join(lines)

@lru_cache(maxsize=None)
def _bundle_package_version(package_key: str) -> str | None:
    """Return the version string for ``package_key`` from the repository."""

    root = Path(__file__).resolve().parents[4]
    version_path = root / package_key / "VERSION"
    try:
        return version_path.read_text(encoding="utf-8").strip()
    except OSError:
        try:
            return importlib_metadata.version(package_key)
        except importlib_metadata.PackageNotFoundError:
            return None


def _bundle_requirement_spec(
    package: str,
    *,
    version: str | None = None,
    extras: str | None = None,
) -> str:
    """Return a pip requirement specifier with optional extras and version."""

    spec = package
    if extras:
        spec += f"[{extras}]"
    if version:
        spec += f"=={version}"
    return spec


def _bundle_requirements_text(state: Mapping[str, Any]) -> str:
    """Render a ``requirements.txt`` tailored to the selected integration."""

    integration_key, _ = _integration_selection(state)

    requirements: List[str] = [
        _bundle_requirement_spec(
            "dc43-service-clients",
            version=_bundle_package_version("dc43-service-clients"),
        ),
        _bundle_requirement_spec(
            "dc43-service-backends",
            extras="http",
            version=_bundle_package_version("dc43-service-backends"),
        ),
        _bundle_requirement_spec(
            "dc43-contracts-app",
            version=_bundle_package_version("dc43-contracts-app"),
        ),
        _bundle_requirement_spec(
            "dc43-integrations",
            version=_bundle_package_version("dc43-integrations"),
        ),
        "uvicorn[standard]>=0.24",
        "boto3>=1.28",
    ]

    if integration_key == "spark":
        requirements.append("pyspark>=3.4")
    if integration_key == "dlt":
        requirements.append("databricks-sdk>=0.12")
        requirements.append("databricks-dlt>=0.3")

    ordered = list(dict.fromkeys(item for item in requirements if item))
    header = [
        "# Requirements generated by the dc43 setup wizard",
        "# Run scripts/bootstrap_environment.sh (or .ps1 on Windows) to install them.",
        "",
    ]
    return "\n".join([*header, *ordered, ""])


def _bootstrap_environment_shell_script() -> str:
    """Return the POSIX shell helper that bootstraps a virtualenv."""

    return (
        textwrap.dedent(
            """
            #!/usr/bin/env bash
            set -euo pipefail

            ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
            VENV_DIR="${VENV_DIR:-$ROOT/.venv}"

            echo "[dc43] Creating virtual environment at $VENV_DIR"
            python3 -m venv "$VENV_DIR"

            # shellcheck source=/dev/null
            source "$VENV_DIR/bin/activate"
            python -m pip install --upgrade pip
            python -m pip install -r "$ROOT/requirements.txt"

            echo "[dc43] Environment ready. Activate it with 'source \"$VENV_DIR/bin/activate\"'."
            """
        ).strip()
        + "\n"
    )


def _bootstrap_environment_powershell_script() -> str:
    """Return the PowerShell helper that bootstraps a virtualenv on Windows."""

    return (
        textwrap.dedent(
            """
            Param(
                [string]$VenvDir
            )

            $Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
            if (-not $VenvDir) {
                $VenvDir = Join-Path $Root '.venv'
            }

            Write-Host "[dc43] Creating virtual environment at $VenvDir"
            python -m venv $VenvDir

            $Python = Join-Path $VenvDir 'Scripts\\python.exe'
            & $Python -m pip install --upgrade pip
            & $Python -m pip install -r (Join-Path $Root 'requirements.txt')

            Write-Host "[dc43] Environment ready. Activate it with & $VenvDir\\Scripts\\Activate.ps1"
            """
        ).strip()
        + "\n"
    )


def _environment_setup_files(state: Mapping[str, Any]) -> List[Tuple[str, str, bool]]:
    """Return files that bootstrap a Python environment for the bundle."""

    return [
        ("dc43-setup/requirements.txt", _bundle_requirements_text(state), False),
        (
            "dc43-setup/scripts/bootstrap_environment.sh",
            _bootstrap_environment_shell_script(),
            True,
        ),
        (
            "dc43-setup/scripts/bootstrap_environment.ps1",
            _bootstrap_environment_powershell_script(),
            True,
        ),
    ]


def _contracts_app_dockerfile() -> str:
    """Return a Dockerfile that installs the contracts UI from PyPI."""

    clients_spec = _bundle_requirement_spec(
        "dc43-service-clients",
        version=_bundle_package_version("dc43-service-clients"),
    )
    backends_spec = _bundle_requirement_spec(
        "dc43-service-backends",
        extras="http",
        version=_bundle_package_version("dc43-service-backends"),
    )
    contracts_spec = _bundle_requirement_spec(
        "dc43-contracts-app",
        version=_bundle_package_version("dc43-contracts-app"),
    )

    return (
        textwrap.dedent(
            f"""
            # syntax=docker/dockerfile:1
            FROM python:3.11-slim AS runtime

            ENV PYTHONUNBUFFERED=1 \
                PIP_DISABLE_PIP_VERSION_CHECK=1 \
                DC43_CONTRACTS_APP_BACKEND_MODE=remote

            WORKDIR /app

            RUN pip install --no-cache-dir \
                \"{clients_spec}\" \
                \"{backends_spec}\" \
                \"{contracts_spec}\" \
                \"uvicorn[standard]>=0.24\"

            EXPOSE 8000

            CMD [\"uvicorn\", \"dc43_contracts_app.server:app\", \"--host\", \"0.0.0.0\", \"--port\", \"8000\"]
            """
        ).strip()
        + "\n"
    )


def _service_backends_dockerfile() -> str:
    """Return a Dockerfile that installs the service backends from PyPI."""

    clients_spec = _bundle_requirement_spec(
        "dc43-service-clients",
        version=_bundle_package_version("dc43-service-clients"),
    )
    backends_spec = _bundle_requirement_spec(
        "dc43-service-backends",
        extras="http",
        version=_bundle_package_version("dc43-service-backends"),
    )

    return (
        textwrap.dedent(
            f"""
            # syntax=docker/dockerfile:1
            FROM python:3.11-slim AS runtime

            ENV PYTHONUNBUFFERED=1 \
                PIP_DISABLE_PIP_VERSION_CHECK=1 \
                DC43_SERVICE_BACKENDS_CONFIG=/app/config/dc43-service-backends.toml

            WORKDIR /app

            RUN pip install --no-cache-dir \
                \"{clients_spec}\" \
                \"{backends_spec}\" \
                \"uvicorn[standard]>=0.24\"

            EXPOSE 8001

            CMD [\"uvicorn\", \"dc43_service_backends.webapp:app\", \"--host\", \"0.0.0.0\", \"--port\", \"8001\"]
            """
        ).strip()
        + "\n"
    )


def _docker_build_script() -> str:
    """Return a helper that builds Docker images from the bundled Dockerfiles."""

    return (
        textwrap.dedent(
            """
            #!/usr/bin/env bash
            set -euo pipefail

            ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
            TAG="${1:-latest}"

            docker build -t "dc43/contracts-app:${TAG}" -f "$ROOT/docker/contracts-app/Dockerfile" "$ROOT"
            docker build -t "dc43/service-backends:${TAG}" -f "$ROOT/docker/service-backends/Dockerfile" "$ROOT"

            echo "[dc43] Built images dc43/contracts-app:${TAG} and dc43/service-backends:${TAG}."
            """
        ).strip()
        + "\n"
    )


def _docker_publish_script() -> str:
    """Return a helper that pushes the Docker images to AWS ECR."""

    return (
        textwrap.dedent(
            """
            #!/usr/bin/env python3
            \"\"\"Publish dc43 Docker images to an AWS Elastic Container Registry.\"\"\"

            from __future__ import annotations

            import argparse
            import base64
            import subprocess
            import sys

            try:  # pragma: no cover - optional dependency guard
                import boto3
            except Exception as exc:  # pragma: no cover - surfaced to the caller
                boto3 = None  # type: ignore[assignment]
                _BOTO_IMPORT_ERROR = exc
            else:  # pragma: no cover - executed when boto3 is present
                _BOTO_IMPORT_ERROR = None

            IMAGES: dict[str, str] = {
                "contracts-app": "dc43/contracts-app",
                "service-backends": "dc43/service-backends",
            }


            def _ensure_repository(client, name: str) -> None:
                try:
                    client.describe_repositories(repositoryNames=[name])
                except client.exceptions.RepositoryNotFoundException:
                    client.create_repository(repositoryName=name)


            def _docker(command: list[str], *, input_text: str | None = None) -> None:
                subprocess.run(
                    command,
                    input=input_text,
                    text=input_text is not None,
                    check=True,
                )


            def main() -> None:
                if boto3 is None:
                    raise SystemExit(
                        "Install boto3 (pip install boto3) before running publish_docker_images.py"
                    ) from _BOTO_IMPORT_ERROR

                parser = argparse.ArgumentParser(
                    description="Create ECR repositories and push the dc43 Docker images.",
                )
                parser.add_argument(
                    "--region",
                    default="us-east-1",
                    help="AWS region for the ECR registry (default: us-east-1).",
                )
                parser.add_argument(
                    "--profile",
                    help="Optional named AWS profile to use for authentication.",
                )
                parser.add_argument(
                    "--account-id",
                    help="AWS account ID. When omitted the caller identity is used.",
                )
                parser.add_argument(
                    "--repository-prefix",
                    default="dc43",
                    help="Prefix used for the ECR repositories (default: dc43).",
                )
                parser.add_argument(
                    "--tag",
                    default="latest",
                    help="Image tag to push (default: latest).",
                )
                args = parser.parse_args()

                session_kwargs: dict[str, str] = {"region_name": args.region}
                if args.profile:
                    session_kwargs["profile_name"] = args.profile
                session = boto3.Session(**session_kwargs)
                ecr = session.client("ecr")
                sts = session.client("sts")

                account_id = args.account_id or sts.get_caller_identity()["Account"]
                repositories = {
                    key: f"{args.repository_prefix}/{key}" for key in IMAGES.keys()
                }

                for repository in repositories.values():
                    print(f"[dc43] Ensuring ECR repository '{repository}' exists in {args.region}.")
                    _ensure_repository(ecr, repository)

                auth = ecr.get_authorization_token(registryIds=[account_id])["authorizationData"][0]
                token = base64.b64decode(auth["authorizationToken"]).decode("utf-8")
                username, password = token.split(":", 1)
                endpoint = auth["proxyEndpoint"]
                print(f"[dc43] Logging in to {endpoint}.")
                _docker(
                    ["docker", "login", "-u", username, "--password-stdin", endpoint],
                    input_text=password,
                )

                for key, local_image in IMAGES.items():
                    repository = repositories[key]
                    remote = f"{account_id}.dkr.ecr.{args.region}.amazonaws.com/{repository}:{args.tag}"
                    local = f"{local_image}:{args.tag}"
                    print(f"[dc43] Tagging {local} as {remote}.")
                    _docker(["docker", "tag", local, remote])
                    print(f"[dc43] Pushing {remote}.")
                    _docker(["docker", "push", remote])

                print("[dc43] Done. Update your deployment manifests to use the pushed tags.")


            if __name__ == "__main__":  # pragma: no cover - helper entry point
                try:
                    main()
                except subprocess.CalledProcessError as exc:
                    raise SystemExit(exc.returncode) from exc

            """
        ).strip()
        + "\n"
    )


def _docker_bundle_files(state: Mapping[str, Any]) -> List[Tuple[str, str, bool]]:
    """Return Docker-related assets for the setup bundle."""

    return [
        (
            "dc43-setup/docker/contracts-app/Dockerfile",
            _contracts_app_dockerfile(),
            False,
        ),
        (
            "dc43-setup/docker/service-backends/Dockerfile",
            _service_backends_dockerfile(),
            False,
        ),
        (
            "dc43-setup/scripts/build_docker_images.sh",
            _docker_build_script(),
            True,
        ),
        (
            "dc43-setup/scripts/publish_docker_images.py",
            _docker_publish_script(),
            True,
        ),
    ]


def _start_stack_script() -> str:
    """Return a helper script that launches the local UI and backend."""

    return textwrap.dedent(
        """\
        #!/usr/bin/env python3
        '''Start the dc43 UI and backend using the exported configuration.'''

        import argparse
        import os
        import subprocess
        import sys
        import time
        from pathlib import Path
        from urllib.error import URLError
        from urllib.request import urlopen

        from dc43_contracts_app.config import load_config as load_contracts_config
        from dc43_service_backends.config import load_config as load_service_config


        def _wait_for(url: str, timeout: float = 30.0) -> None:
            probe = url.rstrip("/") + "/openapi.json"
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    with urlopen(probe, timeout=2):
                        return
                except URLError:
                    time.sleep(0.25)
            raise RuntimeError(f"Service at {url} failed to start within {timeout}s")


        def main() -> None:
            parser = argparse.ArgumentParser(
                description="Launch the dc43 stack using exported configuration files.",
            )
            parser.add_argument(
                "--ui-host",
                default="127.0.0.1",
                help="Host interface for the contracts UI (default: 127.0.0.1)",
            )
            parser.add_argument(
                "--ui-port",
                type=int,
                default=8000,
                help="Port for the contracts UI (default: 8000)",
            )
            parser.add_argument(
                "--wait-timeout",
                type=float,
                default=45.0,
                help="Seconds to wait for the backend API before aborting (default: 45)",
            )
            args = parser.parse_args()

            bundle_root = Path(__file__).resolve().parent.parent
            config_dir = bundle_root / "config"
            contracts_config_path = config_dir / "dc43-contracts-app.toml"
            service_config_path = config_dir / "dc43-service-backends.toml"

            contracts_config = load_contracts_config(contracts_config_path)
            # Ensure the backends configuration file exists even if we do not inspect it yet.
            load_service_config(service_config_path)

            backend_cfg = contracts_config.backend
            process_cfg = backend_cfg.process
            backend_host = process_cfg.host
            backend_port = process_cfg.port
            backend_url = backend_cfg.base_url or process_cfg.url()

            backend_env = os.environ.copy()
            backend_env.setdefault("DC43_SERVICE_BACKENDS_CONFIG", str(service_config_path))

            backend_proc = None
            try:
                if backend_cfg.mode != "remote":
                    backend_cmd = [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "dc43_service_backends.webapp:app",
                        "--host",
                        backend_host,
                        "--port",
                        str(backend_port),
                    ]
                    if process_cfg.log_level:
                        backend_cmd.extend(["--log-level", process_cfg.log_level])

                    print(
                        f"Starting service backends at http://{backend_host}:{backend_port} using {service_config_path}..."
                    )
                    backend_proc = subprocess.Popen(backend_cmd, env=backend_env)
                    try:
                        _wait_for(backend_url, timeout=args.wait_timeout)
                    except Exception:
                        backend_proc.terminate()
                        try:
                            backend_proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            backend_proc.kill()
                        raise
                else:
                    print(
                        "Contracts app configured for remote governance; skipping local backend startup."
                    )

                ui_env = os.environ.copy()
                ui_env.setdefault("DC43_CONTRACTS_APP_CONFIG", str(contracts_config_path))

                ui_cmd = [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "dc43_contracts_app.server:app",
                    "--host",
                    args.ui_host,
                    "--port",
                    str(args.ui_port),
                ]

                print(
                    f"Starting contracts UI on http://{args.ui_host}:{args.ui_port} using {contracts_config_path}..."
                )
                subprocess.run(ui_cmd, check=True, env=ui_env)
            except KeyboardInterrupt:
                pass
            finally:
                if backend_proc is not None:
                    backend_proc.terminate()
                    try:
                        backend_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        backend_proc.kill()


        if __name__ == "__main__":
            main()
        """
    )



def _toml_ready(value: Any) -> Any:
    """Normalise ``value`` into TOML-compatible primitives."""

    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        payload: Dict[str, Any] = {}
        for key, raw in value.items():
            cleaned_key = str(key)
            cleaned_value = _toml_ready(raw)
            if cleaned_value is None:
                continue
            payload[cleaned_key] = cleaned_value
        return payload
    if isinstance(value, (list, tuple, set)):
        items: list[Any] = []
        for raw in value:
            cleaned = _toml_ready(raw)
            if cleaned is None:
                continue
            items.append(cleaned)
        return items
    return str(value)


def _wizard_module_toml(
    module_key: str,
    module_config: Mapping[str, Any],
    selected_option: str | None,
) -> tuple[str, str] | None:
    """Return archive path and TOML text for ``module_config``."""

    payload: Dict[str, Any] = {}
    if selected_option:
        payload["selected_option"] = selected_option

    safe_key = module_key.replace("/", "-")
    path = f"dc43-setup/config/modules/{safe_key}.toml"

    normalised: Dict[str, Any] = {}
    for field_name, raw_value in module_config.items():
        cleaned_value = _toml_ready(raw_value)
        if cleaned_value is None:
            continue
        normalised[str(field_name)] = cleaned_value

    payload.update(normalised)

    toml_text = mapping_to_toml(payload)
    if not toml_text:
        return None

    return path, toml_text


def _setup_bundle_readme(payload: Mapping[str, Any]) -> str:
    """Return README text for the setup export archive."""

    generated_at = payload.get("generated_at") or datetime.utcnow().isoformat() + "Z"
    lines = [
        "# DC43 setup bundle",
        "",
        "This archive was generated by the environment setup wizard.",
        f"Generated at: {generated_at}",
        "",
        "Files included:",
        "- configuration.json — complete summary of module selections and settings.",
        "- modules/<module>.json — per-module configuration stubs for automation.",
        "- config/dc43-service-backends.toml — drop-in configuration for the backend services.",
        "- config/dc43-contracts-app.toml — configuration for the web interface.",
        "- config/modules/<module>.toml — raw field values captured for each wizard module.",
        "- scripts/bootstrap_pipeline.py — helper to load the configuration from pipelines.",
        "- examples/ — integration-aware starter projects provided by each integration.",
        "- requirements.txt — pinned Python dependencies for the starter projects and tooling.",
        "- scripts/bootstrap_environment.sh / scripts/bootstrap_environment.ps1 — create a virtual environment and install requirements.",
        "- scripts/run_local_stack.py — start the local UI and backend services for quick testing.",
        "- docker/contracts-app/Dockerfile — container image definition for the contracts UI.",
        "- docker/service-backends/Dockerfile — container image definition for the governance and product services.",
        "- scripts/build_docker_images.sh — build the Docker images without recreating the Dockerfiles manually.",
        "- scripts/publish_docker_images.py — push the Docker images to AWS Elastic Container Registry (ECR).",
        "- terraform/governance/<provider>/ — Terraform templates and generated variables for governance deployments (when selected).",
        "- terraform/ui/<provider>/ — Terraform variable stubs for UI hosting (when selected).",
        "",
        "How to use:",
        "1. Run `scripts/bootstrap_environment.sh` (or `.ps1` on Windows) to create a `.venv` with the required Python packages and activate it before running the helpers.",
        "2. Copy the TOML files into your deployment repository or configuration management system.",
        "3. Update any commented placeholders (for example Unity Catalog tables or secrets).",
        "4. Execute `scripts/run_local_stack.py` to launch the local stack or `scripts/bootstrap_pipeline.py` inside orchestration jobs using the prepared environment.",
        "5. Use `scripts/build_docker_images.sh` to build container images locally or `scripts/publish_docker_images.py` to create and push AWS ECR repositories when you need prebuilt containers.",
        "",
        "Modules exported:",
    ]

    modules = payload.get("modules") if isinstance(payload, Mapping) else []
    if isinstance(modules, Iterable):
        for module in modules:
            if not isinstance(module, Mapping):
                continue
            title = module.get("title") or module.get("key") or "module"
            option_label = module.get("option_label") or module.get("option") or "selection"
            lines.append(f"- {title} — {option_label}")

    lines.extend(
        [
            "",
            "You can revisit /setup to regenerate this bundle after making different selections.",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_setup_bundle(state: Mapping[str, Any]) -> Tuple[io.BytesIO, Dict[str, Any]]:
    """Return a ZIP archive stream and payload for the current wizard state."""

    payload = _setup_export_payload(state)
    modules = payload.get("modules")
    if not modules:
        raise ValueError("No modules available to export.")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("dc43-setup/README.md", _setup_bundle_readme(payload))
        archive.writestr(
            "dc43-setup/configuration.json",
            json.dumps(payload, indent=2, sort_keys=True),
        )
        for module in modules:
            if not isinstance(module, Mapping):
                continue
            module_key = str(module.get("key") or "module").replace("/", "-")
            archive.writestr(
                f"dc43-setup/modules/{module_key}.json",
                json.dumps(module, indent=2, sort_keys=True),
            )

        service_backends_toml = _service_backends_toml(state)
        if service_backends_toml:
            archive.writestr(
                "dc43-setup/config/dc43-service-backends.toml",
                service_backends_toml,
            )

        contracts_app_toml = _contracts_app_toml(state)
        if contracts_app_toml:
            archive.writestr(
                "dc43-setup/config/dc43-contracts-app.toml",
                contracts_app_toml,
            )

        configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
        selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
        configuration: Dict[str, Mapping[str, Any]]
        if isinstance(configuration_raw, Mapping):
            configuration = {
                str(module_key): module_config
                for module_key, module_config in configuration_raw.items()
                if isinstance(module_config, Mapping)
            }
        else:
            configuration = {}
        if isinstance(selected_raw, Mapping):
            selected = {
                str(module_key): str(option)
                for module_key, option in selected_raw.items()
                if option is not None
            }
        else:
            selected = {}

        for module_key, module_config in configuration.items():
            module_selected = selected.get(module_key)
            result = _wizard_module_toml(module_key, module_config, module_selected)
            if not result:
                continue
            path, text = result
            archive.writestr(path, text)

        bootstrap_script = _pipeline_bootstrap_script(state)
        script_info = zipfile.ZipInfo("dc43-setup/scripts/bootstrap_pipeline.py")
        script_info.external_attr = 0o755 << 16  # Mark the script as executable.
        archive.writestr(script_info, bootstrap_script)

        example_assets = _pipeline_example_assets(state)
        example_path = f"dc43-setup/{example_assets.entrypoint_path}".replace("//", "/")
        example_info = zipfile.ZipInfo(example_path)
        entry_mode = 0o755 if example_assets.entrypoint_executable else 0o644
        example_info.external_attr = entry_mode << 16
        archive.writestr(example_info, example_assets.entrypoint_content)

        for support in example_assets.support_files:
            support_path = f"dc43-setup/{support.path}".replace("//", "/")
            support_info = zipfile.ZipInfo(support_path)
            support_mode = 0o755 if support.executable else 0o644
            support_info.external_attr = support_mode << 16
            archive.writestr(support_info, support.content)

        for relative_path, content, executable in _environment_setup_files(state):
            env_info = zipfile.ZipInfo(relative_path)
            env_mode = 0o755 if executable else 0o644
            env_info.external_attr = env_mode << 16
            archive.writestr(env_info, content)

        for relative_path, content, executable in _docker_bundle_files(state):
            docker_info = zipfile.ZipInfo(relative_path)
            docker_mode = 0o755 if executable else 0o644
            docker_info.external_attr = docker_mode << 16
            archive.writestr(docker_info, content)

        start_script = _start_stack_script()
        start_info = zipfile.ZipInfo("dc43-setup/scripts/run_local_stack.py")
        start_info.external_attr = 0o755 << 16
        archive.writestr(start_info, start_script)

        for relative_path, content in _terraform_bundle_files(state):
            archive.writestr(relative_path, content)

    buffer.seek(0)
    return buffer, payload


def _build_setup_context(
    request: Request,
    state: Mapping[str, Any],
    *,
    step: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return the template context shared by onboarding views."""

    selected_options_raw = state.get("selected_options") if isinstance(state, Mapping) else {}
    raw_selected_options: Dict[str, str] = {}
    if isinstance(selected_options_raw, Mapping):
        raw_selected_options = {str(key): str(value) for key, value in selected_options_raw.items()}

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    configuration: Dict[str, Any] = {}
    if isinstance(configuration_raw, Mapping):
        configuration = {str(key): value for key, value in configuration_raw.items()}

    module_order = list(SETUP_MODULES.keys())
    resolved_selected: Dict[str, str] = dict(raw_selected_options)
    auto_selected: Set[str] = set()
    for module_key in module_order:
        visible_options = _module_visible_options(module_key, resolved_selected)
        current_value = resolved_selected.get(module_key)
        if current_value and visible_options and current_value not in visible_options:
            current_value = None
        auto_choice = False
        if not current_value and visible_options:
            default_option = _module_default_option(module_key, resolved_selected)
            if default_option and (not visible_options or default_option in visible_options):
                current_value = default_option
                if raw_selected_options.get(module_key) != default_option:
                    auto_choice = True
        if current_value:
            resolved_selected[module_key] = current_value
            if auto_choice:
                auto_selected.add(module_key)
            elif module_key in auto_selected and raw_selected_options.get(module_key) == current_value:
                auto_selected.discard(module_key)
        else:
            resolved_selected.pop(module_key, None)
            auto_selected.discard(module_key)

    selected_options = resolved_selected

    requested_step = int(state.get("current_step") or 1) if step is None else int(step)
    if requested_step > 1 and not raw_selected_options:
        requested_step = 1

    if requested_step >= _SETUP_TOTAL_STEPS and not state.get("completed"):
        if _requires_configuration(selected_options, configuration):
            requested_step = 2

    if state.get("completed") and step is None:
        requested_step = _SETUP_TOTAL_STEPS

    workspace = _workspace_hint_map()

    module_to_group: Dict[str, str] = {}
    for group_meta in SETUP_MODULE_GROUPS:
        for group_module in group_meta.get("modules", []):
            module_to_group[str(group_module)] = group_meta["key"]

    modules: List[Dict[str, Any]] = []
    modules_by_key: Dict[str, Dict[str, Any]] = {}
    modules_metadata: Dict[str, Any] = {}
    for module_key in module_order:
        module_meta = SETUP_MODULES[module_key]
        visible_options = _module_visible_options(module_key, selected_options)
        module_hidden = _module_should_hide(module_key, selected_options) or (
            module_meta.get("depends_on") and not visible_options
        )

        option_items: Iterable[Tuple[str, Mapping[str, Any]]] = module_meta["options"].items()
        if visible_options:
            option_items = [
                (key, module_meta["options"][key])
                for key in visible_options
                if key in module_meta["options"]
            ]

        options: List[Dict[str, Any]] = []
        safe_options: Dict[str, Any] = {}
        for option_key, option_meta in option_items:
            options.append(
                {
                    "key": option_key,
                    "label": option_meta.get("label"),
                    "description": option_meta.get("description", ""),
                    "selected": selected_options.get(module_key) == option_key,
                }
            )
            safe_fields: List[Dict[str, Any]] = []
            for field_meta in option_meta.get("fields", []):
                field_name = field_meta.get("name")
                if not field_name:
                    continue
                safe_fields.append(
                    {
                        "name": field_name,
                        "label": field_meta.get("label"),
                        "optional": bool(field_meta.get("optional")),
                    }
                )
            safe_option: Dict[str, Any] = {
                "label": option_meta.get("label"),
                "description": option_meta.get("description", ""),
                "fields": safe_fields,
                "skip_configuration": bool(option_meta.get("skip_configuration")),
            }
            diagram_meta = option_meta.get("diagram")
            if isinstance(diagram_meta, Mapping):
                serialised_diagram = _serialise_diagram(diagram_meta)
                if serialised_diagram:
                    safe_option["diagram"] = serialised_diagram
            safe_options[option_key] = safe_option

        selected_value = selected_options.get(module_key)
        requires_choice = len(visible_options) > 1 and not module_hidden
        module_missing = requires_choice and (
            not selected_value or selected_value not in visible_options
        )
        if (
            not module_missing
            and module_key in auto_selected
            and not module_hidden
            and len(visible_options) > 1
        ):
            selected_option_meta = module_meta["options"].get(selected_value) if selected_value else None
            skip_auto = bool(selected_option_meta and selected_option_meta.get("skip_configuration"))
            if not skip_auto:
                module_missing = True

        module_payload = {
            "key": module_key,
            "title": module_meta.get("title"),
            "summary": module_meta.get("summary", ""),
            "options": options,
            "group_key": module_to_group.get(module_key),
            "hidden": module_hidden,
            "missing_selection": module_missing,
            "visible_options": visible_options,
            "auto_selected": module_key in auto_selected,
        }
        modules.append(module_payload)
        modules_by_key[module_key] = module_payload
        modules_metadata[module_key] = {
            "title": module_meta.get("title"),
            "summary": module_meta.get("summary", ""),
            "group": module_to_group.get(module_key),
            "options": safe_options,
            "depends_on": module_meta.get("depends_on"),
            "visible_when": module_meta.get("visible_when"),
            "hide_when": module_meta.get("hide_when"),
            "default_for": module_meta.get("default_for"),
            "default_option": module_meta.get("default_option"),
            "hidden": module_hidden,
            "auto_selected": module_key in auto_selected,
        }

    module_groups: List[Dict[str, Any]] = []
    grouped_keys: List[str] = []
    for group_meta in SETUP_MODULE_GROUPS:
        group_module_keys = [key for key in group_meta.get("modules", []) if key in modules_by_key]
        if not group_module_keys:
            continue
        group_modules = [modules_by_key[key] for key in group_module_keys]
        visible_count = sum(1 for module in group_modules if not module.get("hidden"))
        missing_count = sum(1 for module in group_modules if module.get("missing_selection"))
        module_groups.append(
            {
                "key": group_meta["key"],
                "title": group_meta.get("title"),
                "summary": group_meta.get("summary", ""),
                "modules": group_modules,
                "module_keys": group_module_keys,
                "visible_count": visible_count,
                "missing_count": missing_count,
                "total_count": len(group_module_keys),
            }
        )
        grouped_keys.extend(group_module_keys)

    remaining_module_keys = [key for key in module_order if key not in grouped_keys]
    if remaining_module_keys:
        remaining_modules = [modules_by_key[key] for key in remaining_module_keys]
        module_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_modules,
                "module_keys": remaining_module_keys,
                "visible_count": sum(1 for module in remaining_modules if not module.get("hidden")),
                "missing_count": sum(1 for module in remaining_modules if module.get("missing_selection")),
                "total_count": len(remaining_module_keys),
            }
        )

    selected_modules: List[Dict[str, Any]] = []
    for module_key in module_order:
        option_key = selected_options.get(module_key)
        if not option_key:
            continue
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
        pending_required = 0
        for field in fields:
            if field.get("optional"):
                continue
            value = str(field.get("value", "") or "").strip()
            if not value:
                pending_required += 1
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
                "group_key": module_to_group.get(module_key),
                "skip_configuration": bool(option_meta.get("skip_configuration")),
                "pending_required": pending_required,
            }
        )

    selected_by_key = {module["key"]: module for module in selected_modules}
    selected_groups: List[Dict[str, Any]] = []
    seen_selected: Set[str] = set()
    for group in module_groups:
        group_selected: List[Dict[str, Any]] = []
        for key in group.get("module_keys", []):
            if key not in selected_by_key:
                continue
            seen_selected.add(key)
            module_entry = selected_by_key[key]
            if module_entry.get("skip_configuration"):
                continue
            module_entry["has_pending"] = module_entry.get("pending_required", 0) > 0
            group_selected.append(module_entry)
        if not group_selected:
            continue
        pending_total = sum(module.get("pending_required", 0) for module in group_selected)
        selected_groups.append(
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": group_selected,
                "module_keys": [module["key"] for module in group_selected],
                "pending_required": pending_total,
            }
        )

    remaining_selected = [
        module
        for module in selected_modules
        if module["key"] not in seen_selected
    ]
    if remaining_selected:
        for module in remaining_selected:
            module["has_pending"] = module.get("pending_required", 0) > 0
        selected_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_selected,
                "module_keys": [module["key"] for module in remaining_selected],
                "pending_required": sum(module.get("pending_required", 0) for module in remaining_selected),
            }
        )

    for group_index, group in enumerate(selected_groups, start=1):
        group["display_index"] = group_index
        modules = group.get("modules", [])
        for module_index, module in enumerate(modules, start=1):
            module["display_index"] = f"{group_index}.{module_index}"

    summary_modules_map: Dict[str, Dict[str, Any]] = {}
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
        summary_modules_map[module_key] = {
            "key": module_key,
            "title": module.get("title"),
            "option_label": module["option"].get("label") if isinstance(module.get("option"), Mapping) else "",
            "installation": module.get("option", {}).get("installation", []) if isinstance(module.get("option"), Mapping) else [],
            "configuration_notes": module.get("option", {}).get("configuration_notes", []) if isinstance(module.get("option"), Mapping) else [],
            "fields": summary_fields,
        }

    summary_groups: List[Dict[str, Any]] = []
    seen_summary: List[str] = []
    for group in module_groups:
        group_summary = [
            summary_modules_map[key]
            for key in group.get("module_keys", [])
            if key in summary_modules_map
        ]
        if not group_summary:
            continue
        summary_groups.append(
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": group_summary,
            }
        )
        seen_summary.extend(item["key"] for item in group_summary)

    remaining_summary = [
        summary
        for summary in summary_modules_map.values()
        if summary["key"] not in seen_summary
    ]
    if remaining_summary:
        summary_groups.append(
            {
                "key": "additional_modules",
                "title": "Additional modules",
                "summary": "Options that are available outside the primary grouping.",
                "modules": remaining_summary,
            }
        )

    summary_modules: List[Dict[str, Any]] = [
        summary_modules_map[key]
        for key in module_order
        if key in summary_modules_map
    ]

    safe_selected: Dict[str, str] = {}
    for module_key, option_key in selected_options.items():
        if option_key is None:
            continue
        safe_selected[str(module_key)] = str(option_key)

    safe_configuration: Dict[str, Dict[str, Any]] = {}
    if isinstance(configuration, Mapping):
        for module_key, module_config in configuration.items():
            if not isinstance(module_config, Mapping):
                continue
            serialised_config: Dict[str, Any] = {}
            for field_name, value in module_config.items():
                if value in (None, ""):
                    continue
                if isinstance(value, (str, int, float, bool)):
                    serialised_config[str(field_name)] = value
                else:
                    serialised_config[str(field_name)] = str(value)
            if serialised_config:
                safe_configuration[str(module_key)] = serialised_config

    explicit_selected_keys: List[str] = []
    if isinstance(raw_selected_options, Mapping):
        seen_explicit: Set[str] = set()
        for module_key, option_key in raw_selected_options.items():
            if option_key is None:
                continue
            module_str = str(module_key)
            if module_str in seen_explicit:
                continue
            seen_explicit.add(module_str)
            explicit_selected_keys.append(module_str)
    explicit_selected_keys.sort()

    setup_state_payload = {
        "order": module_order,
        "selected": safe_selected,
        "configuration": safe_configuration,
        "modules": modules_metadata,
        "autoSelected": sorted(auto_selected),
        "explicitSelected": explicit_selected_keys,
        "groups": [
            {
                "key": group["key"],
                "title": group.get("title"),
                "summary": group.get("summary", ""),
                "modules": list(group.get("module_keys", [])),
            }
            for group in module_groups
        ],
    }

    configuration_ready = not _requires_configuration(selected_options, configuration)
    can_export_bundle = bool(selected_modules) and configuration_ready

    current_step = max(1, min(_SETUP_TOTAL_STEPS, requested_step))

    return {
        "request": request,
        "step": current_step,
        "progress": _setup_progress(current_step),
        "modules": modules,
        "module_groups": module_groups,
        "selected_modules": selected_modules,
        "selected_groups": selected_groups,
        "summary_modules": summary_modules,
        "summary_groups": summary_groups,
        "state": state,
        "errors": errors or [],
        "completed": bool(state.get("completed")),
        "can_export_bundle": can_export_bundle,
        "module_order": module_order,
        "setup_state_payload": setup_state_payload,
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
    observation_operation: str = ""
    observation_scope: str = ""
    observation_label: str = ""


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dataset_record_from_payload(payload: Mapping[str, Any]) -> DatasetRecord | None:
    dataset_name = str(payload.get("dataset_name") or payload.get("dataset_id") or "")
    dataset_version = str(payload.get("dataset_version") or "")
    contract_id = str(payload.get("contract_id") or "")
    contract_version = str(payload.get("contract_version") or "")
    details = payload.get("dq_details")
    dq_details = dict(details) if isinstance(details, Mapping) else {}
    status_value = _normalise_record_status(str(payload.get("status") or ""))
    run_type = str(payload.get("run_type") or "infer")
    violations_raw = payload.get("violations")
    try:
        violations = int(violations_raw) if isinstance(violations_raw, (int, float)) else 0
    except (TypeError, ValueError):
        violations = 0
    record = DatasetRecord(
        contract_id,
        contract_version,
        dataset_name,
        dataset_version,
        status_value,
        dq_details,
        run_type,
        violations,
        draft_contract_version=_coerce_optional_str(payload.get("draft_contract_version")),
        scenario_key=_coerce_optional_str(payload.get("scenario_key")),
        data_product_id=str(payload.get("data_product_id") or ""),
        data_product_port=str(payload.get("data_product_port") or ""),
        data_product_role=str(payload.get("data_product_role") or ""),
        observation_operation=str(payload.get("observation_operation") or ""),
        observation_scope=str(payload.get("observation_scope") or ""),
        observation_label=str(payload.get("observation_label") or ""),
    )
    record.reason = str(payload.get("reason") or "")
    return record


def _records_from_service(
    *, dataset_id: str | None, dataset_version: str | None
) -> List[DatasetRecord] | None:
    service = governance_service_client()
    if service is None:
        return None
    method = getattr(service, "get_dataset_records", None)
    if not callable(method):
        return None
    try:
        payloads = method(dataset_id=dataset_id, dataset_version=dataset_version)
    except Exception:  # pragma: no cover - defensive fallback when backend fails
        logger.exception("Failed to load dataset records via governance service")
        return None
    records: List[DatasetRecord] = []
    for payload in payloads:
        if isinstance(payload, Mapping):
            record = _dataset_record_from_payload(payload)
            if record is not None:
                records.append(record)
    return records


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
    """Summarise validation verdicts for ``dataset_id`` without filesystem access."""

    del dataset_path  # filesystem materialisations are demo-only

    records: List[Dict[str, Any]] = []
    if not dataset_records:
        return records

    scoped_versions: Dict[str, DatasetRecord] = {}
    for record in dataset_records:
        if record.dataset_name != dataset_id:
            continue
        if not record.dataset_version:
            continue
        scoped_versions[record.dataset_version] = record

    for version, record in scoped_versions.items():
        if contract and (contract.id or contract.version):
            if record.contract_id and contract.id and record.contract_id != contract.id:
                continue
            if (
                record.contract_version
                and contract.version
                and record.contract_version != contract.version
            ):
                continue
        status_value = record.status or "unknown"
        records.append(
            {
                "version": version,
                "stored_version": version,
                "status": status_value,
                "status_label": status_value.replace("_", " ").title(),
                "badge": _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
                "contract_id": record.contract_id,
                "contract_version": record.contract_version,
                "recorded_at": record.dq_details.get("recorded_at")
                if isinstance(record.dq_details, Mapping)
                else None,
            }
        )

    records.sort(key=lambda item: _version_sort_key(item["version"]))
    return records


@dataclass
class _ValidationSnapshot:
    status: str
    details: Any
    reason: Optional[str] = None


def _as_validation_result(payload: Any) -> Optional[ValidationResult | _ValidationSnapshot]:
    """Coerce governance payloads into :class:`ValidationResult` instances."""

    if payload is None:
        return None
    if isinstance(payload, ValidationResult):
        return payload
    if isinstance(payload, Mapping):
        try:
            return decode_validation_result(payload)
        except Exception:  # pragma: no cover - defensive guard for malformed payloads
            logger.exception("Failed to decode validation result payload")
            return None
    try:
        status_value = payload.status  # type: ignore[attr-defined]
        details_value = payload.details  # type: ignore[attr-defined]
    except AttributeError:
        return None
    try:
        reason_value = payload.reason  # type: ignore[attr-defined]
    except AttributeError:
        reason_value = None
    return _ValidationSnapshot(status=str(status_value), details=details_value, reason=reason_value)


def _server_details(contract: OpenDataContractStandard) -> Optional[Dict[str, Any]]:
    """Summarise the first server entry for UI consumption."""

    if not contract.servers:
        return None
    first = contract.servers[0]
    custom: Dict[str, Any] = custom_properties_dict(first)
    dataset_id = contract.id or first.dataset or contract.id
    info: Dict[str, Any] = {
        "server": first.server or "",
        "type": first.type or "",
        "format": first.format or "",
        "path": first.path or "",
        "dataset": first.dataset or "",
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
        elif isinstance(prop, CustomProperty):
            key = prop.property
            value = prop.value
        else:
            continue
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


def _latest_event(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    events = entry.get("events")
    if isinstance(events, list):
        for event in reversed(events):
            if isinstance(event, Mapping):
                return dict(event)
    return {}


def _normalise_record_status(value: str | None) -> str:
    """Map backend validation status strings onto UI-friendly labels."""

    if not value:
        return "unknown"
    text = str(value).strip().lower()
    if not text:
        return "unknown"
    if text in {"ok", "pass", "passed", "success", "succeeded", "valid"}:
        return "ok"
    if text in {"warn", "warning", "caution"}:
        return "warn"
    if text in {"block", "blocked", "error", "fail", "failed", "invalid", "ko"}:
        return "block"
    if text in {"stale", "outdated", "expired"}:
        return "stale"
    return "unknown"


def _extract_violation_count(section: Mapping[str, Any] | None) -> int:
    """Return the maximum violation count found within ``section``."""

    if not isinstance(section, Mapping):
        return 0

    total = 0
    candidate = section.get("violations")
    if isinstance(candidate, (int, float)):
        total = max(total, int(candidate))

    metrics = section.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if str(key).startswith("violations") and isinstance(value, (int, float)):
                total = max(total, int(value))

    failed = section.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if not isinstance(info, Mapping):
                continue
            count = info.get("count")
            if isinstance(count, (int, float)):
                total = max(total, int(count))

    errors = section.get("errors")
    if isinstance(errors, list):
        total = max(total, len(errors))

    details = section.get("details")
    if isinstance(details, Mapping):
        total = max(total, _extract_violation_count(details))

    dq_status = section.get("dq_status")
    if isinstance(dq_status, Mapping):
        total = max(total, _extract_violation_count(dq_status))

    return total


def load_records(
    *, dataset_id: str | None = None, dataset_version: str | None = None
) -> List[DatasetRecord]:
    """Return recorded dataset runs provided by the governance services."""

    service_records = _records_from_service(
        dataset_id=dataset_id, dataset_version=dataset_version
    )
    if service_records:
        return service_records

    records: List[DatasetRecord] = []
    record_indices: Dict[Tuple[str, str, str, str], int] = {}
    dataset_ids = [dataset_id] if dataset_id else list_dataset_ids()
    for dataset_name in dataset_ids:
        if not dataset_name:
            continue
        version_filter = dataset_version if dataset_id is not None else None
        activity = dataset_pipeline_activity(
            dataset_name,
            dataset_version=version_filter,
            include_status=True,
        )
        inline_status_available = any(
            isinstance(item, Mapping) and item.get("validation_status") is not None
            for item in activity
        )
        status_lookup: Dict[Tuple[str, str, str], Any] = {}
        if activity and not inline_status_available:
            contract_candidates = {
                str(item.get("contract_id") or "").strip()
                for item in activity
                if item.get("contract_id")
            }
            version_candidates: Set[str] = set()
            if version_filter:
                version_candidates.add(version_filter)
            else:
                version_candidates = {
                    value
                    for value in (
                        str(item.get("dataset_version") or "").strip()
                        for item in activity
                        if item.get("dataset_version")
                    )
                    if value and value.lower() != "latest"
                }
            matrix_entries = dataset_status_matrix(
                dataset_name,
                contract_ids=[c for c in contract_candidates if c],
                dataset_versions=[v for v in version_candidates if v],
            )
            for entry in matrix_entries:
                if isinstance(entry, Mapping):
                    cid = str(entry.get("contract_id") or "").strip()
                    cver = str(entry.get("contract_version") or "").strip()
                    dver = str(entry.get("dataset_version") or "").strip()
                    status_obj: Any = entry.get("status")
                elif isinstance(entry, DatasetContractStatus):
                    cid = str(entry.contract_id or "").strip()
                    cver = str(entry.contract_version or "").strip()
                    dver = str(entry.dataset_version or "").strip()
                    status_obj = entry.status
                else:
                    continue
                result = _as_validation_result(status_obj)
                if cid and cver and dver:
                    status_lookup[(cid, cver, dver)] = result
        for entry in activity:
            dataset_version_value = str(entry.get("dataset_version") or "").strip()
            if (
                version_filter
                and dataset_version_value
                and dataset_version_value != version_filter
            ):
                continue
            contract_id = str(entry.get("contract_id") or "").strip()
            contract_version = str(entry.get("contract_version") or "").strip()

            latest_event = _latest_event(entry)
            validation: Optional[ValidationResult] = None
            inline_details: Mapping[str, Any] | None = None
            if contract_id and contract_version and dataset_version_value:
                inline_payload = entry.get("validation_status")
                if isinstance(inline_payload, Mapping):
                    maybe_details = inline_payload.get("details")
                    if isinstance(maybe_details, Mapping):
                        inline_details = dict(maybe_details)
                if inline_payload is not None:
                    validation = _as_validation_result(inline_payload)
                else:
                    validation = status_lookup.get(
                        (contract_id, contract_version, dataset_version_value)
                    )
                if (
                    validation is None
                    and not inline_status_available
                    and dataset_version_value.lower() != "latest"
                ):
                    validation = _as_validation_result(
                        dataset_validation_status(
                            contract_id=contract_id,
                            contract_version=contract_version,
                            dataset_id=dataset_name,
                            dataset_version=dataset_version_value,
                        )
                    )

            details: Dict[str, Any] = {}
            reason = ""
            status_value = "unknown"
            observation_operation = ""
            observation_scope = ""
            observation_label = ""
            if validation is not None:
                try:
                    status_value = _normalise_record_status(validation.status)
                    details = dict(validation.details)
                    if inline_details is not None:
                        details = inline_details
                    reason = validation.reason or ""
                except Exception:  # pragma: no cover - defensive guard
                    logger.exception(
                        "Failed to interpret validation status for %s@%s",
                        dataset_name,
                        dataset_version_value,
                    )
                    details = {}
            else:
                raw_status = str(latest_event.get("dq_status", "unknown"))
                status_value = _normalise_record_status(raw_status)
                details = dict(latest_event.get("dq_details") or {})
                reason = str(latest_event.get("dq_reason") or "")

            if details:
                observation_operation = str(details.get("observation_operation") or "")
                observation_scope = str(details.get("observation_scope") or "")
                observation_label = str(details.get("observation_label") or "")
                if observation_scope and not observation_label:
                    observation_label = observation_scope.replace("_", " ").title()

            pipeline_context = latest_event.get("pipeline_context")
            run_type = "infer"
            scenario_key = None
            if isinstance(pipeline_context, Mapping):
                run_type = str(pipeline_context.get("run_type", run_type))
                scenario_key_value = pipeline_context.get("scenario_key")
                if isinstance(scenario_key_value, str) and scenario_key_value:
                    scenario_key = scenario_key_value

            if latest_event.get("scenario_key") and not scenario_key:
                scenario_key = str(latest_event.get("scenario_key"))

            draft_version = details.get("draft_contract_version")
            if not isinstance(draft_version, str):
                draft_version = latest_event.get("draft_contract_version")
                if not isinstance(draft_version, str):
                    draft_version = None

            data_product_id = ""
            data_product_port = ""
            data_product_role = ""
            data_product_info = latest_event.get("data_product")
            if isinstance(data_product_info, Mapping):
                data_product_id = str(data_product_info.get("id") or "")
                data_product_port = str(data_product_info.get("port") or "")
                data_product_role = str(data_product_info.get("role") or "")
            else:
                if latest_event.get("data_product_id"):
                    data_product_id = str(latest_event.get("data_product_id"))
                if latest_event.get("data_product_port"):
                    data_product_port = str(latest_event.get("data_product_port"))
                if latest_event.get("data_product_role"):
                    data_product_role = str(latest_event.get("data_product_role"))

            violations = _extract_violation_count(details)

            record = DatasetRecord(
                contract_id,
                contract_version,
                dataset_name,
                dataset_version_value,
                status_value,
                details,
                run_type,
                violations,
                draft_contract_version=draft_version,
                scenario_key=scenario_key,
                data_product_id=data_product_id,
                data_product_port=data_product_port,
                data_product_role=data_product_role,
                observation_operation=observation_operation,
                observation_scope=observation_scope,
                observation_label=observation_label,
            )
            record.reason = reason

            dedup_key: Tuple[str, str, str, str] | None = None
            if (
                dataset_name
                and dataset_version_value
                and contract_id
                and contract_version
            ):
                dedup_key = (
                    dataset_name,
                    dataset_version_value,
                    contract_id,
                    contract_version,
                )

            if dedup_key is None:
                records.append(record)
            else:
                existing_index = record_indices.get(dedup_key)
                if existing_index is None:
                    record_indices[dedup_key] = len(records)
                    records.append(record)
                else:
                    records[existing_index] = record

    return records


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
    for cid in list_contract_ids():
        for ver in contract_versions(cid):
            try:
                contract = get_contract(cid, ver)
            except FileNotFoundError:
                continue
            server = (contract.servers or [None])[0]
            path = ""
            if server:
                parts = [part for part in (server.path, server.dataset) if part]
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
    for obj in contract.schema_ or []:
        object_name = str(obj.name or "")
        prefix = f"{object_name}." if object_name else ""
        for prop in obj.properties or []:
            field_name = str(prop.name or "")
            full_name = f"{prefix}{field_name}".strip(".")
            entries.append(
                {
                    "field": full_name,
                    "object": object_name,
                    "name": field_name,
                    "physicalType": prop.physicalType or "",
                    "logicalType": prop.logicalType or "",
                    "required": bool(prop.required),
                    "description": prop.description or "",
                    "businessName": prop.businessName or "",
                }
            )
    return entries


def _integration_data_product_links() -> Dict[str, List[Dict[str, Any]]]:
    """Return data-product associations grouped by contract identifier."""

    associations: Dict[str, List[Dict[str, Any]]] = {}
    for product in load_data_products():
        product_id = str(product.id or "")
        product_name = product.name or product_id
        product_status = product.status or ""
        product_status_label = product_status.replace("_", " ").title() if product_status else ""
        product_version = str(product.version or "")

        for direction, ports in (("input", product.input_ports), ("output", product.output_ports)):
            for port in ports:
                contract_id = port.contract_id or ""
                if not contract_id:
                    continue
                props = _port_custom_map(port)
                entry = {
                    "product_id": product_id,
                    "product_name": product_name,
                    "product_status": product_status,
                    "product_status_label": product_status_label,
                    "product_version": product_version,
                    "direction": direction,
                    "port_name": port.name or "",
                    "port_version": port.version or "",
                    "contract_version": port.version or "",
                    "dataset_id": props.get("dc43.dataset.id") or props.get("dc43.contract.ref"),
                    "stage_contract": props.get("dc43.stage.contract"),
                    "source_data_product": props.get("dc43.input.source_data_product"),
                    "source_output_port": props.get("dc43.input.source_output_port"),
                    "custom_properties": props,
                }
                associations.setdefault(str(contract_id), []).append(entry)

    return associations


def _data_product_description_text(product: OpenDataProductStandard) -> str:
    """Return a plain-text description for a data product."""

    description = product.description
    if isinstance(description, Mapping):
        return str(
            description.get("usage")
            or description.get("summary")
            or description.get("text")
            or ""
        )
    if isinstance(description, str):
        return description
    if isinstance(description, Description):
        return str(description.usage or "")
    return ""


def _contract_description_text(contract: OpenDataContractStandard) -> str:
    """Return the usage text from a contract description when available."""

    description = contract.description
    if isinstance(description, Mapping):
        return str(description.get("usage") or "")
    if isinstance(description, str):
        return description
    if isinstance(description, Description):
        return str(description.usage or "")
    return ""


def _integration_data_product_ports(
    product: OpenDataProductStandard,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return normalised port metadata for helper summaries."""

    inputs: List[Dict[str, Any]] = []
    outputs: List[Dict[str, Any]] = []
    for port in product.input_ports:
        props = _port_custom_map(port)
        contract_version = props.get("dc43.contract.version") or port.version
        inputs.append(
            {
                "name": port.name or "",
                "portName": port.name or "",
                "direction": "input",
                "contractId": port.contract_id or "",
                "contractVersion": contract_version or "",
                "portVersion": port.version or "",
                "sourceDataProduct": props.get("dc43.input.source_data_product") or "",
                "sourceOutputPort": props.get("dc43.input.source_output_port") or "",
                "customProperties": props,
            }
        )
    for port in product.output_ports:
        props = _port_custom_map(port)
        contract_version = props.get("dc43.contract.version") or port.version
        outputs.append(
            {
                "name": port.name or "",
                "portName": port.name or "",
                "direction": "output",
                "contractId": port.contract_id or "",
                "contractVersion": contract_version or "",
                "portVersion": port.version or "",
                "datasetId": props.get("dc43.dataset.id") or props.get("dc43.contract.ref") or "",
                "stageContract": props.get("dc43.stage.contract") or "",
                "customProperties": props,
            }
        )
    inputs.sort(key=lambda item: (item["contractId"], item["portName"]))
    outputs.sort(key=lambda item: (item["contractId"], item["portName"]))
    return {"inputs": inputs, "outputs": outputs}


def _integration_data_product_summary(
    product: OpenDataProductStandard,
) -> Dict[str, Any]:
    """Build a serialisable summary for a data product."""

    product_id = str(product.id or "")
    status = str(product.status or "")
    status_label = status.replace("_", " ").title() if status else ""
    version = str(product.version or "")
    ports = _integration_data_product_ports(product)
    tags = [str(tag) for tag in product.tags if str(tag)]
    try:
        versions = _sorted_versions(data_product_versions(product_id))
    except FileNotFoundError:
        versions = []
    except Exception:  # pragma: no cover - defensive guard for backend issues
        logger.exception("Failed to list versions for data product %s", product_id)
        versions = []
    if version and version not in versions:
        versions = _sorted_versions([*versions, version])
    latest_version = versions[-1] if versions else version
    description = _data_product_description_text(product)
    search_terms = [
        product_id,
        product.name or "",
        status,
        version,
        description,
        *tags,
    ]
    for entry in ports["inputs"] + ports["outputs"]:
        search_terms.extend(
            [
                entry.get("name", ""),
                entry.get("contractId", ""),
                entry.get("contractVersion", ""),
            ]
        )
    summary = {
        "id": product_id,
        "name": product.name or product_id,
        "status": status,
        "statusLabel": status_label,
        "version": version,
        "latestVersion": latest_version,
        "versions": versions,
        "description": description,
        "tags": tags,
        "ports": ports,
        "inputCount": len(ports["inputs"]),
        "outputCount": len(ports["outputs"]),
        "searchText": " ".join(str(term or "") for term in search_terms if term),
    }
    return summary


def _integration_data_product_catalog() -> List[Dict[str, Any]]:
    """Return metadata for all stored data products."""

    summaries: List[Dict[str, Any]] = []
    for product in load_data_products():
        try:
            summary = _integration_data_product_summary(product)
        except Exception:  # pragma: no cover - defensive guard for corrupt payloads
            logger.exception("Failed to summarise data product %s", product.id or "?")
            continue
        summaries.append(summary)
    summaries.sort(key=lambda item: item["id"])
    return summaries


async def _load_integration_data_product(
    product_id: str, version: str
) -> IntegrationDataProductContext:
    """Return data product details enriched for the integration helper."""

    selector = str(version or "").strip()
    if not selector or selector.lower() in {"latest", "newest"}:
        product = latest_data_product(product_id)
        if product is None:
            raise HTTPException(
                status_code=404, detail=f"Data product {product_id} has no versions"
            )
    else:
        try:
            product = get_data_product(product_id, selector)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    summary = _integration_data_product_summary(product)
    return IntegrationDataProductContext(data_product=product, summary=summary)


def _integration_catalog() -> List[Dict[str, Any]]:
    """Return basic metadata for all stored contracts."""

    catalog: List[Dict[str, Any]] = []
    product_links = _integration_data_product_links()
    for cid in sorted(list_contract_ids()):
        try:
            versions = contract_versions(cid)
        except FileNotFoundError:
            continue
        sorted_versions = _sorted_versions(versions)
        if not sorted_versions:
            continue
        latest_contract: Optional[OpenDataContractStandard] = None
        for version in reversed(sorted_versions):
            try:
                latest_contract = get_contract(cid, version)
                break
            except FileNotFoundError:
                continue
        description = ""
        status = ""
        name = ""
        if latest_contract is not None:
            name = latest_contract.name or ""
            description = _contract_description_text(latest_contract)
            status = latest_contract.status or ""

        linked_products = product_links.get(cid, [])
        product_terms = sorted(
            {
                term
                for entry in linked_products
                for term in (
                    entry.get("product_id"),
                    entry.get("product_name"),
                    entry.get("port_name"),
                )
                if term
            }
        )

        catalog.append(
            {
                "id": cid,
                "name": name or cid,
                "description": description,
                "versions": sorted_versions,
                "latestVersion": sorted_versions[-1],
                "status": status,
                "dataProducts": linked_products,
                "dataProductTerms": product_terms,
            }
        )
    return catalog


@dataclass
class IntegrationContractContext:
    """Container storing contract objects alongside serialized metadata."""

    contract: OpenDataContractStandard
    summary: Dict[str, Any]


@dataclass
class IntegrationDataProductContext:
    """Container storing data products alongside serialized metadata."""

    data_product: OpenDataProductStandard
    summary: Dict[str, Any]


async def _load_integration_contract(cid: str, ver: str) -> IntegrationContractContext:
    """Return the contract and summary information for helper endpoints."""

    try:
        contract = get_contract(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    expectations = await _expectation_predicates(contract)
    server_info = _server_details(contract)
    description = _contract_description_text(contract)
    schema_entries = _flatten_schema_entries(contract)
    product_links = _integration_data_product_links().get(cid, [])
    version_links = [
        entry
        for entry in product_links
        if not str(entry.get("contract_version") or "").strip()
        or str(entry.get("contract_version") or "").strip() == ver
    ]

    summary: Dict[str, Any] = {
        "id": cid,
        "version": ver,
        "name": contract.name or cid,
        "description": description,
        "server": jsonable_encoder(server_info) if server_info else None,
        "expectations": expectations,
        "schemaEntries": schema_entries,
        "fieldCount": len(schema_entries),
        "datasetId": (server_info.get("dataset_id") if server_info else contract.id or cid),
        "dataProducts": jsonable_encoder(version_links),
    }
    return IntegrationContractContext(contract=contract, summary=summary)


def _resolve_contract_version(cid: str, selector: str) -> str:
    """Return the contract version indicated by ``selector``."""

    selector_value = str(selector or "").strip()
    if not selector_value:
        raise HTTPException(status_code=422, detail="Version selector cannot be empty")
    if selector_value.lower() in {"latest", "newest"}:
        try:
            contract = latest_contract(cid)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if contract is None:
            raise HTTPException(status_code=404, detail=f"Contract {cid} has no versions")
        version_value = contract.version
        if not version_value:
            raise HTTPException(
                status_code=422,
                detail=f"Contract {cid} does not expose a version for selection {selector_value}",
            )
        return str(version_value)
    if selector_value.startswith("=="):
        candidate = selector_value[2:].strip()
        if not candidate:
            raise HTTPException(
                status_code=422,
                detail=f"Version selector {selector_value!r} must include a target version",
            )
        return candidate
    return selector_value


def _normalise_selection(entries: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Normalise payload selections including optional data-product bindings."""

    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        contract_payload = entry.get("contract") if isinstance(entry.get("contract"), Mapping) else {}
        cid = (
            contract_payload.get("contract_id")
            or contract_payload.get("id")
            or entry.get("contract_id")
            or entry.get("contractId")
            or entry.get("id")
        )
        selector = (
            contract_payload.get("version_selector")
            or contract_payload.get("versionSelector")
            or entry.get("version_selector")
            or entry.get("versionSelector")
        )
        ver = (
            entry.get("version")
            or entry.get("contract_version")
            or entry.get("contractVersion")
            or contract_payload.get("contract_version")
            or contract_payload.get("version")
        )
        if not cid:
            raise HTTPException(status_code=422, detail="contract_id is required")
        if not ver:
            if selector:
                ver = _resolve_contract_version(str(cid), str(selector))
            else:
                raise HTTPException(status_code=422, detail="version is required")
        payload: Dict[str, Any] = {"contract_id": str(cid), "version": str(ver)}
        if selector:
            payload["version_selector"] = str(selector)

        dp_payload = entry.get("data_product") if isinstance(entry.get("data_product"), Mapping) else {}
        if not dp_payload:
            dp_payload = {}

        dp_id = (
            dp_payload.get("id")
            or dp_payload.get("data_product")
            or entry.get("data_product_id")
        )
        port_name = (
            dp_payload.get("port")
            or dp_payload.get("port_name")
            or entry.get("port_name")
        )
        if dp_id or port_name:
            binding: Dict[str, Any] = {
                "product_id": str(dp_id) if dp_id else None,
                "product_name": dp_payload.get("product_name") or entry.get("product_name"),
                "product_version": dp_payload.get("product_version") or entry.get("product_version"),
                "product_status": dp_payload.get("product_status") or entry.get("product_status"),
                "product_status_label": dp_payload.get("product_status_label")
                or entry.get("product_status_label"),
                "direction": dp_payload.get("direction") or entry.get("direction"),
                "port_name": str(port_name) if port_name else None,
                "port_version": dp_payload.get("port_version") or entry.get("port_version"),
                "dataset_id": dp_payload.get("dataset_id") or entry.get("dataset_id"),
                "stage_contract": dp_payload.get("stage_contract") or entry.get("stage_contract"),
                "source_data_product": dp_payload.get("source_data_product") or entry.get("source_data_product"),
                "source_output_port": dp_payload.get("source_output_port") or entry.get("source_output_port"),
                "custom_properties": dp_payload.get("custom_properties")
                if isinstance(dp_payload.get("custom_properties"), Mapping)
                else entry.get("custom_properties"),
            }
            binding = {key: value for key, value in binding.items() if value is not None}
            if binding:
                payload["data_product"] = binding

        result.append(payload)
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

    data = dict(payload or {})
    raw_mode = data.get("mode")
    mode = str(raw_mode or "status").lower()
    if mode not in {"status", "strict"}:
        raise HTTPException(status_code=400, detail=f"Unsupported read strategy: {mode}")

    def _coerce_statuses(value: Any) -> Optional[tuple[str, ...]]:
        if value is None:
            return None
        if isinstance(value, str):
            candidates = [item.strip() for item in value.split(",")]
        else:
            candidates = [str(item).strip() for item in value if str(item).strip()]
        cleaned = tuple(item for item in candidates if item)
        return cleaned or None

    strategy: Dict[str, Any] = {"mode": mode}
    contract_statuses = _coerce_statuses(data.get("allowed_contract_statuses"))
    if contract_statuses is not None:
        strategy["allowed_contract_statuses"] = contract_statuses
    if "allow_missing_contract_status" in data:
        strategy["allow_missing_contract_status"] = bool(
            data.get("allow_missing_contract_status")
        )
    if "contract_status_case_insensitive" in data:
        strategy["contract_status_case_insensitive"] = bool(
            data.get("contract_status_case_insensitive")
        )
    failure_message = str(data.get("contract_status_failure_message") or "").strip()
    if failure_message:
        strategy["contract_status_failure_message"] = failure_message

    product_statuses = _coerce_statuses(data.get("allowed_data_product_statuses"))
    if product_statuses is not None:
        strategy["allowed_data_product_statuses"] = product_statuses
    if "allow_missing_data_product_status" in data:
        strategy["allow_missing_data_product_status"] = bool(
            data.get("allow_missing_data_product_status")
        )
    if "data_product_status_case_insensitive" in data:
        strategy["data_product_status_case_insensitive"] = bool(
            data.get("data_product_status_case_insensitive")
        )
    product_failure = str(data.get("data_product_status_failure_message") or "").strip()
    if product_failure:
        strategy["data_product_status_failure_message"] = product_failure
    if "enforce_data_product_status" in data:
        raw_enforce = data.get("enforce_data_product_status")
        if raw_enforce is None:
            strategy["enforce_data_product_status"] = None
        elif isinstance(raw_enforce, str):
            flag = raw_enforce.strip().lower()
            if flag in {"", "none"}:
                strategy["enforce_data_product_status"] = None
            elif flag in {"false", "0", "no"}:
                strategy["enforce_data_product_status"] = False
            else:
                strategy["enforce_data_product_status"] = True
        else:
            strategy["enforce_data_product_status"] = bool(raw_enforce)

    if "draft_on_violation" in data:
        strategy["draft_on_violation"] = bool(data.get("draft_on_violation"))
    bump = str(data.get("bump") or "").strip()
    if bump:
        strategy["bump"] = bump

    return strategy


def _normalise_write_strategy(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalise the requested write strategy."""

    data = dict(payload or {})
    mode_raw = data.get("mode")
    mode = str(mode_raw or "split").lower()
    if mode == "noop":
        strategy: Dict[str, Any] = {"mode": "noop"}
    else:
        include_valid = bool(data.get("include_valid", True))
        include_reject = bool(data.get("include_reject", True))
        if not include_valid and not include_reject:
            include_valid = True
        strategy = {
            "mode": mode,
            "include_valid": include_valid,
            "include_reject": include_reject,
        }
        if mode not in {"split", "strict"}:
            raise HTTPException(status_code=400, detail=f"Unsupported write strategy: {mode}")
        if mode == "strict":
            strategy["fail_on_warnings"] = bool(data.get("fail_on_warnings", False))

    def _coerce_statuses(value: Any) -> Optional[tuple[str, ...]]:
        if value is None:
            return None
        if isinstance(value, str):
            entries = [item.strip() for item in value.split(",")]
        else:
            entries = [str(item).strip() for item in value if str(item).strip()]
        cleaned = tuple(item for item in entries if item)
        return cleaned or None

    product_statuses = _coerce_statuses(data.get("allowed_data_product_statuses"))
    if product_statuses is not None:
        strategy["allowed_data_product_statuses"] = product_statuses
    if "allow_missing_data_product_status" in data:
        strategy["allow_missing_data_product_status"] = bool(
            data.get("allow_missing_data_product_status")
        )
    if "data_product_status_case_insensitive" in data:
        strategy["data_product_status_case_insensitive"] = bool(
            data.get("data_product_status_case_insensitive")
        )
    failure_message = str(data.get("data_product_status_failure_message") or "").strip()
    if failure_message:
        strategy["data_product_status_failure_message"] = failure_message
    if "enforce_data_product_status" in data:
        raw_enforce = data.get("enforce_data_product_status")
        if raw_enforce is None:
            strategy["enforce_data_product_status"] = None
        elif isinstance(raw_enforce, str):
            flag = raw_enforce.strip().lower()
            if flag in {"", "none"}:
                strategy["enforce_data_product_status"] = None
            elif flag in {"false", "0", "no"}:
                strategy["enforce_data_product_status"] = False
            else:
                strategy["enforce_data_product_status"] = True
        else:
            strategy["enforce_data_product_status"] = bool(raw_enforce)

    if "draft_on_violation" in data:
        strategy["draft_on_violation"] = bool(data.get("draft_on_violation"))
    bump = str(data.get("bump") or "").strip()
    if bump:
        strategy["bump"] = bump

    return strategy


def _spark_stub_for_selection(
    inputs: List[Dict[str, Any]],
    outputs: List[Dict[str, Any]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    *,
    read_strategy: Mapping[str, Any],
    write_strategy: Mapping[str, Any],
) -> str:
    """Return a Spark pipeline stub tailored to the selected contracts."""

    read_mode = str(read_strategy.get("mode") or "status").lower()
    write_mode = str(write_strategy.get("mode") or "split").lower()

    def _format_bool(flag: bool) -> str:
        return "True" if flag else "False"

    def _format_tuple(values: Iterable[str]) -> str:
        entries = [repr(str(value)) for value in values]
        if not entries:
            return "()"
        if len(entries) == 1:
            return f"({entries[0]},)"
        return f"({', '.join(entries)})"

    violation_imports: List[str] = []
    if outputs:
        if write_mode == "noop":
            violation_imports.append("NoOpWriteViolationStrategy")
        else:
            violation_imports.append("SplitWriteViolationStrategy")
            if write_mode == "strict":
                violation_imports.append("StrictWriteViolationStrategy")

    read_status_args: Dict[str, str] = {}
    contract_statuses = tuple(read_strategy.get("allowed_contract_statuses") or ())
    if contract_statuses:
        read_status_args["allowed_contract_statuses"] = _format_tuple(contract_statuses)
    if "allow_missing_contract_status" in read_strategy:
        read_status_args["allow_missing_contract_status"] = _format_bool(
            bool(read_strategy.get("allow_missing_contract_status"))
        )
    if "contract_status_case_insensitive" in read_strategy:
        read_status_args["contract_status_case_insensitive"] = _format_bool(
            bool(read_strategy.get("contract_status_case_insensitive"))
        )
    if read_strategy.get("contract_status_failure_message"):
        read_status_args["contract_status_failure_message"] = repr(
            str(read_strategy.get("contract_status_failure_message"))
        )

    product_statuses = tuple(read_strategy.get("allowed_data_product_statuses") or ())
    if product_statuses:
        read_status_args["allowed_data_product_statuses"] = _format_tuple(product_statuses)
    if "allow_missing_data_product_status" in read_strategy:
        read_status_args["allow_missing_data_product_status"] = _format_bool(
            bool(read_strategy.get("allow_missing_data_product_status"))
        )
    if "data_product_status_case_insensitive" in read_strategy:
        read_status_args["data_product_status_case_insensitive"] = _format_bool(
            bool(read_strategy.get("data_product_status_case_insensitive"))
        )
    if read_strategy.get("data_product_status_failure_message"):
        read_status_args["data_product_status_failure_message"] = repr(
            str(read_strategy.get("data_product_status_failure_message"))
        )

    io_imports = [
        "GovernanceSparkReadRequest",
        "GovernanceSparkWriteRequest",
        "read_with_governance",
        "write_with_governance",
    ]
    if read_status_args:
        io_imports.insert(0, "DefaultReadStatusStrategy")

    lines: List[str] = ["from pyspark.sql import SparkSession", "from dc43_integrations.spark.io import ("]
    for name in io_imports:
        lines.append(f"    {name},")
    lines.append(")")
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
            "from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient",
            "",
            "# Generated by the DC43 integration helper",
            'BASE_URL = "http://dc43-services"',
            "",
            "governance_client = RemoteGovernanceServiceClient(base_url=BASE_URL)",
            "",
            'spark = SparkSession.builder.appName("dc43-pipeline").getOrCreate()',
        ]
    )

    if read_status_args:
        lines.append("")
        lines.append("# Make contract/data-product status handling explicit for governed reads.")
        lines.append("read_status_strategy = DefaultReadStatusStrategy(")
        for key, value in read_status_args.items():
            lines.append(f"    {key}={value},")
        lines.append(")")

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

    read_context_overrides: List[tuple[str, str]] = []
    if read_strategy.get("draft_on_violation"):
        read_context_overrides.append(("draft_on_violation", "True"))
    if read_strategy.get("bump"):
        read_context_overrides.append(("bump", repr(str(read_strategy.get("bump")))))
    if product_statuses:
        read_context_overrides.append(
            ("allowed_data_product_statuses", _format_tuple(product_statuses))
        )
    if "allow_missing_data_product_status" in read_strategy:
        read_context_overrides.append(
            (
                "allow_missing_data_product_status",
                _format_bool(bool(read_strategy.get("allow_missing_data_product_status"))),
            )
        )
    if "data_product_status_case_insensitive" in read_strategy:
        read_context_overrides.append(
            (
                "data_product_status_case_insensitive",
                _format_bool(bool(read_strategy.get("data_product_status_case_insensitive"))),
            )
        )
    if read_strategy.get("data_product_status_failure_message"):
        read_context_overrides.append(
            (
                "data_product_status_failure_message",
                repr(str(read_strategy.get("data_product_status_failure_message"))),
            )
        )
    if "enforce_data_product_status" in read_strategy:
        enforce_value = read_strategy.get("enforce_data_product_status")
        if enforce_value is None:
            read_context_overrides.append(("enforce_data_product_status", "None"))
        else:
            read_context_overrides.append(
                (
                    "enforce_data_product_status",
                    _format_bool(bool(enforce_value)),
                )
            )

    input_vars: List[str] = []
    for index, entry in enumerate(inputs, start=1):
        key = (entry["contract_id"], entry["version"])
        ctx = context_map[key]
        summary = ctx.summary
        server_raw = summary.get("server") or {}
        server = dict(server_raw) if isinstance(server_raw, Mapping) else {}
        location = (
            server.get("dataset")
            or server.get("table")
            or server.get("path")
        )
        fmt = server.get("format")
        binding = entry.get("data_product") or {}
        base_name = _sanitise_identifier(summary["id"], f"input{index}")
        df_var = f"{base_name}_df"
        status_var = f"{base_name}_status"
        input_vars.append(df_var)

        lines.append("")
        lines.append(f"# Input: {summary['id']} {summary['version']} ({summary['datasetId']})")
        if location:
            lines.append(f"#   Location: {location}")
        if fmt:
            lines.append(f"#   Format: {fmt}")
        if entry.get("version_selector"):
            lines.append(f"#   Version selector: {entry['version_selector']}")
        if binding:
            product_id = binding.get("product_id")
            port_name = binding.get("port_name")
            direction = (binding.get("direction") or "output").lower()
            if product_id and port_name:
                lines.append(
                    f"#   Data product: {product_id} · {port_name} ({direction})"
                )
            elif product_id:
                lines.append(f"#   Data product: {product_id} ({direction})")
            if binding.get("product_status_label"):
                lines.append(
                    f"#   Data product status: {binding['product_status_label']}"
                )
            elif binding.get("product_status"):
                lines.append(
                    f"#   Data product status: {binding['product_status']}"
                )
            if binding.get("source_data_product") or binding.get("source_output_port"):
                source_product = binding.get("source_data_product") or "?"
                source_port = binding.get("source_output_port") or "?"
                lines.append(
                    f"#   Source: {source_product} · {source_port} (upstream)"
                )

        lines.append(f"{df_var}, {status_var} = read_with_governance(")
        lines.append("    spark,")
        lines.append("    GovernanceSparkReadRequest(")
        lines.append("        context={")
        lines.append("            \"contract\": {")
        lines.append(f"                \"contract_id\": {summary['id']!r},")
        lines.append(f"                \"contract_version\": {summary['version']!r},")
        lines.append("            },")

        if binding.get("product_id") or binding.get("port_name"):
            lines.append("            \"input_binding\": {")
            lines.append(
                f"                \"data_product\": {binding.get('product_id')!r},"
            )
            if binding.get("port_name"):
                lines.append(
                    f"                \"port_name\": {binding.get('port_name')!r},"
                )
            if binding.get("product_version"):
                lines.append(
                    f"                \"data_product_version\": {binding.get('product_version')!r},"
                )
            if binding.get("source_data_product"):
                lines.append(
                    f"                \"source_data_product\": {binding.get('source_data_product')!r},"
                )
            if binding.get("source_output_port"):
                lines.append(
                    f"                \"source_output_port\": {binding.get('source_output_port')!r},"
                )
            lines.append("            },")

        for key_name, value in read_context_overrides:
            lines.append(f"            \"{key_name}\": {value},")

        dataset_id_value = binding.get("dataset_id") or summary.get("datasetId")
        if dataset_id_value:
            lines.append(f"            \"dataset_id\": {dataset_id_value!r},")
        if fmt:
            lines.append(f"            \"dataset_format\": {fmt!r},")

        lines.append("        },")
        table_value = server.get("dataset") or server.get("table")
        path_value = server.get("path") if not table_value else None
        if table_value:
            lines.append(f"        table={table_value!r},")
        if path_value:
            lines.append(f"        path={path_value!r},")
        if fmt:
            lines.append(f"        format={fmt!r},")
        lines.append("    ),")
        lines.append("    governance_service=governance_client,")
        lines.append("    enforce=True,")
        lines.append("    auto_cast=True,")
        if read_status_args:
            lines.append("    status_strategy=read_status_strategy,")
        lines.append("    return_status=True,")
        lines.append(")")

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
        location = (
            server.get("dataset")
            or server.get("table")
            or server.get("path")
        )

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
        if entry.get("version_selector"):
            lines.append(f"#   Version selector: {entry['version_selector']}")
        if binding:
            product_id = binding.get("product_id")
            port_name = binding.get("port_name")
            if product_id and port_name:
                lines.append(f"#   Data product: {product_id} · {port_name} (output)")
            elif product_id:
                lines.append(f"#   Data product: {product_id} (output)")
            if binding.get("product_status_label"):
                lines.append(
                    f"#   Data product status: {binding['product_status_label']}"
                )
            elif binding.get("product_status"):
                lines.append(
                    f"#   Data product status: {binding['product_status']}"
                )
            if binding.get("stage_contract"):
                lines.append(f"#   Stage contract: {binding['stage_contract']}")
        write_lines: List[str] = [
            f"{validation_var}, {status_var} = write_with_governance(",
            "    df=transformed_df,  # TODO: replace with dataframe for this output",
            "    request=GovernanceSparkWriteRequest(",
            "        context={",
            "            \"contract\": {",
            f"                \"contract_id\": {summary['id']!r},",
            f"                \"contract_version\": {summary['version']!r},",
            "            },",
        ]
        binding = entry.get("data_product") or {}
        if binding.get("product_id") or binding.get("port_name"):
            write_lines.append("            \"output_binding\": {")
            write_lines.append(
                f"                \"data_product\": {binding.get('product_id')!r},"
            )
            if binding.get("port_name"):
                write_lines.append(
                    f"                \"port_name\": {binding.get('port_name')!r},"
                )
            if binding.get("product_version"):
                write_lines.append(
                    f"                \"data_product_version\": {binding.get('product_version')!r},"
                )
            write_lines.append("            },")

        write_context_overrides: List[tuple[str, str]] = []
        if write_strategy.get("draft_on_violation"):
            write_context_overrides.append(("draft_on_violation", "True"))
        if write_strategy.get("bump"):
            write_context_overrides.append(("bump", repr(str(write_strategy.get("bump")))))
        product_write_statuses = tuple(write_strategy.get("allowed_data_product_statuses") or ())
        if product_write_statuses:
            write_context_overrides.append(
                ("allowed_data_product_statuses", _format_tuple(product_write_statuses))
            )
        if "allow_missing_data_product_status" in write_strategy:
            write_context_overrides.append(
                (
                    "allow_missing_data_product_status",
                    _format_bool(bool(write_strategy.get("allow_missing_data_product_status"))),
                )
            )
        if "data_product_status_case_insensitive" in write_strategy:
            write_context_overrides.append(
                (
                    "data_product_status_case_insensitive",
                    _format_bool(bool(write_strategy.get("data_product_status_case_insensitive"))),
                )
            )
        if write_strategy.get("data_product_status_failure_message"):
            write_context_overrides.append(
                (
                    "data_product_status_failure_message",
                    repr(str(write_strategy.get("data_product_status_failure_message"))),
                )
            )
        if "enforce_data_product_status" in write_strategy:
            enforce_value = write_strategy.get("enforce_data_product_status")
            if enforce_value is None:
                write_context_overrides.append(("enforce_data_product_status", "None"))
            else:
                write_context_overrides.append(
                    (
                        "enforce_data_product_status",
                        _format_bool(bool(enforce_value)),
                    )
                )
        for key_name, value in write_context_overrides:
            write_lines.append(f"            \"{key_name}\": {value},")
        dataset_id_value = binding.get("dataset_id") or summary.get("datasetId")
        if dataset_id_value:
            write_lines.append(f"            \"dataset_id\": {dataset_id_value!r},")
        if fmt:
            write_lines.append(f"            \"dataset_format\": {fmt!r},")
        write_lines.append("        },")
        table_value = server.get("dataset") or server.get("table")
        path_value = server.get("path") if not table_value else None
        if table_value:
            write_lines.append(f"        table={table_value!r},")
        if path_value:
            write_lines.append(f"        path={path_value!r},")
        if fmt:
            write_lines.append(f"        format={fmt!r},")
        write_lines.extend(
            [
                "    ),",
                "    governance_service=governance_client,",
            ]
        )
        if outputs and write_mode != "noop":
            write_lines.append("    violation_strategy=write_strategy,")
        write_lines.extend(
            [
                "    return_status=True,",
                ")",
                "",
                f"if {status_var}:",
                f"    print(\"{summary['id']} governance status:\", {status_var}.status)",
                f"print(\"{summary['id']} write validation ok:\", {validation_var}.ok)",
            ]
        )
        lines.extend(write_lines)

    return "\n".join(lines).rstrip() + "\n"


def _read_strategy_notes(
    selections: List[Dict[str, Any]],
    context_map: Mapping[Tuple[str, str], IntegrationContractContext],
    strategy: Mapping[str, Any],
) -> List[Dict[str, str]]:
    """Describe how read strategies are applied for the helper UI."""

    mode = str(strategy.get("mode") or "status").lower()
    if mode == "strict":
        intro = (
            "read_with_governance(... return_status=True) enforces schema alignment and the stub "
            "raises a RuntimeError whenever validation verdicts are not OK."
        )
    else:
        intro = (
            "read_with_governance(... return_status=True) enforces schema alignment and logs non-OK "
            "statuses so orchestration can branch on data quality verdicts."
        )
    intro += " Non-active contract statuses raise unless the strategy explicitly allows them."

    contract_statuses = tuple(strategy.get("allowed_contract_statuses") or ())
    allow_missing_contract_status = strategy.get("allow_missing_contract_status")
    contract_case_insensitive = strategy.get("contract_status_case_insensitive")
    contract_failure_message = strategy.get("contract_status_failure_message")
    product_statuses = tuple(strategy.get("allowed_data_product_statuses") or ())
    allow_missing_product_status = strategy.get("allow_missing_data_product_status")
    product_case_insensitive = strategy.get("data_product_status_case_insensitive")
    product_failure_message = strategy.get("data_product_status_failure_message")
    draft_on_violation = bool(strategy.get("draft_on_violation"))
    bump = str(strategy.get("bump") or "").strip()
    enforce_product_status = strategy.get("enforce_data_product_status")

    status_clauses: List[str] = []
    if contract_statuses:
        status_clauses.append(
            "Allowed contract statuses: " + ", ".join(contract_statuses) + "."
        )
    if allow_missing_contract_status:
        status_clauses.append("Missing contract statuses are tolerated.")
    if contract_case_insensitive:
        status_clauses.append("Contract status comparisons are case-insensitive.")
    if contract_failure_message:
        status_clauses.append(
            f"Failure message override: {contract_failure_message!r}."
        )
    if product_statuses:
        status_clauses.append(
            "Allowed data-product statuses: " + ", ".join(product_statuses) + "."
        )
    if allow_missing_product_status:
        status_clauses.append("Missing data-product statuses are tolerated.")
    if product_case_insensitive:
        status_clauses.append("Data-product status comparisons are case-insensitive.")
    if product_failure_message:
        status_clauses.append(
            f"Data-product failure message override: {product_failure_message!r}."
        )
    if enforce_product_status is True:
        status_clauses.append("Data-product statuses are enforced by governance.")
    elif enforce_product_status is False:
        status_clauses.append(
            "Data-product statuses are advisory (enforce_data_product_status=False)."
        )
    if draft_on_violation:
        status_clauses.append("Violations request draft datasets for triage.")
    if bump:
        status_clauses.append(f"Version bump mode: {bump}.")

    if status_clauses:
        intro += " " + " ".join(status_clauses)
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
        binding = entry.get("data_product") or {}
        binding_clause = ""
        if binding:
            product_id = binding.get("product_name") or binding.get("product_id")
            port_name = binding.get("port_name")
            if product_id and port_name:
                binding_clause = f"Data product binding: {product_id} · {port_name}."
            elif product_id:
                binding_clause = f"Data product binding: {product_id}."
            if binding.get("product_status_label"):
                binding_clause += (
                    f" Status: {binding['product_status_label']}."
                )
            elif binding.get("product_status"):
                binding_clause += f" Status: {binding['product_status']}."
            if binding.get("source_data_product") or binding.get("source_output_port"):
                source_product = binding.get("source_data_product") or "?"
                source_port = binding.get("source_output_port") or "?"
                binding_clause += (
                    f" Upstream: {source_product} · {source_port}."
                )
        version_selector = entry.get("version_selector")
        version_clause = (
            f"Version selector: {version_selector}." if version_selector else ""
        )
        notes.append(
            {
                "title": f"{summary['id']} {summary['version']} read",
                "description": " ".join(
                    part
                    for part in (
                        location_clause,
                        predicate_clause,
                        action_clause,
                        binding_clause.strip(),
                        version_clause,
                    )
                    if part
                ),
            }
        )
    return notes


def _write_strategy_notes(
    selections: List[Dict[str, Any]],
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
                "write_with_governance(... return_status=True) records validation results and relays "
                "dataset versions to the governance client so each pipeline run is traceable."
                " By default non-active contracts are rejected; extend the contract-status options"
                " on your chosen strategy when drafts should be allowed."
            ),
        }
    ]

    product_statuses = tuple(strategy.get("allowed_data_product_statuses") or ())
    allow_missing_product_status = strategy.get("allow_missing_data_product_status")
    product_case_insensitive = strategy.get("data_product_status_case_insensitive")
    product_failure_message = strategy.get("data_product_status_failure_message")
    draft_on_violation = bool(strategy.get("draft_on_violation"))
    bump = str(strategy.get("bump") or "").strip()
    enforce_product_status = strategy.get("enforce_data_product_status")

    write_status_clauses: List[str] = []
    if product_statuses:
        write_status_clauses.append(
            "Allowed data-product statuses: " + ", ".join(product_statuses) + "."
        )
    if allow_missing_product_status:
        write_status_clauses.append("Missing data-product statuses are tolerated.")
    if product_case_insensitive:
        write_status_clauses.append(
            "Data-product status comparisons are case-insensitive."
        )
    if product_failure_message:
        write_status_clauses.append(
            f"Data-product failure message override: {product_failure_message!r}."
        )
    if enforce_product_status is True:
        write_status_clauses.append("Data-product statuses are enforced by governance.")
    elif enforce_product_status is False:
        write_status_clauses.append(
            "Data-product statuses are advisory (enforce_data_product_status=False)."
        )
    if draft_on_violation:
        write_status_clauses.append("Violations request draft datasets for triage.")
    if bump:
        write_status_clauses.append(f"Version bump mode: {bump}.")

    if write_status_clauses:
        notes.append(
            {
                "title": "Status handling",
                "description": " ".join(write_status_clauses),
            }
        )

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
        binding = entry.get("data_product") or {}
        binding_clause = ""
        if binding:
            product_id = binding.get("product_name") or binding.get("product_id")
            port_name = binding.get("port_name")
            if product_id and port_name:
                binding_clause = f"Data product binding: {product_id} · {port_name}."
            elif product_id:
                binding_clause = f"Data product binding: {product_id}."
            if binding.get("product_status_label"):
                binding_clause += (
                    f" Status: {binding['product_status_label']}."
                )
            elif binding.get("product_status"):
                binding_clause += f" Status: {binding['product_status']}."
            if binding.get("stage_contract"):
                binding_clause += f" Stage contract: {binding['stage_contract']}."
        version_selector = entry.get("version_selector")
        version_clause = (
            f"Version selector: {version_selector}." if version_selector else ""
        )
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
                        binding_clause.strip(),
                        version_clause,
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
        contract = get_contract(cid, ver)
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
    if (
        read_with_governance is None
        or ContractVersionLocator is None
        or GovernanceSparkReadRequest is None
    ):
        raise HTTPException(status_code=503, detail="pyspark is required for data previews")
    try:
        contract = get_contract(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    effective_dataset_id = str(dataset_id or contract.id or cid)
    server = (contract.servers or [None])[0]
    dataset_path_hint = server.path if server else None
    version_contract = contract if effective_dataset_id == (contract.id or cid) else None
    dataset_records = load_records(dataset_id=effective_dataset_id)
    scoped_records = [
        record
        for record in dataset_records
        if record.contract_id == cid
        and record.contract_version == ver
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
            _, _, governance_client = _thread_service_clients()
            spark = _spark_session()
            locator = ContractVersionLocator(
                dataset_version=selected_version,
                dataset_id=effective_dataset_id,
            )
            request = GovernanceSparkReadRequest(
                context={
                    "contract": {
                        "contract_id": cid,
                        "contract_version": ver,
                    }
                },
                dataset_locator=locator,
            )
            df = read_with_governance(  # type: ignore[misc]
                spark,
                request,
                governance_service=governance_client,
                enforce=False,
                auto_cast=False,
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

    status_lookup = {entry["version"]: entry for entry in version_records}
    status_entry = status_lookup.get(selected_version)
    status_value = str(status_entry.get("status", "unknown")) if status_entry else "unknown"
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
            "details": None,
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
            contract = get_contract(r.contract_id, r.contract_version)
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


@router.get("/api/integration-helper/data-products")
async def api_integration_data_products() -> Dict[str, Any]:
    """Return data-product metadata for the integration helper UI."""

    return {"data_products": _integration_data_product_catalog()}


@router.get("/api/integration-helper/contracts/{cid}/{ver}")
async def api_integration_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    """Return contract details enriched for the integration helper."""

    context = await _load_integration_contract(cid, ver)
    return {
        "contract": contract_to_dict(context.contract),
        "summary": jsonable_encoder(context.summary),
    }


@router.get("/api/integration-helper/data-products/{product_id}/{version}")
async def api_integration_data_product_detail(
    product_id: str, version: str
) -> Dict[str, Any]:
    """Return data-product details enriched for the integration helper."""

    context = await _load_integration_data_product(product_id, version)
    return {
        "data_product": jsonable_encoder(context.data_product.to_dict()),
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
        for module_key in SETUP_MODULES.keys():
            module_meta = SETUP_MODULES[module_key]
            field_name = f"module__{module_key}"
            raw_value = str(form.get(field_name) or "").strip()
            allowed_options = _module_visible_options(module_key, selections)
            hide_module = _module_should_hide(module_key, selections) or (
                module_meta.get("depends_on") and not allowed_options
            )

            if hide_module:
                default_option = _module_default_option(module_key, selections)
                if default_option:
                    selections[module_key] = default_option
                continue

            if allowed_options:
                if len(allowed_options) == 1:
                    selections[module_key] = allowed_options[0]
                    continue
                if raw_value and raw_value in allowed_options:
                    selections[module_key] = raw_value
                    continue
                default_option = _module_default_option(module_key, selections)
                if default_option and default_option in allowed_options:
                    selections[module_key] = default_option
                    continue
                errors.append(f"Select an option for {module_meta.get('title') or module_key}.")
                continue

            if raw_value and raw_value in module_meta["options"]:
                selections[module_key] = raw_value
                continue

            default_option = _module_default_option(module_key, selections)
            if default_option:
                selections[module_key] = default_option
            else:
                errors.append(f"Select an option for {module_meta.get('title') or module_key}.")

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


@router.get("/setup/export", response_class=StreamingResponse)
async def setup_export() -> StreamingResponse:
    """Return a ZIP archive with the current setup selections."""

    state = load_setup_state()
    selected_options = state.get("selected_options") if isinstance(state, Mapping) else {}
    if not isinstance(selected_options, Mapping) or not selected_options:
        raise HTTPException(
            status_code=400,
            detail="Select module implementations before exporting the configuration bundle.",
        )

    configuration = state.get("configuration") if isinstance(state, Mapping) else {}
    if not isinstance(configuration, Mapping):
        configuration = {}

    if _requires_configuration(selected_options, configuration):
        raise HTTPException(
            status_code=400,
            detail="Provide the required configuration values for each module before exporting.",
        )

    try:
        buffer, export_payload = _build_setup_bundle(state)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    generated_at = str(export_payload.get("generated_at") or datetime.utcnow().isoformat() + "Z")
    slug = re.sub(r"[^0-9A-Za-z]", "", generated_at)
    timestamp = slug or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"dc43-setup-{timestamp}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/integration-helper", response_class=HTMLResponse)
async def integration_helper(request: Request) -> HTMLResponse:
    """Render the contract integration helper interface."""

    context = {
        "request": request,
        "catalog": _integration_catalog(),
        "data_products": _integration_data_product_catalog(),
        "integration_options": [
            {"value": "spark", "label": "Spark (PySpark / Delta Lake)"},
        ],
    }
    return templates.TemplateResponse("integration_helper.html", context)


@router.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contract_ids = list_contract_ids()
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
            put_contract(model)
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
    versions = contract_versions(cid)
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
            contract = get_contract(cid, ver)
        except FileNotFoundError:
            continue

        status_raw = contract.status or "unknown"
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
        contract = get_contract(cid, ver)
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
    metrics_summary = _empty_metrics_summary()
    metrics_error: str | None = None

    if dataset_id:

        def _load_contract_metrics() -> Sequence[Mapping[str, object]]:
            _, _, governance_client = _thread_service_clients()
            kwargs: dict[str, object] = {
                "dataset_id": dataset_id,
                "contract_id": cid,
                "contract_version": ver,
            }
            return governance_client.get_metrics(**kwargs)

        try:
            metrics_records = await run_in_threadpool(_load_contract_metrics)
        except Exception as exc:  # pragma: no cover - defensive fallback when backend fails
            metrics_error = str(exc)
            logger.exception(
                "Failed to load governance metrics for contract %s:%s (dataset %s)",
                cid,
                ver,
                dataset_id,
            )
        else:
            metrics_summary = _summarise_metrics(metrics_records)
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
        "metrics_summary": metrics_summary,
        "metrics_error": metrics_error,
    }
    return templates.TemplateResponse("contract_detail.html", context)


def _next_version(ver: str) -> str:
    """Return the next semantic version for ``ver``.

    Draft-style suffixes (``-draft``/``-rc1``/...) are tolerated by the local
    :class:`SemVer` helper so we can bump the base version even when the string
    is not a valid :mod:`packaging` release identifier. If the value does not
    resemble a semantic version at all we fall back to returning it unchanged so
    the UI keeps working for bespoke schemes.
    """

    try:
        semver = SemVer.parse(ver)
    except ValueError:
        try:
            v = Version(ver)
        except InvalidVersion:
            return ver
        return f"{v.major}.{v.minor}.{v.micro + 1}"
    return str(semver.bump("patch"))


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
        elif isinstance(item, CustomProperty):
            key = item.property
            value = item.value
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
    examples = list(prop.examples or [])
    return {
        "name": prop.name or "",
        "physicalType": prop.physicalType or "",
        "description": prop.description or "",
        "businessName": prop.businessName or "",
        "logicalType": prop.logicalType or "",
        "logicalTypeOptions": _stringify_value(prop.logicalTypeOptions),
        "required": bool(prop.required),
        "unique": bool(prop.unique),
        "partitioned": bool(prop.partitioned),
        "primaryKey": bool(prop.primaryKey),
        "classification": prop.classification or "",
        "examples": "\n".join(str(item) for item in examples),
        "customProperties": _custom_properties_state(prop.customProperties),
        "quality": _quality_state(prop.quality),
    }


def _schema_object_state(obj: SchemaObject) -> Dict[str, Any]:
    properties = [
        _schema_property_state(prop)
        for prop in obj.properties or []
    ]
    return {
        "name": obj.name or "",
        "description": obj.description or "",
        "businessName": obj.businessName or "",
        "logicalType": obj.logicalType or "",
        "customProperties": _custom_properties_state(obj.customProperties),
        "quality": _quality_state(obj.quality),
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
    try:
        raw = server.model_dump(by_alias=True)
    except AttributeError:  # pragma: no cover - Pydantic v1 fallback
        raw = server.dict(by_alias=True)  # type: ignore[attr-defined]
    state = {
        "server": raw.get("server", "") or "",
        "type": raw.get("type", "") or "",
        "port": raw.get("port") or "",
    }
    for field, attr in _SERVER_FIELD_MAP.items():
        value = raw.get(field)
        if value in (None, ""):
            value = raw.get(attr)
        state[field] = value or ""
    versioning_value: Any | None = None
    path_pattern_value: Any | None = None
    custom_entries: List[Dict[str, str]] = []
    for item in normalise_custom_properties(server.customProperties):
        key = None
        value = None
        if isinstance(item, Mapping):
            key = item.get("property")
            value = item.get("value")
        elif isinstance(item, CustomProperty):
            key = item.property
            value = item.value
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
        try:
            raw = entry.model_dump(exclude_none=True)
        except AttributeError:  # pragma: no cover - Pydantic v1 fallback
            raw = entry.dict(exclude_none=True)  # type: ignore[attr-defined]
        payload = {
            field: raw[field]
            for field in ("channel", "url", "description", "tool", "scope", "invitationUrl")
            if raw.get(field) not in (None, "")
        }
        if payload:
            result.append(payload)
    return result


def _sla_state(items: Optional[Iterable[ServiceLevelAgreementProperty]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    if not items:
        return result
    for entry in items:
        try:
            raw = entry.model_dump(exclude_none=True)
        except AttributeError:  # pragma: no cover - Pydantic v1 fallback
            raw = entry.dict(exclude_none=True)  # type: ignore[attr-defined]
        payload: Dict[str, Any] = {}
        for field in ("property", "value", "valueExt", "unit", "element", "driver"):
            value = raw.get(field)
            if value is None:
                continue
            if field in {"value", "valueExt"}:
                payload[field] = _stringify_value(value)
            else:
                payload[field] = value
        if payload:
            result.append(payload)
    return result


def _data_product_editor_state(
    product: Optional[OpenDataProductStandard] = None,
) -> Dict[str, Any]:
    if product is None:
        return {
            "id": "",
            "version": "",
            "status": "",
            "name": "",
            "description": "",
            "tags": [],
            "customProperties": [],
            "inputPorts": [],
            "outputPorts": [],
        }
    description = ""
    if isinstance(product.description, Mapping):
        description = str(product.description.get("usage") or "")
    state = {
        "id": product.id or "",
        "version": product.version or "",
        "status": product.status or "",
        "name": product.name or "",
        "description": description,
        "tags": list(product.tags or []),
        "customProperties": _custom_properties_state(product.custom_properties),
        "inputPorts": [],
        "outputPorts": [],
    }
    for port in product.input_ports:
        props = _port_custom_map(port)
        state["inputPorts"].append(
            {
                "name": port.name or "",
                "contractId": port.contract_id or "",
                "contractVersion": port.version or "",
                "sourceDataProduct": props.get("dc43.input.source_data_product"),
                "sourceOutputPort": props.get("dc43.input.source_output_port"),
                "customProperties": _custom_properties_state(port.custom_properties),
            }
        )
    for port in product.output_ports:
        props = _port_custom_map(port)
        state["outputPorts"].append(
            {
                "name": port.name or "",
                "contractId": port.contract_id or "",
                "contractVersion": port.version or "",
                "datasetId": props.get("dc43.dataset.id") or props.get("dc43.contract.ref"),
                "stageContract": props.get("dc43.stage.contract"),
                "customProperties": _custom_properties_state(port.custom_properties),
            }
        )
    return state


def _data_product_editor_meta(
    *,
    editor_state: Mapping[str, Any],
    editing: bool,
    original_version: Optional[str],
    baseline_state: Optional[Mapping[str, Any]],
    baseline_product: Optional[OpenDataProductStandard],
) -> Dict[str, Any]:
    contract_ids = sorted(list_contract_ids())
    contract_versions_map: Dict[str, List[str]] = {}
    for contract_id in contract_ids:
        try:
            contract_versions_map[contract_id] = _sorted_versions(contract_versions(contract_id))
        except FileNotFoundError:
            contract_versions_map[contract_id] = []
    dataset_ids = sorted(list_dataset_ids())
    dataset_versions_map: Dict[str, List[str]] = {}
    for dataset_id in dataset_ids:
        versions = {
            str(entry.get("dataset_version") or "")
            for entry in dataset_pipeline_activity(dataset_id)
            if entry.get("dataset_version")
        }
        dataset_versions_map[dataset_id] = sorted(
            [version for version in versions if version],
            key=_version_sort_key,
        )
    existing_products = sorted(list_data_product_ids())
    product_versions_map: Dict[str, List[str]] = {}
    for product_id in existing_products:
        product_versions_map[product_id] = _sorted_versions(data_product_versions(product_id))
    meta: Dict[str, Any] = {
        "contractOptions": contract_ids,
        "contractVersions": contract_versions_map,
        "datasetOptions": dataset_ids,
        "datasetVersions": dataset_versions_map,
        "existingProducts": existing_products,
        "existingVersions": product_versions_map,
        "editing": editing,
        "productId": str(editor_state.get("id", "") or ""),
    }
    if original_version:
        meta["originalVersion"] = original_version
    if baseline_state is not None:
        meta["baselineState"] = jsonable_encoder(baseline_state)
    if baseline_product is not None:
        meta["baseProduct"] = baseline_product.to_dict()
    return meta


def _data_product_editor_context(
    request: Request,
    *,
    editor_state: Dict[str, Any],
    editing: bool = False,
    original_version: Optional[str] = None,
    baseline_state: Optional[Mapping[str, Any]] = None,
    baseline_product: Optional[OpenDataProductStandard] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    context = {
        "request": request,
        "editing": editing,
        "editor_state": editor_state,
        "status_options": _STATUS_OPTIONS,
        "editor_meta": _data_product_editor_meta(
            editor_state=editor_state,
            editing=editing,
            original_version=original_version,
            baseline_state=baseline_state,
            baseline_product=baseline_product,
        ),
    }
    if original_version:
        context["original_version"] = original_version
    if error:
        context["error"] = error
    return context


def _validate_data_product_payload(
    payload: Mapping[str, Any],
    *,
    editing: bool,
    base_version: Optional[str] = None,
) -> None:
    product_id = (str(payload.get("id", ""))).strip()
    if not product_id:
        raise ValueError("Data product ID is required")
    version_value = (str(payload.get("version", ""))).strip()
    if not version_value:
        raise ValueError("Version is required")
    try:
        new_version = SemVer.parse(version_value)
    except ValueError as exc:
        raise ValueError(f"Invalid version '{version_value}': {exc}") from exc
    if editing:
        if not base_version:
            raise ValueError("Base version is required when editing a data product")
        try:
            base_semver = SemVer.parse(base_version)
        except ValueError as exc:
            raise ValueError(f"Invalid base version '{base_version}': {exc}") from exc
        if new_version <= base_semver:
            raise ValueError("New version must be greater than the base version")


def _input_ports_from_payload(
    payload: Any,
    *,
    baseline: Optional[OpenDataProductStandard],
) -> List[DataProductInputPort]:
    ports: List[DataProductInputPort] = []
    baseline_map = {port.name: port for port in (baseline.input_ports if baseline else [])}

    items = payload if isinstance(payload, list) else []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        contract_id = (str(item.get("contractId", ""))).strip()
        version = (str(item.get("contractVersion", ""))).strip()
        if not name or not contract_id or not version:
            continue
        source_product = (str(item.get("sourceDataProduct", ""))).strip()
        source_port = (str(item.get("sourceOutputPort", ""))).strip()
        overrides = _custom_properties_entries(
            item.get("customProperties") if isinstance(item.get("customProperties"), list) else []
        )
        baseline_port = baseline_map.get(name)
        custom_entries = _merge_custom_entries(
            baseline_port.custom_properties if baseline_port else None,
            overrides,
        )

        def _apply(entries: List[Dict[str, Any]], key: str, value: str) -> List[Dict[str, Any]]:
            filtered = [entry for entry in entries if entry.get("property") != key]
            if value:
                filtered.append({"property": key, "value": value})
            return filtered

        custom_entries = _apply(custom_entries, "dc43.input.source_data_product", source_product)
        custom_entries = _apply(custom_entries, "dc43.input.source_output_port", source_port)

        ports.append(
            DataProductInputPort(
                name=name,
                version=version,
                contract_id=contract_id,
                custom_properties=custom_entries,
                authoritative_definitions=list(
                    baseline_port.authoritative_definitions if baseline_port else []
                ),
                extra=dict(baseline_port.extra if baseline_port else {}),
            )
        )
    return ports


def _output_ports_from_payload(
    payload: Any,
    *,
    baseline: Optional[OpenDataProductStandard],
) -> List[DataProductOutputPort]:
    ports: List[DataProductOutputPort] = []
    baseline_map = {port.name: port for port in (baseline.output_ports if baseline else [])}

    items = payload if isinstance(payload, list) else []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = (str(item.get("name", ""))).strip()
        contract_id = (str(item.get("contractId", ""))).strip()
        version = (str(item.get("contractVersion", ""))).strip()
        if not name or not contract_id or not version:
            continue
        dataset_id = (str(item.get("datasetId", ""))).strip()
        stage_contract = (str(item.get("stageContract", ""))).strip()
        overrides = _custom_properties_entries(
            item.get("customProperties") if isinstance(item.get("customProperties"), list) else []
        )
        baseline_port = baseline_map.get(name)
        custom_entries = _merge_custom_entries(
            baseline_port.custom_properties if baseline_port else None,
            overrides,
        )

        def _apply(entries: List[Dict[str, Any]], key: str, value: str) -> List[Dict[str, Any]]:
            filtered = [entry for entry in entries if entry.get("property") != key]
            if value:
                filtered.append({"property": key, "value": value})
            return filtered

        custom_entries = _apply(custom_entries, "dc43.dataset.id", dataset_id)
        if not dataset_id:
            custom_entries = [
                entry for entry in custom_entries if entry.get("property") != "dc43.dataset.id"
            ]
        custom_entries = _apply(custom_entries, "dc43.stage.contract", stage_contract)

        ports.append(
            DataProductOutputPort(
                name=name,
                version=version,
                contract_id=contract_id,
                description=baseline_port.description if baseline_port else None,
                type=baseline_port.type if baseline_port else None,
                sbom=list(baseline_port.sbom if baseline_port else []),
                input_contracts=list(baseline_port.input_contracts if baseline_port else []),
                custom_properties=custom_entries,
                authoritative_definitions=list(
                    baseline_port.authoritative_definitions if baseline_port else []
                ),
                extra=dict(baseline_port.extra if baseline_port else {}),
            )
        )
    return ports


def _build_data_product_from_payload(
    payload: Mapping[str, Any],
    *,
    baseline_product: Optional[OpenDataProductStandard] = None,
) -> OpenDataProductStandard:
    product_id = (str(payload.get("id", ""))).strip()
    version_value = (str(payload.get("version", ""))).strip()
    status_value = (str(payload.get("status", ""))).strip() or "draft"
    name_value = (str(payload.get("name", ""))).strip()
    description_text = (str(payload.get("description", ""))).strip()
    tags = _normalise_tags(payload.get("tags")) or []
    custom_entries = _custom_properties_entries(
        payload.get("customProperties") if isinstance(payload.get("customProperties"), list) else []
    )
    baseline_extra = dict(baseline_product.extra if baseline_product else {})
    input_ports = _input_ports_from_payload(
        payload.get("inputPorts"),
        baseline=baseline_product,
    )
    output_ports = _output_ports_from_payload(
        payload.get("outputPorts"),
        baseline=baseline_product,
    )
    description_payload = {"usage": description_text} if description_text else None
    return OpenDataProductStandard(
        id=product_id,
        status=status_value,
        api_version=(baseline_product.api_version if baseline_product else None)
        or ODPS_REQUIRED,
        kind=(baseline_product.kind if baseline_product else "DataProduct") or "DataProduct",
        version=version_value,
        name=name_value or None,
        description=description_payload,
        tags=tags,
        custom_properties=custom_entries,
        input_ports=input_ports,
        output_ports=output_ports,
        extra=baseline_extra,
    )


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
    description = _contract_description_text(contract)
    state = {
        "id": contract.id or "",
        "version": contract.version or "",
        "kind": contract.kind or "DataContract",
        "apiVersion": contract.apiVersion or "3.0.2",
        "name": contract.name or "",
        "description": description,
        "status": contract.status or "",
        "domain": contract.domain or "",
        "dataProduct": contract.dataProduct or "",
        "tenant": contract.tenant or "",
        "tags": list(contract.tags or []),
        "customProperties": _custom_properties_state(contract.customProperties),
        "servers": [_server_state(server) for server in contract.servers or []],
        "schemaObjects": [
            _schema_object_state(obj) for obj in contract.schema_ or []
        ],
        "support": _support_state(contract.support),
        "slaProperties": _sla_state(contract.slaProperties),
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
    existing_contracts = sorted(list_contract_ids())
    version_map: Dict[str, List[str]] = {}
    for contract_id in existing_contracts:
        try:
            versions = contract_versions(contract_id)
        except FileNotFoundError:
            versions = []
        version_map[contract_id] = _sorted_versions(versions)
    meta: Dict[str, Any] = {
        "existingContracts": existing_contracts,
        "existingVersions": version_map,
        "editing": editing,
        "originalVersion": original_version,
        "contractId": str(editor_state.get("id", ""))
        or (baseline_contract.id if baseline_contract else ""),
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


def _custom_properties_entries(items: Optional[Iterable[Mapping[str, Any]]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if not items:
        return entries
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = (str(item.get("property", ""))).strip()
        if not key:
            continue
        value = _parse_json_value(item.get("value"))
        entries.append({"property": key, "value": value})
    return entries


def _merge_custom_entries(
    base_entries: Iterable[Mapping[str, Any]] | None,
    overrides: Iterable[Mapping[str, Any]] | None,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Any] = {}
    if base_entries:
        for item in base_entries:
            if not isinstance(item, Mapping):
                continue
            key = (str(item.get("property", ""))).strip()
            if key:
                merged[key] = item.get("value")
    if overrides:
        for item in overrides:
            if not isinstance(item, Mapping):
                continue
            key = (str(item.get("property", ""))).strip()
            if key:
                merged[key] = item.get("value")
    return [{"property": key, "value": merged[key]} for key in merged]


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
    existing_contracts = set(list_contract_ids())
    existing_versions = (
        set(contract_versions(contract_id)) if contract_id in existing_contracts else set()
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
                prop.name for prop in properties if prop.name
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
        contract = get_contract(cid, ver)
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
        baseline_contract = get_contract(cid, base_version)
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
            put_contract(model)
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


def _dataset_history_sort_key(record: Mapping[str, Any]) -> Tuple[str, str, str]:
    return (
        str(record.get("dataset_version") or ""),
        str(record.get("contract_id") or ""),
        str(record.get("contract_version") or ""),
    )


def _sort_dataset_history_rows(records: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return sorted((dict(item) for item in records), key=_dataset_history_sort_key, reverse=True)


@router.get("/datasets/{dataset_name}", response_class=HTMLResponse)
async def dataset_versions(request: Request, dataset_name: str) -> HTMLResponse:
    records = _sort_dataset_history_rows(
        [r.__dict__.copy() for r in load_records(dataset_id=dataset_name)]
    )
    contract_filter_fallbacks = _contract_filter_fallbacks(records)
    metrics_summary = _empty_metrics_summary()
    metrics_error: str | None = None

    if dataset_name:

        def _load_metrics() -> Sequence[Mapping[str, object]]:
            _, _, governance_client = _thread_service_clients()
            return governance_client.get_metrics(dataset_id=dataset_name)

        try:
            metrics_records = await run_in_threadpool(_load_metrics)
        except Exception as exc:  # pragma: no cover - defensive
            metrics_error = str(exc)
            logger.exception("Failed to load dataset metrics for %s", dataset_name)
        else:
            metrics_summary = _summarise_metrics(
                metrics_records,
                fallback_contract_versions=contract_filter_fallbacks,
            )

    context = {
        "request": request,
        "dataset_name": dataset_name,
        "records": records,
        "metrics_summary": metrics_summary,
        "metrics_error": metrics_error,
        "show_metrics_history": True,
        "metrics_panel_title": "Dataset metrics",
        "metrics_panel_scope_label": f"Dataset {dataset_name}",
        "metrics_empty_message": "No metrics recorded for this dataset yet.",
    }
    return templates.TemplateResponse("dataset_versions.html", context)


@router.get("/data-products", response_class=HTMLResponse)
async def list_data_products(request: Request) -> HTMLResponse:
    records = load_records()
    catalog = data_product_catalog(records)
    context = {
        "request": request,
        "products": catalog,
        "can_manage_products": bool(data_product_service),
    }
    return templates.TemplateResponse("data_products.html", context)


@router.get("/data-products/new", response_class=HTMLResponse)
async def new_data_product_form(request: Request) -> HTMLResponse:
    if not data_product_service:
        raise HTTPException(status_code=503, detail="Data product backend is not configured")
    editor_state = _data_product_editor_state()
    editor_state["version"] = editor_state.get("version") or "0.1.0"
    editor_state["status"] = editor_state.get("status") or "draft"
    context = _data_product_editor_context(request, editor_state=editor_state)
    return templates.TemplateResponse("new_data_product.html", context)


@router.post("/data-products/new", response_class=HTMLResponse)
async def create_data_product(
    request: Request,
    payload: str = Form(...),
) -> HTMLResponse:
    if not data_product_service:
        raise HTTPException(status_code=503, detail="Data product backend is not configured")
    error: Optional[str] = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _data_product_editor_state()
        editor_state["version"] = editor_state.get("version") or "0.1.0"
        editor_state["status"] = editor_state.get("status") or "draft"
    else:
        try:
            _validate_data_product_payload(editor_state, editing=False)
            model = _build_data_product_from_payload(editor_state)
            put_data_product(model)
            return RedirectResponse(url="/data-products", status_code=303)
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _data_product_editor_context(
        request,
        editor_state=editor_state,
        error=error,
    )
    return templates.TemplateResponse("new_data_product.html", context)


@router.get("/data-products/{product_id}/{version}/edit", response_class=HTMLResponse)
async def edit_data_product_form(
    request: Request,
    product_id: str,
    version: str,
) -> HTMLResponse:
    if not data_product_service:
        raise HTTPException(status_code=503, detail="Data product backend is not configured")
    try:
        baseline_product = get_data_product(product_id, version)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    editor_state = _data_product_editor_state(baseline_product)
    baseline_state = json.loads(json.dumps(editor_state))
    editor_state["version"] = _next_version(version)
    context = _data_product_editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=version,
        baseline_state=baseline_state,
        baseline_product=baseline_product,
    )
    return templates.TemplateResponse("new_data_product.html", context)


@router.post("/data-products/{product_id}/{version}/edit", response_class=HTMLResponse)
async def save_data_product_edits(
    request: Request,
    product_id: str,
    version: str,
    payload: str = Form(...),
    original_version: str = Form(""),
) -> HTMLResponse:
    if not data_product_service:
        raise HTTPException(status_code=503, detail="Data product backend is not configured")
    base_version = original_version or version
    try:
        baseline_product = get_data_product(product_id, base_version)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    baseline_state = json.loads(json.dumps(_data_product_editor_state(baseline_product)))
    error: Optional[str] = None
    try:
        editor_state = json.loads(payload)
    except json.JSONDecodeError as exc:
        error = f"Invalid editor payload: {exc.msg}"
        editor_state = _data_product_editor_state(baseline_product)
        editor_state["version"] = _next_version(base_version)
    else:
        try:
            _validate_data_product_payload(
                editor_state,
                editing=True,
                base_version=base_version,
            )
            model = _build_data_product_from_payload(
                editor_state,
                baseline_product=baseline_product,
            )
            put_data_product(model)
            return RedirectResponse(url=f"/data-products/{product_id}", status_code=303)
        except (ValidationError, ValueError) as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - display unexpected errors
            error = str(exc)
    context = _data_product_editor_context(
        request,
        editor_state=editor_state,
        editing=True,
        original_version=base_version,
        baseline_state=baseline_state,
        baseline_product=baseline_product,
        error=error,
    )
    return templates.TemplateResponse("new_data_product.html", context)


@router.get("/data-products/{product_id}", response_class=HTMLResponse)
async def data_product_detail_view(request: Request, product_id: str) -> HTMLResponse:
    records = load_records()
    details = describe_data_product(product_id, records)
    if details is None:
        raise HTTPException(status_code=404, detail="Data product not found")
    context = {
        "request": request,
        "product": details,
        "can_manage_products": bool(data_product_service),
    }
    return templates.TemplateResponse("data_product_detail.html", context)


def _dataset_preview(
    contract: OpenDataContractStandard | None,
    dataset_name: str,
    dataset_version: str,
) -> str:
    """Return a placeholder preview.

    Materialised datasets are managed by external pipelines. The demo package
    overrides this helper with a filesystem-backed implementation that exposes
    sample data.
    """

    del contract, dataset_name, dataset_version
    return ""


def _data_products_payload_file() -> List[Mapping[str, Any]]:
    """Legacy fallback retained for the demo package."""

    return []


def load_data_products() -> List[OpenDataProductStandard]:
    documents: List[OpenDataProductStandard] = []
    for product_id in list_data_product_ids():
        try:
            product = latest_data_product(product_id)
        except Exception:  # pragma: no cover - defensive guard when backend fails
            logger.exception("Failed to load data product %s", product_id)
            continue
        if product is not None:
            documents.append(product)
    if documents:
        return documents

    fallback: List[OpenDataProductStandard] = []
    return fallback


def _port_custom_map(port: Any) -> Dict[str, Any]:
    if isinstance(port, (DataProductInputPort, DataProductOutputPort)):
        props = list(port.custom_properties or [])
    elif isinstance(port, Mapping):
        raw = port.get("custom_properties") or port.get("customProperties")
        props = list(raw or []) if isinstance(raw, list) else []
    else:
        props = []
    mapping: Dict[str, Any] = {}
    for entry in props:
        if isinstance(entry, Mapping):
            key = entry.get("property")
            if key:
                mapping[str(key)] = entry.get("value")
        elif isinstance(entry, CustomProperty):
            if entry.property:
                mapping[str(entry.property)] = entry.value
    return mapping


def _normalise_identifier(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text.lower()


def _data_product_port_index(
    products: Iterable[OpenDataProductStandard],
) -> Tuple[
    Dict[str, set[str]],
    Dict[Tuple[str, str], set[str]],
    Dict[str, set[str]],
]:
    dataset_index: Dict[str, set[str]] = {}
    contract_version_index: Dict[Tuple[str, str], set[str]] = {}
    contract_index: Dict[str, set[str]] = {}
    for product in products:
        for port in list(product.output_ports or []):
            props = _port_custom_map(port)
            dataset_ref = props.get("dc43.dataset.id") or props.get("dc43.contract.ref")
            dataset_key = _normalise_identifier(dataset_ref)
            if dataset_key:
                dataset_index.setdefault(dataset_key, set()).add(product.id)
            contract_key = _normalise_identifier(port.contract_id)
            version_key = _normalise_identifier(port.version)
            if contract_key:
                if version_key:
                    contract_version_index.setdefault(
                        (contract_key, version_key), set()
                    ).add(product.id)
                contract_index.setdefault(contract_key, set()).add(product.id)
    return dataset_index, contract_version_index, contract_index


def _pick_single_candidate(candidates: set[str] | None) -> str:
    if not candidates:
        return ""
    if len(candidates) == 1:
        return next(iter(candidates))
    return ""


def _infer_data_product_id(
    record: DatasetRecord,
    dataset_index: Mapping[str, set[str]],
    contract_version_index: Mapping[Tuple[str, str], set[str]],
    contract_index: Mapping[str, set[str]],
) -> str:
    dataset_key = _normalise_identifier(record.dataset_name)
    match = _pick_single_candidate(dataset_index.get(dataset_key))
    if match:
        return match
    contract_key = _normalise_identifier(record.contract_id)
    version_key = _normalise_identifier(record.contract_version)
    if contract_key and version_key:
        match = _pick_single_candidate(contract_version_index.get((contract_key, version_key)))
        if match:
            return match
    if contract_key:
        match = _pick_single_candidate(contract_index.get(contract_key))
        if match:
            return match
    return ""


def _records_by_data_product(
    records: Iterable[DatasetRecord],
    *,
    products: Iterable[OpenDataProductStandard] | None = None,
) -> Dict[str, List[DatasetRecord]]:
    product_docs = list(products) if products is not None else load_data_products()
    dataset_index, contract_version_index, contract_index = _data_product_port_index(
        product_docs
    )
    grouped: Dict[str, List[DatasetRecord]] = {}
    for record in records:
        product_id = record.data_product_id or _infer_data_product_id(
            record, dataset_index, contract_version_index, contract_index
        )
        if product_id:
            grouped.setdefault(product_id, []).append(record)
    for entries in grouped.values():
        entries.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
    return grouped


def data_product_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    products = load_data_products()
    grouped = _records_by_data_product(records, products=products)
    summaries: List[Dict[str, Any]] = []
    for product in products:
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
    product_docs = list(load_data_products())
    products = {doc.id: doc for doc in product_docs}
    product = products.get(product_id)
    if product is None:
        return None
    grouped = _records_by_data_product(records, products=product_docs)
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
    products = load_data_products()
    grouped_records = _records_by_data_product(records, products=products)
    for product in products:
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
    relevant_records = [r for r in records if r.dataset_name == dataset_name]
    if not relevant_records:
        return []

    by_contract: Dict[str, List[DatasetRecord]] = {}
    for record in relevant_records:
        if record.contract_id:
            by_contract.setdefault(record.contract_id, []).append(record)
    for contract_records in by_contract.values():
        contract_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))

    products = list(load_data_products())
    product_map = {doc.id: doc for doc in products if doc.id}
    summaries: Dict[tuple[str, str, str, str, str], Dict[str, Any]] = {}

    def _status_payload(record: DatasetRecord) -> tuple[str, str, str]:
        status_value = _normalise_record_status(record.status)
        return (
            status_value,
            status_value.replace("_", " ").title(),
            _DQ_STATUS_BADGES.get(status_value, "bg-secondary"),
        )

    def _update_summary(key: tuple[str, str, str, str, str], payload: Dict[str, Any]) -> None:
        existing = summaries.get(key)
        if existing is None:
            summaries[key] = payload
            return
        current_version = str(existing.get("latest_dataset_version") or "")
        candidate_version = str(payload.get("latest_dataset_version") or "")
        if _version_sort_key(candidate_version) >= _version_sort_key(current_version):
            summaries[key] = payload

    def _register_port(
        product: OpenDataProductStandard,
        port: DataProductInputPort | DataProductOutputPort,
        direction: str,
    ) -> None:
        contract_id = port.contract_id or ""
        if not contract_id:
            return
        contract_records = by_contract.get(contract_id)
        if not contract_records:
            return
        port_version = str(port.version or "")
        matching = [
            rec
            for rec in contract_records
            if not port_version or not rec.contract_version or rec.contract_version == port_version
        ]
        if not matching:
            matching = list(contract_records)
        latest = matching[-1]
        status_value, status_label, status_badge = _status_payload(latest)
        key = (
            product.id or "",
            port.name or "",
            direction,
            contract_id,
            port_version,
        )
        summaries[key] = {
            "product_id": product.id or "",
            "product_name": product.name or product.id or "",
            "direction": direction,
            "port_name": port.name or "",
            "port_version": port_version,
            "contract_id": contract_id,
            "contract_version": port_version or latest.contract_version or "",
            "latest_dataset_version": latest.dataset_version,
            "latest_status": status_value,
            "latest_status_label": status_label,
            "latest_status_badge": status_badge,
        }

    for product in products:
        for port in product.input_ports:
            _register_port(product, port, "input")
        for port in product.output_ports:
            _register_port(product, port, "output")

    for record in relevant_records:
        if not record.data_product_id:
            continue
        product_info = product_map.get(record.data_product_id)
        key = (
            record.data_product_id,
            record.data_product_port or "",
            record.data_product_role or "",
            record.contract_id or "",
            record.contract_version or "",
        )
        status_value, status_label, status_badge = _status_payload(record)
        payload = {
            "product_id": record.data_product_id,
            "product_name": (product_info.name if product_info and product_info.name else record.data_product_id),
            "direction": record.data_product_role or "",
            "port_name": record.data_product_port or "",
            "port_version": record.contract_version or "",
            "contract_id": record.contract_id or "",
            "contract_version": record.contract_version or "",
            "latest_dataset_version": record.dataset_version,
            "latest_status": status_value,
            "latest_status_label": status_label,
            "latest_status_badge": status_badge,
        }
        _update_summary(key, payload)

    ordered = list(summaries.values())
    ordered.sort(
        key=lambda item: (
            str(item.get("product_id") or ""),
            str(item.get("port_name") or ""),
            str(item.get("direction") or ""),
        )
    )
    return ordered



def dataset_catalog(records: Iterable[DatasetRecord]) -> List[Dict[str, Any]]:
    """Summarise known datasets and associated contract information."""

    grouped: Dict[str, Dict[str, Any]] = {}
    contract_datasets: Dict[str, set[str]] = {}
    for record in records:
        if not record.dataset_name:
            continue
        bucket = grouped.setdefault(
            record.dataset_name,
            {"dataset_name": record.dataset_name, "records": []},
        )
        bucket["records"].append(record)
        if record.contract_id:
            contract_datasets.setdefault(record.contract_id, set()).add(
                record.dataset_name
            )

    for contract_id in list_contract_ids():
        observed_dataset_names = sorted(
            name for name in contract_datasets.get(contract_id, set()) if name
        )
        for version in contract_versions(contract_id):
            try:
                contract = get_contract(contract_id, version)
            except FileNotFoundError:
                continue
            server_info = _server_details(contract) or {}
            dataset_targets = list(observed_dataset_names)
            if not dataset_targets:
                dataset_id = (
                    server_info.get("dataset_id")
                    or server_info.get("dataset")
                    or contract.id
                    or contract_id
                )
                if dataset_id:
                    dataset_targets = [dataset_id]
            for dataset_id in dataset_targets:
                bucket = grouped.setdefault(
                    dataset_id,
                    {"dataset_name": dataset_id, "records": []},
                )
                contracts_map = bucket.setdefault("contracts_by_id", {})
                contracts = contracts_map.setdefault(contract_id, [])
                contracts.append(
                    {
                        "version": version,
                        "status": contract.status or "",
                        "server": server_info,
                    }
                )

    catalog: List[Dict[str, Any]] = []
    for dataset_name, payload in grouped.items():
        dataset_records: List[DatasetRecord] = list(payload.get("records", []))
        dataset_records.sort(key=lambda item: _version_sort_key(item.dataset_version or ""))
        latest_record: Optional[DatasetRecord] = dataset_records[-1] if dataset_records else None

        latest_status_value: Optional[str] = None
        latest_status_label: str = ""
        latest_status_badge: str = ""
        latest_reason: Optional[str] = None
        latest_observation_label: str = ""
        latest_observation_scope: str = ""
        latest_observation_operation: str = ""
        if latest_record:
            status_raw = str(latest_record.status or "unknown")
            latest_status_value = status_raw.lower()
            latest_status_label = status_raw.replace("_", " ").title()
            latest_status_badge = _DQ_STATUS_BADGES.get(latest_status_value, "bg-secondary")
            latest_reason = latest_record.reason or None
            latest_observation_label = getattr(latest_record, "observation_label", "") or ""
            latest_observation_scope = getattr(latest_record, "observation_scope", "") or ""
            latest_observation_operation = (
                getattr(latest_record, "observation_operation", "") or ""
            )

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
                "latest_observation_label": latest_observation_label,
                "latest_observation_scope": latest_observation_scope,
                "latest_observation_operation": latest_observation_operation,
                "contract_summaries": contracts_summary,
                "run_drafts_count": len(run_drafts),
                "run_latest_draft_version": run_drafts[-1] if run_drafts else None,
            }
        )

    catalog.sort(key=lambda item: item["dataset_name"])
    return catalog


@router.get("/datasets/{dataset_name}/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_name: str, dataset_version: str) -> HTMLResponse:
    records = load_records(dataset_id=dataset_name)
    associations = data_products_for_dataset(dataset_name, records)
    for r in records:
        if r.dataset_name == dataset_name and r.dataset_version == dataset_version:
            contract_obj: OpenDataContractStandard | None = None
            if r.contract_id and r.contract_version:
                try:
                    contract_obj = get_contract(r.contract_id, r.contract_version)
                except FileNotFoundError:
                    contract_obj = None
            preview = _dataset_preview(contract_obj, dataset_name, dataset_version)
            metrics_summary = _empty_metrics_summary()
            metrics_error: str | None = None

            if r.dataset_name:

                def _load_metrics(
                    include_contract_filters: bool = True,
                ) -> Sequence[Mapping[str, object]]:
                    _, _, governance_client = _thread_service_clients()
                    kwargs: dict[str, object] = {
                        "dataset_id": r.dataset_name,
                    }
                    if r.dataset_version:
                        kwargs["dataset_version"] = r.dataset_version
                    if include_contract_filters:
                        if r.contract_id:
                            kwargs["contract_id"] = r.contract_id
                        if r.contract_version:
                            kwargs["contract_version"] = r.contract_version
                    return governance_client.get_metrics(**kwargs)

                try:
                    metrics_records = await run_in_threadpool(_load_metrics)
                    if (
                        not metrics_records
                        and r.dataset_version
                        and (r.contract_id or r.contract_version)
                    ):
                        metrics_records = await run_in_threadpool(
                            partial(_load_metrics, include_contract_filters=False)
                        )
                except Exception as exc:  # pragma: no cover - defensive fallback when backend fails
                    metrics_error = str(exc)
                    logger.exception(
                        "Failed to load governance metrics for %s@%s",
                        r.dataset_name,
                        r.dataset_version,
                    )
                else:
                    metrics_summary = _summarise_metrics(metrics_records)
            context = {
                "request": request,
                "record": r,
                "contract": contract_to_dict(contract_obj) if contract_obj else None,
                "data_preview": preview,
                "data_products": associations,
                "metrics_summary": metrics_summary,
                "metrics_error": metrics_error,
                "show_metrics_history": False,
                "metrics_panel_title": "Dataset metrics",
                "metrics_panel_scope_label": (
                    f"Dataset {dataset_name} {dataset_version}".strip()
                ),
                "metrics_empty_message": "No metrics recorded for this dataset version.",
            }
            return templates.TemplateResponse("dataset_detail.html", context)
    raise HTTPException(status_code=404, detail="Dataset not found")


def create_app() -> FastAPI:
    """Return a FastAPI application serving contract and dataset views."""

    application = FastAPI(title="dc43 app")

    @application.middleware("http")
    async def setup_guard(request: Request, call_next):  # type: ignore[override]
        docs_status = docs_chat.status()
        request.state.docs_chat_status = docs_status
        request.state.docs_chat_enabled = docs_status.enabled
        request.state.docs_chat_ready = docs_status.ready
        request.state.docs_chat_message = docs_status.message

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
    docs_chat.mount_gradio_app(application, path=docs_chat.GRADIO_MOUNT_PATH)
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
