from __future__ import annotations

"""FastAPI application exposing contract and dataset explorer views."""

import asyncio
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from httpx import ASGITransport
from packaging.version import InvalidVersion, Version

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.web import build_local_app
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients._http_sync import close_client

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
STATIC_DIR = BASE_DIR / "static"

if STATIC_DIR.is_dir():  # pragma: no cover - static assets optional for now
    static_files = StaticFiles(directory=str(STATIC_DIR))
else:  # pragma: no cover - default empty static handler
    static_files = StaticFiles(directory=str(BASE_DIR))

_CONTRACT_STATUS_BADGES = {
    "draft": "bg-warning text-dark",
    "active": "bg-success",
    "deprecated": "bg-secondary",
    "retired": "bg-dark",
    "suspended": "bg-danger",
    "unknown": "bg-secondary",
}

_DATASET_STATUS_BADGES = {
    "ok": "bg-success",
    "warn": "bg-warning text-dark",
    "block": "bg-danger",
    "unknown": "bg-secondary",
}

_backend_app: FastAPI | None = None
_backend_transport: ASGITransport | None = None
_backend_client: httpx.AsyncClient | None = None
contract_service: RemoteContractServiceClient
_governance_service: RemoteGovernanceServiceClient


def _client_base_url(base_url: str | None) -> str:
    if base_url:
        return base_url.rstrip("/")
    return "http://dc43-services"


def _close_backend_client() -> None:
    global _backend_client
    client = _backend_client
    if client is None:
        return
    close_client(client)
    _backend_client = None


def _initialise_backend(*, base_url: str | None = None) -> None:
    """Configure remote service clients for the portal."""

    global _backend_app, _backend_transport, _backend_client
    global contract_service, quality_service, _governance_service

    _close_backend_client()

    client_base_url = _client_base_url(base_url)

    if base_url:
        _backend_app = None
        _backend_transport = None
        _backend_client = httpx.AsyncClient(base_url=client_base_url)
    else:
        store_root = os.getenv("DC43_PORTAL_STORE")
        if store_root:
            store_path = store_root
        else:
            store_path = str(Path.cwd() / "contracts")
        store = FSContractStore(store_path)
        _backend_app = build_local_app(store)
        _backend_transport = ASGITransport(app=_backend_app)
        _backend_client = httpx.AsyncClient(
            base_url=client_base_url,
            transport=_backend_transport,
        )

    contract_service = RemoteContractServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )
    _governance_service = RemoteGovernanceServiceClient(
        base_url=client_base_url,
        client=_backend_client,
    )


_initialise_backend(base_url=os.getenv("DC43_PORTAL_BACKEND_URL"))


def _sort_versions(values: Iterable[str]) -> List[str]:
    items = [str(v) for v in values]

    def _key(value: str) -> tuple[int, Any]:
        try:
            return (0, Version(value))
        except InvalidVersion:
            return (1, value)

    return sorted(items, key=_key)


def _status_payload(status: str | None) -> Dict[str, str]:
    normalised = (status or "unknown").lower()
    return {
        "value": normalised,
        "label": normalised.replace("_", " ").title() or "Unknown",
        "badge": _CONTRACT_STATUS_BADGES.get(normalised, "bg-secondary"),
    }


def _dataset_status_payload(status: str | None) -> Dict[str, str]:
    normalised = (status or "unknown").lower()
    return {
        "value": normalised,
        "label": normalised.replace("_", " ").title() or "Unknown",
        "badge": _DATASET_STATUS_BADGES.get(normalised, "bg-secondary"),
    }


async def _latest_contract_status(contract_id: str, versions: Sequence[str]) -> Dict[str, str] | None:
    if not versions:
        return None
    latest_version = _sort_versions(versions)[-1]

    def _load() -> Optional[Mapping[str, Any]]:
        model = contract_service.get(contract_id, latest_version)
        return {
            "version": latest_version,
            "status": getattr(model, "status", None),
            "description": getattr(model, "description", ""),
        }

    payload = await asyncio.to_thread(_load)
    if payload is None:
        return None
    status_info = _status_payload(payload.get("status"))
    status_info["version"] = latest_version
    status_info["description"] = str(payload.get("description") or "")
    return status_info


async def _summarise_contract(contract_id: str) -> Mapping[str, Any]:
    versions = await asyncio.to_thread(contract_service.list_versions, contract_id)
    versions = _sort_versions(versions)
    latest = await _latest_contract_status(contract_id, versions)
    return {
        "id": contract_id,
        "versions": versions,
        "latest": latest,
    }


def _contract_status(contract: Mapping[str, Any]) -> Dict[str, str]:
    return _status_payload(str(contract.get("status", "")))


def _normalise_event(event: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(event)
    recorded_at = payload.get("recorded_at")
    if isinstance(recorded_at, str):
        try:
            payload["recorded_at"] = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        except ValueError:  # pragma: no cover - fallback for unexpected formats
            payload["recorded_at"] = recorded_at
    return payload


async def _dataset_activity_summary(dataset_id: str) -> Mapping[str, Any]:
    entries = await asyncio.to_thread(
        _governance_service.get_pipeline_activity, dataset_id=dataset_id
    )
    versions: List[str] = []
    latest_event: Dict[str, Any] | None = None
    for entry in entries:
        dataset_version = str(entry.get("dataset_version", ""))
        if dataset_version:
            versions.append(dataset_version)
        events = entry.get("events")
        if isinstance(events, list) and events:
            candidate = events[-1]
            if isinstance(candidate, Mapping):
                latest_event = dict(candidate)
    version_set = {v for v in versions if v}
    versions = _sort_versions(version_set)
    status_info = _dataset_status_payload(latest_event.get("dq_status") if latest_event else None)
    if latest_event and "recorded_at" in latest_event:
        status_info["recorded_at"] = latest_event["recorded_at"]
    return {
        "id": dataset_id,
        "versions": versions,
        "latest": status_info if versions else None,
    }


async def _dataset_version_activity(dataset_id: str, dataset_version: str) -> Mapping[str, Any]:
    entries = await asyncio.to_thread(
        _governance_service.get_pipeline_activity,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
    )
    if not entries:
        return {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "events": [],
        }
    record = dict(entries[0])
    raw_events = record.get("events")
    events: List[Dict[str, Any]] = []
    if isinstance(raw_events, list):
        for event in raw_events:
            if isinstance(event, Mapping):
                normalised = _normalise_event(event)
                status_info = _dataset_status_payload(normalised.get("dq_status"))
                normalised["status_info"] = status_info
                events.append(normalised)
    return {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "events": events,
        "contract_id": record.get("contract_id"),
        "contract_version": record.get("contract_version"),
    }


def create_app() -> FastAPI:
    app = FastAPI(title="DC43 Services Portal")

    if STATIC_DIR.is_dir():  # pragma: no cover - static assets optional
        app.mount("/static", static_files, name="static")

    @app.on_event("shutdown")
    async def shutdown_event() -> None:  # pragma: no cover - invoked by ASGI runtime
        _close_backend_client()

    @app.get("/", include_in_schema=False)
    async def index() -> RedirectResponse:
        return RedirectResponse(url="/contracts")

    @app.get("/contracts", response_class=HTMLResponse)
    async def list_contracts(request: Request) -> HTMLResponse:
        contract_ids = await asyncio.to_thread(contract_service.list_contracts)
        summaries: List[Mapping[str, Any]] = []
        for cid in _sort_versions(contract_ids):
            summaries.append(await _summarise_contract(cid))
        context = {"request": request, "contracts": summaries}
        return TEMPLATES.TemplateResponse("contracts.html", context)

    @app.get("/contracts/{contract_id}", response_class=HTMLResponse)
    async def contract_versions(request: Request, contract_id: str) -> HTMLResponse:
        versions = await asyncio.to_thread(contract_service.list_versions, contract_id)
        versions = _sort_versions(versions)
        contracts: List[Mapping[str, Any]] = []
        for version in versions:
            model = await asyncio.to_thread(contract_service.get, contract_id, version)
            payload = model.model_dump(by_alias=True, exclude_none=True)
            payload["status"] = _contract_status(payload)
            contracts.append(payload)
        context = {
            "request": request,
            "contract_id": contract_id,
            "contracts": contracts,
        }
        return TEMPLATES.TemplateResponse("contract_versions.html", context)

    @app.get("/contracts/{contract_id}/{contract_version}", response_class=HTMLResponse)
    async def contract_detail(
        request: Request,
        contract_id: str,
        contract_version: str,
    ) -> HTMLResponse:
        try:
            contract = await asyncio.to_thread(contract_service.get, contract_id, contract_version)
        except Exception as exc:  # pragma: no cover - propagated as HTTP error
            raise HTTPException(status_code=404, detail=str(exc))
        payload = contract.model_dump(by_alias=True, exclude_none=True)
        payload["status_info"] = _contract_status(payload)
        servers = payload.get("servers")
        if isinstance(servers, list):
            payload["servers"] = [dict(server) for server in servers if isinstance(server, Mapping)]
        schema_objects = payload.get("schemaObjects")
        if isinstance(schema_objects, list):
            payload["schemaObjects"] = [
                dict(obj) for obj in schema_objects if isinstance(obj, Mapping)
            ]
        context = {
            "request": request,
            "contract": payload,
        }
        return TEMPLATES.TemplateResponse("contract_detail.html", context)

    @app.get("/datasets", response_class=HTMLResponse)
    async def list_datasets(request: Request) -> HTMLResponse:
        dataset_ids = await asyncio.to_thread(_governance_service.list_datasets)
        summaries: List[Mapping[str, Any]] = []
        for dataset_id in sorted(set(dataset_ids)):
            summaries.append(await _dataset_activity_summary(dataset_id))
        context = {"request": request, "datasets": summaries}
        return TEMPLATES.TemplateResponse("datasets.html", context)

    @app.get("/datasets/{dataset_id}", response_class=HTMLResponse)
    async def dataset_versions(request: Request, dataset_id: str) -> HTMLResponse:
        entries = await asyncio.to_thread(
            _governance_service.get_pipeline_activity, dataset_id=dataset_id
        )
        versions: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for entry in entries:
            version = str(entry.get("dataset_version", ""))
            if not version:
                continue
            events = entry.get("events")
            if isinstance(events, list) and events:
                latest = events[-1]
                if isinstance(latest, Mapping):
                    versions[version] = {
                        "status": _dataset_status_payload(latest.get("dq_status")),
                        "event": latest,
                    }
        ordered = [
            {
                "dataset_version": version,
                "status": data.get("status"),
            }
            for version, data in sorted(versions.items(), key=lambda item: _key_version(item[0]))
        ]
        context = {
            "request": request,
            "dataset_id": dataset_id,
            "versions": ordered,
        }
        return TEMPLATES.TemplateResponse("dataset_versions.html", context)

    def _key_version(value: str) -> tuple[int, Any]:  # helper scoped for dataset_versions
        try:
            return (0, Version(value))
        except InvalidVersion:
            return (1, value)

    @app.get(
        "/datasets/{dataset_id}/{dataset_version}",
        response_class=HTMLResponse,
    )
    async def dataset_detail(
        request: Request,
        dataset_id: str,
        dataset_version: str,
    ) -> HTMLResponse:
        record = await _dataset_version_activity(dataset_id, dataset_version)
        if not record["events"]:
            raise HTTPException(status_code=404, detail="Dataset activity not found")
        linked_contract = await asyncio.to_thread(
            _governance_service.get_linked_contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        context = {
            "request": request,
            "record": record,
            "status": _dataset_status_payload(
                record["events"][-1].get("dq_status") if record["events"] else None
            ),
            "linked_contract": linked_contract,
        }
        return TEMPLATES.TemplateResponse("dataset_detail.html", context)

    return app


app = create_app()

