from __future__ import annotations

"""FastAPI application exposing contract and dataset explorer views."""

import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from httpx import ASGITransport
from packaging.version import InvalidVersion, Version

from dc43.odcs import custom_properties_dict
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.web import build_local_app
from dc43_service_clients.contracts.client.remote import RemoteContractServiceClient
from dc43_service_clients.governance.client.remote import RemoteGovernanceServiceClient
from dc43_service_clients._http_sync import close_client
from dc43.portal_app.editor import (
    ValidationError,
    build_contract_from_payload,
    contract_editor_state,
    editor_context,
    next_version,
    validate_contract_payload,
)
from open_data_contract_standard.model import (  # type: ignore
    DataQuality,
    OpenDataContractStandard,
    Server,
)

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

_STATUS_BADGES = {
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

_backend_app: FastAPI | None = None
_backend_transport: ASGITransport | None = None
_backend_client: httpx.AsyncClient | None = None
_contract_store: FSContractStore | None = None
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

    global _backend_app, _backend_transport, _backend_client, _contract_store
    global contract_service, _governance_service

    _close_backend_client()

    client_base_url = _client_base_url(base_url)

    if base_url:
        _backend_app = None
        _backend_transport = None
        _backend_client = httpx.AsyncClient(base_url=client_base_url)
        _contract_store = None
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
        _contract_store = store

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


def _key_version(value: str) -> tuple[int, Any]:
    try:
        return (0, Version(value))
    except InvalidVersion:
        return (1, value)


def _require_store() -> FSContractStore:
    store = _contract_store
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Contract editing is unavailable with the configured backend.",
        )
    return store


def _dataset_status_payload(status: str | None) -> Dict[str, str]:
    normalised = (status or "unknown").lower()
    return {
        "value": normalised,
        "label": normalised.replace("_", " ").title() or "Unknown",
        "badge": _DATASET_STATUS_BADGES.get(normalised, "bg-secondary"),
    }


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


def _server_details(contract: OpenDataContractStandard) -> Optional[Dict[str, Any]]:
    if not contract.servers:
        return None
    first = contract.servers[0]
    custom = custom_properties_dict(first)
    dataset_id = contract.id or getattr(first, "dataset", None) or ""
    info: Dict[str, Any] = {
        "server": getattr(first, "server", "") or "",
        "type": getattr(first, "type", "") or "",
        "format": getattr(first, "format", "") or "",
        "path": getattr(first, "path", "") or "",
        "dataset": getattr(first, "dataset", "") or "",
        "dataset_id": dataset_id or getattr(first, "dataset", ""),
    }
    if custom:
        info["custom"] = custom
        if "dc43.versioning" in custom:
            info["versioning"] = custom.get("dc43.versioning")
        if "dc43.pathPattern" in custom:
            info["path_pattern"] = custom.get("dc43.pathPattern")
    return info


def _quality_rule_summary(dq: DataQuality) -> Dict[str, Any]:
    conditions: List[str] = []
    if dq.description:
        conditions.append(str(dq.description))
    if dq.mustBeGreaterThan is not None:
        conditions.append(f"Value must be greater than {dq.mustBeGreaterThan}")
    if dq.mustBeGreaterOrEqualTo is not None:
        conditions.append(
            f"Value must be greater than or equal to {dq.mustBeGreaterOrEqualTo}"
        )
    if dq.mustBeLessThan is not None:
        conditions.append(f"Value must be less than {dq.mustBeLessThan}")
    if dq.mustBeLessOrEqualTo is not None:
        conditions.append(
            f"Value must be less than or equal to {dq.mustBeLessOrEqualTo}"
        )
    if dq.mustBeBetween:
        low, high = dq.mustBeBetween
        conditions.append(f"Value must be between {low} and {high}")
    if dq.mustNotBeBetween:
        low, high = dq.mustNotBeBetween
        conditions.append(f"Value must not be between {low} and {high}")
    if dq.mustBe is not None:
        conditions.append(f"Value must be {dq.mustBe}")
    if dq.mustNotBe is not None:
        conditions.append(f"Value must not be {dq.mustNotBe}")
    title = getattr(dq, "title", None)
    fallback = getattr(dq, "rule", None) or getattr(dq, "name", None)
    return {
        "title": (title or fallback or "Rule"),
        "conditions": conditions or ["Condition not specified"],
        "severity": dq.severity or "",
        "dimension": dq.dimension or "",
    }


def _field_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        for prop in getattr(obj, "properties", []) or []:
            quality_rules = [
                _quality_rule_summary(rule)
                for rule in getattr(prop, "quality", []) or []
                if isinstance(rule, DataQuality)
            ]
            sections.append(
                {
                    "name": getattr(prop, "name", ""),
                    "type": getattr(prop, "physicalType", ""),
                    "required": bool(getattr(prop, "required", False)),
                    "rules": quality_rules,
                }
            )
    return sections


def _dataset_quality_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        section_rules = [
            _quality_rule_summary(rule)
            for rule in getattr(obj, "quality", []) or []
            if isinstance(rule, DataQuality)
        ]
        if section_rules:
            sections.append({"name": getattr(obj, "name", ""), "rules": section_rules})
    return sections


def _schema_sections(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    for obj in contract.schema_ or []:
        properties: List[Dict[str, Any]] = []
        for prop in getattr(obj, "properties", []) or []:
            properties.append(
                {
                    "name": getattr(prop, "name", ""),
                    "physicalType": getattr(prop, "physicalType", ""),
                    "required": bool(getattr(prop, "required", False)),
                }
            )
        sections.append({"name": getattr(obj, "name", ""), "properties": properties})
    return sections


def _compatibility_entries(dataset_id: str, records: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for record in records:
        version = str(record.get("dataset_version", ""))
        status = str(record.get("status", "unknown") or "unknown")
        entries.append(
            {
                "version": version,
                "status": status,
                "status_label": status.replace("_", " ").title(),
                "badge": _DATASET_STATUS_BADGES.get(status, "bg-secondary"),
            }
        )
    entries.sort(key=lambda item: _key_version(item["version"]))
    return entries


def _contract_json(contract: OpenDataContractStandard) -> str:
    return contract.model_dump_json(indent=2, by_alias=True, exclude_none=True)


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
        ordered = sorted({str(cid) for cid in contract_ids})
        context = {"request": request, "contracts": ordered}
        return TEMPLATES.TemplateResponse("contracts.html", context)

    @app.get("/contracts/new", response_class=HTMLResponse)
    async def new_contract_form(request: Request) -> HTMLResponse:
        store = _require_store()
        editor_state = contract_editor_state()
        editor_state["version"] = editor_state.get("version") or "1.0.0"
        context = editor_context(
            request,
            store=store,
            editor_state=editor_state,
        )
        return TEMPLATES.TemplateResponse("new_contract.html", context)

    @app.post("/contracts/new", response_class=HTMLResponse)
    async def create_contract(request: Request, payload: str = Form(...)) -> HTMLResponse:
        store = _require_store()
        error: Optional[str] = None
        try:
            editor_state = json.loads(payload)
        except json.JSONDecodeError as exc:
            error = f"Invalid editor payload: {exc.msg}"
            editor_state = contract_editor_state()
            editor_state["version"] = editor_state.get("version") or "1.0.0"
        else:
            try:
                validate_contract_payload(editor_state, store=store, editing=False)
                model = build_contract_from_payload(editor_state)
                await asyncio.to_thread(store.put, model)
                return RedirectResponse(url="/contracts", status_code=303)
            except (ValidationError, ValueError) as exc:
                error = str(exc)
            except Exception as exc:  # pragma: no cover - defensive logging of unexpected issues
                error = str(exc)
        context = editor_context(
            request,
            store=store,
            editor_state=editor_state,
            error=error,
        )
        return TEMPLATES.TemplateResponse("new_contract.html", context)

    @app.get(
        "/contracts/{contract_id}/{contract_version}/edit",
        response_class=HTMLResponse,
    )
    async def edit_contract_form(
        request: Request, contract_id: str, contract_version: str
    ) -> HTMLResponse:
        store = _require_store()
        try:
            contract = await asyncio.to_thread(store.get, contract_id, contract_version)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        editor_state = contract_editor_state(contract)
        baseline_state = json.loads(json.dumps(editor_state))
        editor_state["version"] = next_version(contract_version)
        context = editor_context(
            request,
            store=store,
            editor_state=editor_state,
            editing=True,
            original_version=contract_version,
            baseline_state=baseline_state,
            baseline_contract=contract,
        )
        return TEMPLATES.TemplateResponse("new_contract.html", context)

    @app.post(
        "/contracts/{contract_id}/{contract_version}/edit",
        response_class=HTMLResponse,
    )
    async def save_contract_edits(
        request: Request,
        contract_id: str,
        contract_version: str,
        payload: str = Form(...),
        original_version: str = Form(""),
    ) -> HTMLResponse:
        store = _require_store()
        error: Optional[str] = None
        base_version = original_version or contract_version
        baseline_contract: Optional[OpenDataContractStandard] = None
        baseline_state: Optional[Dict[str, Any]] = None
        try:
            baseline_contract = await asyncio.to_thread(store.get, contract_id, base_version)
            baseline_state = json.loads(json.dumps(contract_editor_state(baseline_contract)))
        except FileNotFoundError:
            baseline_contract = None
            baseline_state = None
        try:
            editor_state = json.loads(payload)
        except json.JSONDecodeError as exc:
            error = f"Invalid editor payload: {exc.msg}"
            editor_state = contract_editor_state()
            editor_state["id"] = contract_id
            editor_state["version"] = next_version(contract_version)
        else:
            try:
                validate_contract_payload(
                    editor_state,
                    store=store,
                    editing=True,
                    base_contract_id=contract_id,
                    base_version=base_version,
                )
                model = build_contract_from_payload(editor_state)
                await asyncio.to_thread(store.put, model)
                return RedirectResponse(
                    url=f"/contracts/{model.id}/{model.version}",
                    status_code=303,
                )
            except (ValidationError, ValueError) as exc:
                error = str(exc)
            except Exception as exc:  # pragma: no cover - defensive logging of unexpected issues
                error = str(exc)
        context = editor_context(
            request,
            store=store,
            editor_state=editor_state,
            editing=True,
            original_version=base_version,
            baseline_state=baseline_state,
            baseline_contract=baseline_contract,
            error=error,
        )
        return TEMPLATES.TemplateResponse("new_contract.html", context)

    @app.get("/contracts/{contract_id}", response_class=HTMLResponse)
    async def contract_versions(request: Request, contract_id: str) -> HTMLResponse:
        versions = await asyncio.to_thread(contract_service.list_versions, contract_id)
        versions = _sort_versions(versions)
        contracts: List[Mapping[str, Any]] = []
        dataset_id: Optional[str] = None
        dataset_activity: List[Mapping[str, Any]] = []
        if versions:
            first = await asyncio.to_thread(contract_service.get, contract_id, versions[-1])
            server_info = _server_details(first)
            dataset_id = server_info.get("dataset_id") if server_info else contract_id
            if dataset_id:
                dataset_activity = list(
                    await asyncio.to_thread(
                        _governance_service.get_pipeline_activity, dataset_id=dataset_id
                    )
                )
        for version in versions:
            model = await asyncio.to_thread(contract_service.get, contract_id, version)
            status_value = str(getattr(model, "status", "unknown") or "unknown").lower()
            status_payload = {
                "status": status_value,
                "status_label": status_value.replace("_", " ").title() or "Unknown",
                "badge": _CONTRACT_STATUS_BADGES.get(status_value, "bg-secondary"),
            }
            server_info = _server_details(model)
            latest_run: Optional[Dict[str, Any]] = None
            if dataset_id:
                for entry in dataset_activity:
                    if str(entry.get("contract_version", "")) != version:
                        continue
                    dataset_version = str(entry.get("dataset_version", ""))
                    events = entry.get("events")
                    status = "unknown"
                    if isinstance(events, list) and events:
                        latest_event = events[-1]
                        if isinstance(latest_event, Mapping):
                            status = str(latest_event.get("dq_status", "unknown") or "unknown")
                    latest_run = {
                        "dataset_name": dataset_id,
                        "dataset_version": dataset_version,
                        "status": status,
                    }
                    break
            contracts.append(
                {
                    "version": version,
                    "status_badge": status_payload["badge"],
                    "status_label": status_payload["status_label"],
                    "server": server_info,
                    "dataset_hint": server_info.get("dataset") if server_info else "",
                    "latest_run": latest_run,
                }
            )
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
        server_info = _server_details(contract)
        dataset_id = server_info.get("dataset_id") if server_info else contract.id or contract_id
        dataset_activity: List[Mapping[str, Any]] = []
        if dataset_id:
            dataset_activity = list(
                await asyncio.to_thread(
                    _governance_service.get_pipeline_activity, dataset_id=dataset_id
                )
            )
        dataset_records: List[Dict[str, Any]] = []
        compatibility_source: List[Mapping[str, Any]] = []
        for entry in dataset_activity:
            dataset_version = str(entry.get("dataset_version", ""))
            events = entry.get("events")
            status_value = "unknown"
            recorded_at: Optional[str] = None
            if isinstance(events, list) and events:
                latest_event = events[-1]
                if isinstance(latest_event, Mapping):
                    status_value = str(latest_event.get("dq_status", "unknown") or "unknown")
                    recorded_at = latest_event.get("recorded_at") or latest_event.get("timestamp")
            dataset_records.append(
                {
                    "dataset_name": dataset_id,
                    "dataset_version": dataset_version,
                    "status": status_value.replace("_", " ").title() or "Unknown",
                }
            )
            compatibility_source.append(
                {"dataset_version": dataset_version, "status": status_value, "recorded_at": recorded_at}
            )
        compatibility_versions = []
        for entry in compatibility_source:
            version = str(entry.get("dataset_version", ""))
            status_value = str(entry.get("status", "unknown") or "unknown")
            compatibility_versions.append(
                {
                    "version": version,
                    "status": status_value,
                    "status_label": status_value.replace("_", " ").title() or "Unknown",
                    "badge": _DATASET_STATUS_BADGES.get(status_value, "bg-secondary"),
                }
            )
        compatibility_versions.sort(key=lambda item: _key_version(item["version"]))
        context = {
            "request": request,
            "contract": contract,
            "datasets": dataset_records,
            "expectations": {},
            "field_quality": _field_quality_sections(contract),
            "dataset_quality": _dataset_quality_sections(contract),
            "schema_sections": _schema_sections(contract),
            "change_log": [],
            "status_badges": _STATUS_BADGES,
            "server_info": server_info,
            "compatibility_versions": compatibility_versions,
            "preview_dataset_id": dataset_id,
            "contract_json": _contract_json(contract),
        }
        return TEMPLATES.TemplateResponse("contract_detail.html", context)

    @app.get("/api/contracts/{contract_id}/{contract_version}")
    async def contract_json_endpoint(contract_id: str, contract_version: str) -> JSONResponse:
        try:
            contract = await asyncio.to_thread(contract_service.get, contract_id, contract_version)
        except Exception as exc:  # pragma: no cover - forwarded as HTTP error
            raise HTTPException(status_code=404, detail=str(exc))
        payload = contract.model_dump(by_alias=True, exclude_none=True)
        return JSONResponse(content=payload)

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

