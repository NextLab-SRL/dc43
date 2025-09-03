from __future__ import annotations

"""FastAPI demo application for dc43.

This application provides a small Bootstrap-powered UI to manage data
contracts and run an example Spark pipeline that records dataset versions
with their validation status. Contracts are stored on the local
filesystem using :class:`~dc43.storage.fs.FSContractStore` and dataset
metadata lives in a JSON file.

Run the application with::

    uvicorn dc43.demo_app.server:app --reload

Optional dependencies needed: ``fastapi``, ``uvicorn``, ``jinja2`` and
``pyspark``.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any
import json

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlencode

from dc43.storage.fs import FSContractStore
from dc43.dq.metrics import expectations_from_contract
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Description,
    Server,
)
from pydantic import ValidationError

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "demo_data"
CONTRACT_DIR = DATA_DIR / "contracts"
DATASETS_FILE = DATA_DIR / "datasets.json"
CONTRACT_META_FILE = DATA_DIR / "contract_meta.json"

CONTRACT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
if not DATASETS_FILE.exists():
    DATASETS_FILE.write_text("[]", encoding="utf-8")
if not CONTRACT_META_FILE.exists():
    CONTRACT_META_FILE.write_text("[]", encoding="utf-8")

store = FSContractStore(str(CONTRACT_DIR))

app = FastAPI(title="DC43 Demo")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static"), check_dir=False), name="static")


@dataclass
class DatasetRecord:
    contract_id: str
    contract_version: str
    dataset_name: str = ""
    dataset_version: str = ""
    status: str = "unknown"
    dq_details: Dict[str, Any] = field(default_factory=dict)
    run_type: str = "unknown"


def load_records() -> List[DatasetRecord]:
    raw = json.loads(DATASETS_FILE.read_text())
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord]) -> None:
    DATASETS_FILE.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


def load_contract_meta() -> List[Dict[str, Any]]:
    meta = json.loads(CONTRACT_META_FILE.read_text())
    for m in meta:
        try:
            contract = store.get(m["id"], m["version"])
            server = (contract.servers or [None])[0]
            path = ""
            if server:
                parts = []
                if getattr(server, "path", None):
                    parts.append(server.path)
                if getattr(server, "dataset", None):
                    parts.append(server.dataset)
                path = "/".join(parts)
            m["path"] = path
        except FileNotFoundError:
            m["path"] = ""
    return meta


def save_contract_meta(meta: List[Dict[str, Any]]) -> None:
    stripped = [{k: v for k, v in m.items() if k in {"id", "version", "status"}} for m in meta]
    CONTRACT_META_FILE.write_text(
        json.dumps(stripped, indent=2), encoding="utf-8"
    )


def get_contract_status(cid: str, ver: str) -> str:
    meta = load_contract_meta()
    for m in meta:
        if m["id"] == cid and m["version"] == ver:
            return m.get("status", "unknown")
    return "unknown"


def set_contract_status(cid: str, ver: str, status: str) -> None:
    meta = load_contract_meta()
    for m in meta:
        if m["id"] == cid and m["version"] == ver:
            m["status"] = status
            break
    else:
        meta.append({"id": cid, "version": ver, "status": status})
    save_contract_meta(meta)


def contract_to_dict(c: OpenDataContractStandard) -> Dict[str, Any]:
    try:
        data = c.model_dump()
    except AttributeError:  # pydantic v1 fallback
        data = c.dict()  # type: ignore
    # Pydantic names the "schema" field "schema_" to avoid clashing with the
    # ``BaseModel.schema`` method. Rename it here so the client can simply
    # access ``contract.schema`` without worrying about the underscore suffix.
    if "schema_" in data:
        data["schema"] = data.pop("schema_")
    return data


@app.get("/api/contracts")
async def api_contracts() -> List[Dict[str, Any]]:
    return load_contract_meta()


@app.get("/api/contracts/{cid}/{ver}")
async def api_contract_detail(cid: str, ver: str) -> Dict[str, Any]:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r.__dict__ for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    expectations = expectations_from_contract(contract)
    return {
        "contract": contract_to_dict(contract),
        "status": get_contract_status(cid, ver),
        "datasets": datasets,
        "expectations": expectations,
    }


@app.post("/api/contracts/{cid}/{ver}/validate")
async def api_validate_contract(cid: str, ver: str) -> Dict[str, str]:
    set_contract_status(cid, ver, "active")
    return {"status": "active"}


@app.get("/api/datasets")
async def api_datasets() -> List[Dict[str, Any]]:
    records = load_records()
    out: List[Dict[str, Any]] = []
    for r in records:
        rec = r.__dict__.copy()
        rec["contract_status"] = get_contract_status(r.contract_id, r.contract_version)
        out.append(rec)
    return out


@app.get("/api/datasets/{dataset_version}")
async def api_dataset_detail(dataset_version: str) -> Dict[str, Any]:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            return {
                "record": r.__dict__,
                "contract": contract_to_dict(contract),
                "contract_status": get_contract_status(r.contract_id, r.contract_version),
                "expectations": expectations_from_contract(contract),
            }
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


@app.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contracts = load_contract_meta()
    return templates.TemplateResponse("contracts.html", {"request": request, "contracts": contracts})


@app.get("/contracts/new", response_class=HTMLResponse)
async def new_contract_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("new_contract.html", {"request": request})


@app.post("/contracts/new", response_class=HTMLResponse)
async def create_contract(
    request: Request,
    contract_id: str = Form(...),
    contract_version: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    columns: str = Form(""),
    dataset_path: str = Form(""),
) -> HTMLResponse:
    try:
        props = []
        for line in columns.splitlines():
            line = line.strip()
            if not line:
                continue
            col_name, col_type = [p.strip() for p in line.split(":", 1)]
            props.append(SchemaProperty(name=col_name, physicalType=col_type, required=True))
        model = OpenDataContractStandard(
            version=contract_version,
            kind="DataContract",
            apiVersion="3.0.2",
            id=contract_id,
            name=name,
            description=Description(usage=description),
            schema=[SchemaObject(name=name, properties=props)],
            servers=[Server(server="local", type="filesystem", path=dataset_path)],
        )
        store.put(model)
        meta = load_contract_meta()
        for m in meta:
            if m["id"] == contract_id and m["version"] == contract_version:
                m.update({"status": "draft"})
                break
        else:
            meta.append({"id": contract_id, "version": contract_version, "status": "draft"})
        save_contract_meta(meta)
        return RedirectResponse(url="/contracts", status_code=303)
    except ValidationError as ve:
        error = str(ve)
    except Exception as exc:  # pragma: no cover - display any other error
        error = str(exc)
    context = {
        "request": request,
        "error": error,
        "contract_id": contract_id,
        "contract_version": contract_version,
        "name": name,
        "description": description,
        "columns": columns,
        "dataset_path": dataset_path,
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.get("/datasets", response_class=HTMLResponse)
async def list_datasets(request: Request) -> HTMLResponse:
    records = load_records()
    meta = load_contract_meta()
    contract_ids = sorted({m["id"] for m in meta})
    contract_versions = {cid: sorted([m["version"] for m in meta if m["id"] == cid], reverse=True) for cid in contract_ids}
    dataset_versions = sorted({r.dataset_version for r in records if r.dataset_version})
    dataset_names = sorted({r.dataset_name for r in records if r.dataset_name})
    context = {
        "request": request,
        "records": records,
        "contract_ids": contract_ids,
        "contract_versions": contract_versions,
        "dataset_versions": dataset_versions,
        "dataset_names": dataset_names,
        "message": request.query_params.get("msg"),
        "error": request.query_params.get("error"),
    }
    return templates.TemplateResponse("datasets.html", context)


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(
    contract_id: str = Form(...),
    contract_version: str = Form(...),
    dataset_name: str = Form(...),
    dataset_version: str = Form(""),
    run_type: str = Form("unknown"),
) -> HTMLResponse:
    from .pipeline import run_pipeline

    input_path = str(DATA_DIR / "sample_input.json")
    try:
        new_version = run_pipeline(
            contract_id,
            contract_version,
            dataset_name,
            dataset_version or None,
            run_type,
            input_path,
        )
        params = urlencode({"msg": f"Run succeeded: {dataset_name} {new_version}"})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        params = urlencode({"error": str(exc)})
    return RedirectResponse(url=f"/datasets?{params}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    """Run the demo app with uvicorn."""
    import uvicorn

    uvicorn.run("dc43.demo_app.server:app", host="0.0.0.0", port=8000)
