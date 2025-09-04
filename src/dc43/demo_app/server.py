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
from packaging.version import Version

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "demo_data"
CONTRACT_DIR = DATA_DIR / "contracts"
DATA_INPUT_DIR = DATA_DIR / "data"
RECORDS_DIR = DATA_DIR / "records"
DATASETS_FILE = RECORDS_DIR / "datasets.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
CONTRACT_DIR.mkdir(parents=True, exist_ok=True)
DATA_INPUT_DIR.mkdir(parents=True, exist_ok=True)
RECORDS_DIR.mkdir(parents=True, exist_ok=True)
if not DATASETS_FILE.exists():
    DATASETS_FILE.write_text("[]", encoding="utf-8")

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
    run_type: str = "infer"
    violations: int = 0
    draft_contract_version: str | None = None


def load_records() -> List[DatasetRecord]:
    raw = json.loads(DATASETS_FILE.read_text())
    return [DatasetRecord(**r) for r in raw]


def save_records(records: List[DatasetRecord]) -> None:
    DATASETS_FILE.write_text(
        json.dumps([r.__dict__ for r in records], indent=2), encoding="utf-8"
    )


# Predefined pipeline scenarios exposed in the UI. Each scenario describes the
# parameters passed to the example pipeline along with a human readable
# description shown to the user.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "no-contract": {
        "label": "No contract provided",
        "description": (
            "<p>Run the pipeline without supplying an output contract.</p>"
            "<ul>"
            "<li>Reads orders:1.1.0 and customers:1.0.0.</li>"
            "<li>Write is attempted in <em>enforce</em> mode so the missing contract"
            " triggers an error.</li>"
            "</ul>"
        ),
        "params": {
            "contract_id": None,
            "contract_version": None,
            "dataset_name": "result-no-existing-contract",
            "run_type": "enforce",
        },
    },
    "ok": {
        "label": "Existing contract OK",
        "description": (
            "<p>Happy path using contract <code>orders_enriched:1.0.0</code>.</p>"
            "<ul>"
            "<li>Input data matches the contract schema and quality rules.</li>"
            "<li>The pipeline writes a new dataset version and records an OK status.</li>"
            "</ul>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.0.0",
            "dataset_name": "output-ok-contract",
            "run_type": "enforce",
        },
    },
    "dq": {
        "label": "Existing contract fails DQ",
        "description": (
            "<p>Demonstrates a data quality failure.</p>"
            "<ul>"
            "<li>Contract <code>orders_enriched:1.1.0</code> requires amount &gt; 100.</li>"
            "<li>Sample data contains smaller amounts, producing DQ violations.</li>"
            "<li>The pipeline blocks the write and surfaces an error.</li>"
            "</ul>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "dataset_name": "output-wrong-quality",
            "run_type": "enforce",
            "collect_examples": True,
            "examples_limit": 3,
        },
    },
    "schema-dq": {
        "label": "Contract fails schema and DQ",
        "description": (
            "<p>Shows combined schema and data quality issues.</p>"
            "<ul>"
            "<li>Contract <code>orders_enriched:2.0.0</code> introduces new fields.</li>"
            "<li>The DataFrame does not match the schema and violates quality rules.</li>"
            "<li>A draft contract is generated for review and the run fails.</li>"
            "</ul>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "2.0.0",
            "dataset_name": "output-ko",
            "run_type": "enforce",
        },
    },
}


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
        "datasets": datasets,
        "expectations": expectations,
    }


@app.post("/api/contracts/{cid}/{ver}/validate")
async def api_validate_contract(cid: str, ver: str) -> Dict[str, str]:
    return {"status": "active"}


@app.get("/api/datasets")
async def api_datasets() -> List[Dict[str, Any]]:
    records = load_records()
    return [r.__dict__.copy() for r in records]


@app.get("/api/datasets/{dataset_version}")
async def api_dataset_detail(dataset_version: str) -> Dict[str, Any]:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            return {
                "record": r.__dict__,
                "contract": contract_to_dict(contract),
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


@app.get("/contracts/{cid}", response_class=HTMLResponse)
async def list_contract_versions(request: Request, cid: str) -> HTMLResponse:
    versions = store.list_versions(cid)
    if not versions:
        raise HTTPException(status_code=404, detail="Contract not found")
    contracts = []
    for ver in versions:
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
        contracts.append({"id": cid, "version": ver, "path": path})
    context = {"request": request, "contract_id": cid, "contracts": contracts}
    return templates.TemplateResponse("contract_versions.html", context)


@app.get("/contracts/{cid}/{ver}", response_class=HTMLResponse)
async def contract_detail(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    datasets = [r for r in load_records() if r.contract_id == cid and r.contract_version == ver]
    context = {
        "request": request,
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations_from_contract(contract),
    }
    return templates.TemplateResponse("contract_detail.html", context)


def _next_version(ver: str) -> str:
    v = Version(ver)
    return f"{v.major}.{v.minor}.{v.micro + 1}"


@app.get("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def edit_contract_form(request: Request, cid: str, ver: str) -> HTMLResponse:
    try:
        contract = store.get(cid, ver)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    new_ver = _next_version(ver)
    server = (contract.servers or [None])[0]
    path = getattr(server, "path", "") if server else ""
    props = []
    if contract.schema:
        props = contract.schema[0].properties or []
    columns = "\n".join(f"{p.name}:{p.physicalType}" for p in props)
    context = {
        "request": request,
        "editing": True,
        "contract_id": contract.id,
        "contract_version": new_ver,
        "name": contract.name,
        "description": getattr(contract.description, "usage", ""),
        "dataset_path": path,
        "columns": columns,
        "original_version": ver,
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.post("/contracts/{cid}/{ver}/edit", response_class=HTMLResponse)
async def save_contract_edits(
    request: Request,
    cid: str,
    ver: str,
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
        return RedirectResponse(url=f"/contracts/{contract_id}/{contract_version}", status_code=303)
    except ValidationError as ve:
        error = str(ve)
    except Exception as exc:  # pragma: no cover - display any other error
        error = str(exc)
    context = {
        "request": request,
        "editing": True,
        "error": error,
        "contract_id": contract_id,
        "contract_version": contract_version,
        "name": name,
        "description": description,
        "columns": columns,
        "dataset_path": dataset_path,
        "original_version": ver,
    }
    return templates.TemplateResponse("new_contract.html", context)


@app.post("/contracts/{cid}/{ver}/validate")
async def html_validate_contract(cid: str, ver: str) -> HTMLResponse:
    return RedirectResponse(url=f"/contracts/{cid}/{ver}", status_code=303)


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
    recs = []
    for r in records:
        recs.append(r.__dict__.copy())
    context = {
        "request": request,
        "records": recs,
        "scenarios": SCENARIOS,
        "message": request.query_params.get("msg"),
        "error": request.query_params.get("error"),
    }
    return templates.TemplateResponse("datasets.html", context)


@app.get("/datasets/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_version: str) -> HTMLResponse:
    for r in load_records():
        if r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            context = {
                "request": request,
                "record": r,
                "contract": contract_to_dict(contract),
            }
            return templates.TemplateResponse("dataset_detail.html", context)
    raise HTTPException(status_code=404, detail="Dataset not found")


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(scenario: str = Form(...)) -> HTMLResponse:
    from .pipeline import run_pipeline

    cfg = SCENARIOS.get(scenario)
    if not cfg:
        params = urlencode({"error": f"Unknown scenario: {scenario}"})
        return RedirectResponse(url=f"/datasets?{params}", status_code=303)
    p = cfg["params"]
    try:
        new_version = run_pipeline(
            p.get("contract_id"),
            p.get("contract_version"),
            p["dataset_name"],
            p.get("dataset_version"),
            p.get("run_type", "infer"),
            p.get("collect_examples", False),
            p.get("examples_limit", 5),
        )
        params = urlencode({"msg": f"Run succeeded: {p['dataset_name']} {new_version}"})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        params = urlencode({"error": str(exc)})
    return RedirectResponse(url=f"/datasets?{params}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    """Run the demo app with uvicorn."""
    import uvicorn

    uvicorn.run("dc43.demo_app.server:app", host="0.0.0.0", port=8000)
