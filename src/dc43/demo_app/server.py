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
import shutil
import tempfile

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlencode

from dc43.storage.fs import FSContractStore
from dc43.dq.spark_metrics import expectations_from_contract
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
SAMPLE_DIR = BASE_DIR / "demo_data"
WORK_DIR = Path(tempfile.mkdtemp(prefix="dc43_demo_"))
CONTRACT_DIR = WORK_DIR / "contracts"
DATA_DIR = WORK_DIR / "data"
RECORDS_DIR = WORK_DIR / "records"
DATASETS_FILE = RECORDS_DIR / "datasets.json"

# Copy sample data and records into a temporary working directory so the
# application operates on absolute paths that are isolated per run.
shutil.copytree(SAMPLE_DIR / "data", DATA_DIR)
shutil.copytree(SAMPLE_DIR / "records", RECORDS_DIR)

# Prepare contracts with absolute server paths pointing inside the working dir.
for src in (SAMPLE_DIR / "contracts").rglob("*.json"):
    model = OpenDataContractStandard.model_validate_json(src.read_text())
    for srv in model.servers or []:
        p = Path(srv.path or "")
        if not p.is_absolute():
            p = (WORK_DIR / p).resolve()
        base = p.parent if p.suffix else p
        base.mkdir(parents=True, exist_ok=True)
        srv.path = str(p)
    dest = CONTRACT_DIR / src.relative_to(SAMPLE_DIR / "contracts")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        model.model_dump_json(indent=2, by_alias=True, exclude_none=True),
        encoding="utf-8",
    )

store = FSContractStore(str(CONTRACT_DIR))

# Populate server paths with sample datasets matching recorded versions
_sample_records = json.loads((RECORDS_DIR / "datasets.json").read_text())
for _r in _sample_records:
    try:
        _c = store.get(_r["contract_id"], _r["contract_version"])
    except FileNotFoundError:
        continue
    _srv = (_c.servers or [None])[0]
    if not _srv or not _srv.path:
        continue
    _dest = Path(_srv.path)
    _src = SAMPLE_DIR / "data" / f"{_r['dataset_name']}.json"
    if not _src.exists():
        continue
    base = _dest.parent if _dest.suffix else _dest
    _ds_dir = base / _r["dataset_name"] / _r["dataset_version"]
    _ds_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_src, _ds_dir / _src.name)

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
    """Return a plain dict for a contract using public field aliases."""
    try:
        return c.model_dump(by_alias=True, exclude_none=True)
    except AttributeError:  # pragma: no cover - Pydantic v1 fallback
        return c.dict(by_alias=True, exclude_none=True)  # type: ignore[call-arg]


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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/contracts", response_class=HTMLResponse)
async def list_contracts(request: Request) -> HTMLResponse:
    contract_ids = store.list_contracts()
    return templates.TemplateResponse(
        "contracts.html", {"request": request, "contracts": contract_ids}
    )


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
    path = Path(dataset_path)
    try:
        props = []
        for line in columns.splitlines():
            line = line.strip()
            if not line:
                continue
            col_name, col_type = [p.strip() for p in line.split(":", 1)]
            props.append(SchemaProperty(name=col_name, physicalType=col_type, required=True))
        if not path.is_absolute():
            path = (Path(DATA_DIR).parent / path).resolve()
        model = OpenDataContractStandard(
            version=contract_version,
            kind="DataContract",
            apiVersion="3.0.2",
            id=contract_id,
            name=name,
            description=Description(usage=description),
            schema=[SchemaObject(name=name, properties=props)],
            servers=[Server(server="local", type="filesystem", path=str(path))],
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
        "dataset_path": str(path),
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
    path = Path(dataset_path)
    try:
        props = []
        for line in columns.splitlines():
            line = line.strip()
            if not line:
                continue
            col_name, col_type = [p.strip() for p in line.split(":", 1)]
            props.append(SchemaProperty(name=col_name, physicalType=col_type, required=True))
        if not path.is_absolute():
            path = (Path(DATA_DIR).parent / path).resolve()
        model = OpenDataContractStandard(
            version=contract_version,
            kind="DataContract",
            apiVersion="3.0.2",
            id=contract_id,
            name=name,
            description=Description(usage=description),
            schema=[SchemaObject(name=name, properties=props)],
            servers=[Server(server="local", type="filesystem", path=str(path))],
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
        "dataset_path": str(path),
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


@app.get("/datasets/{dataset_name}", response_class=HTMLResponse)
async def dataset_versions(request: Request, dataset_name: str) -> HTMLResponse:
    records = [r.__dict__.copy() for r in load_records() if r.dataset_name == dataset_name]
    context = {"request": request, "dataset_name": dataset_name, "records": records}
    return templates.TemplateResponse("dataset_versions.html", context)


def _dataset_path(contract: OpenDataContractStandard | None, dataset_name: str, dataset_version: str) -> Path:
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(DATA_DIR).parent
    base = Path(getattr(server, "path", "")) if server else data_root
    if base.suffix:
        base = base.parent
    if not base.is_absolute():
        base = data_root / base
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


@app.get("/datasets/{dataset_name}/{dataset_version}", response_class=HTMLResponse)
async def dataset_detail(request: Request, dataset_name: str, dataset_version: str) -> HTMLResponse:
    for r in load_records():
        if r.dataset_name == dataset_name and r.dataset_version == dataset_version:
            contract = store.get(r.contract_id, r.contract_version)
            preview = _dataset_preview(contract, dataset_name, dataset_version)
            context = {
                "request": request,
                "record": r,
                "contract": contract_to_dict(contract),
                "data_preview": preview,
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
