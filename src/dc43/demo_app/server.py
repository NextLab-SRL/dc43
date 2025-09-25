from __future__ import annotations

"""FastAPI demo application for dc43.

This application provides a small Bootstrap-powered UI to manage data
contracts and run an example Spark pipeline that records dataset versions
with their validation status. Contracts are stored on the local
filesystem using :class:`~dc43.components.contract_store.impl.filesystem.FSContractStore` and dataset
metadata lives in a JSON file.

Run the application with::

    uvicorn dc43.demo_app.server:app --reload

Optional dependencies needed: ``fastapi``, ``uvicorn``, ``jinja2`` and
``pyspark``.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple, Mapping
from uuid import uuid4
from threading import Lock
from textwrap import dedent
import json
import shutil
import tempfile
import os

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from urllib.parse import urlencode

from dc43.components.contract_store.impl.filesystem import FSContractStore
from dc43.components.data_quality.integration import expectations_from_contract
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Description,
    Server,
    DataQuality,
)
from pydantic import ValidationError
from packaging.version import Version

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = BASE_DIR / "demo_data"
WORK_DIR = Path(tempfile.mkdtemp(prefix="dc43_demo_"))
if not os.getenv("SHOW_WORK_DIR") == "false":
    print(f"The working dir for the demo is: {WORK_DIR}")
    import subprocess, sys
    if sys.platform == "darwin":
        subprocess.run(["open", WORK_DIR])
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
    for prop in contract.customProperties or []:
        if prop.property != "draft_change_log":
            continue
        for item in prop.value or []:
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
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and "
            "<code>customers:1.0.0</code> with schema validation.</li>"
            "<li><strong>Contract:</strong> None provided, so no draft can be"
            " created.</li>"
            "<li><strong>Writes:</strong> Planned dataset <code>result-no-existing-contract</code>"
            " version <code>1.0.0</code> is blocked before any files are"
            " materialised.</li>"
            "<li><strong>Status:</strong> The run exits with an error because the contract is"
            " missing.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders[orders:1.1.0] --> Join[Join datasets]
                    Customers[customers:1.0.0] --> Join
                    Join --> Write[Plan result-no-existing-contract 1.0.0]
                    Write -->|no contract| Block[Run blocked, nothing written]
                """
            ).strip()
            + "</div>"
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
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> then aligns to the target schema.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:1.0.0</code>"
            " with no draft changes.</li>"
            "<li><strong>Writes:</strong> Persists dataset <code>orders_enriched</code>"
            " version <code>1.0.0</code> on the first run; later runs"
            " auto-increment the patch segment (<code>1.0.1</code>,"
            " <code>1.0.2</code>, …).</li>"
            "<li><strong>Status:</strong> Post-write validation reports OK.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders[orders:1.1.0] --> Join[Join datasets]
                    Customers[customers:1.0.0] --> Join
                    Join --> Validate[Align to contract 1.0.0]
                    Validate --> Write[Write orders_enriched 1.0.x]
                    Write --> Status[Run status: OK]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.0.0",
            "run_type": "enforce",
        },
    },
    "dq": {
        "label": "Existing contract fails DQ",
        "description": (
            "<p>Demonstrates a data quality failure.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and prepares draft"
            " <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Writes:</strong> Planned dataset"
            " <code>orders_enriched</code> version <code>1.0.x</code> is never"
            " persisted because post-write validation fails.</li>"
            "<li><strong>Status:</strong> The enforcement run errors when rule"
            " <code>amount &gt; 100</code> is violated.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders[orders:1.1.0] --> Join[Join datasets]
                    Customers[customers:1.0.0] --> Join
                    Join --> Validate[Validate contract 1.1.0]
                    Validate --> Draft[Draft orders_enriched 1.2.0]
                    Validate -->|violations| Block[Run blocked, no dataset version]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
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
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:2.0.0</code>"
            " and proposes draft <code>orders_enriched:2.1.0</code>.</li>"
            "<li><strong>Writes:</strong> Validation stops the job before any"
            " dataset version of <code>orders_enriched</code> is created.</li>"
            "<li><strong>Status:</strong> Schema drift plus failed expectations"
            " produce an error outcome.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders[orders:1.1.0] --> Join[Join datasets]
                    Customers[customers:1.0.0] --> Join
                    Join --> Align[Schema align to contract 2.0.0]
                    Align --> Draft[Draft orders_enriched 2.1.0]
                    Align -->|errors| Block[Run blocked, no dataset version]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "2.0.0",
            "run_type": "enforce",
        },
    },
    "read-partial-block": {
        "label": "Blocked partial input",
        "description": (
            "<p>Attempts to process a batch flagged as invalid.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Orders batch <code>partial-batch</code>"
            " mixes valid and invalid rows; governance marks the dataset as"
            " <code>block</code>.</li>"
            "<li><strong>Contract:</strong> Aims for <code>orders_enriched:1.1.0</code>"
            " but aborts during the read step.</li>"
            "<li><strong>Status:</strong> Default enforcement stops the run before"
            " any writes occur.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Partial[orders:partial-batch] -->|DQ block| Halt[Read fails]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "inputs": {
                "orders": {
                    "dataset_version": "partial-batch",
                    "path": str(DATA_DIR / "orders_partial.json"),
                }
            },
        },
    },
    "read-partial-valid": {
        "label": "Prefer valid subset",
        "description": (
            "<p>Steers reads toward the curated valid slice.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Uses <code>orders::valid</code>"
            " derived from <code>partial-batch</code> to satisfy governance</li>"
            "<li><strong>Contract:</strong> Continues to target"
            " <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Status:</strong> Read succeeds and downstream checks"
            " only flag issues introduced later in the flow.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Partial[orders:partial-batch] --> Option[Governance]
                    Option -->|valid slice| Valid[orders::valid]
                    Valid --> Join[Join datasets]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "partial-batch",
                    "path": str(DATA_DIR / "orders_partial_valid.json"),
                }
            },
        },
    },
    "read-partial-full": {
        "label": "Override with full batch",
        "description": (
            "<p>Documents what happens when the blocked data is forced through.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reuses the blocked"
            " <code>partial-batch</code> but downgrades the read status.</li>"
            "<li><strong>Contract:</strong> Still targets"
            " <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Status:</strong> The UI highlights the manual override"
            " plus any downstream violations.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Partial[orders:partial-batch] --> Override[Override block]
                    Override --> Join[Join datasets]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_version": "partial-batch",
                    "path": str(DATA_DIR / "orders_partial.json"),
                    "status_strategy": {
                        "name": "allow-block",
                        "note": "Manual override: accepted partial batch",
                        "target_status": "warn",
                    },
                }
            },
        },
    },
    "split-lenient": {
        "label": "Split invalid rows",
        "description": (
            "<p>Routes violations to dedicated datasets using the split strategy.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> before aligning to"
            " <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and stores draft"
            " <code>orders_enriched:1.2.0</code> when rejects exist.</li>"
            "<li><strong>Writes:</strong> Persists three datasets sharing the same"
            " auto-incremented version: the contracted"
            " <code>orders_enriched</code> (full batch),"
            " <code>orders_enriched::valid</code>, and"
            " <code>orders_enriched::reject</code>.</li>"
            "<li><strong>Status:</strong> Run finishes with a warning because"
            " validation finds violations, and the UI links the auxiliary"
            " datasets.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders[orders:1.1.0] --> Join[Join datasets]
                    Customers[customers:1.0.0] --> Join
                    Join --> Validate[Validate contract 1.1.0]
                    Validate --> Strategy[Split strategy]
                    Strategy --> Full[orders_enriched 1.0.x]
                    Strategy --> Valid[orders_enriched::valid 1.0.x]
                    Strategy --> Reject[orders_enriched::reject 1.0.x]
                """
            ).strip()
            + "</div>"
        ),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "violation_strategy": {
                "name": "split",
                "include_valid": True,
                "include_reject": True,
                "write_primary_on_violation": True,
            },
        },
    },
}


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
    field_quality = _field_quality_sections(contract)
    dataset_quality = _dataset_quality_sections(contract)
    change_log = _contract_change_log(contract)
    context = {
        "request": request,
        "contract": contract_to_dict(contract),
        "datasets": datasets,
        "expectations": expectations_from_contract(contract),
        "field_quality": field_quality,
        "dataset_quality": dataset_quality,
        "change_log": change_log,
        "status_badges": _STATUS_BADGES,
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
    flash_token = request.query_params.get("flash")
    flash_message: str | None = None
    flash_error: str | None = None
    if flash_token:
        flash_message, flash_error = pop_flash(flash_token)
    else:
        flash_message = request.query_params.get("msg")
        flash_error = request.query_params.get("error")
    context = {
        "request": request,
        "records": recs,
        "scenarios": SCENARIOS,
        "message": flash_message,
        "error": flash_error,
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
        dataset_name, new_version = run_pipeline(
            p.get("contract_id"),
            p.get("contract_version"),
            p.get("dataset_name"),
            p.get("dataset_version"),
            p.get("run_type", "infer"),
            p.get("collect_examples", False),
            p.get("examples_limit", 5),
            p.get("violation_strategy"),
            p.get("inputs"),
        )
        label = dataset_name or p.get("dataset_name") or p.get("contract_id") or "dataset"
        token = queue_flash(message=f"Run succeeded: {label} {new_version}")
        params = urlencode({"flash": token})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        token = queue_flash(error=str(exc))
        params = urlencode({"flash": token})
    return RedirectResponse(url=f"/datasets?{params}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    """Run the demo app with uvicorn."""
    import uvicorn

    uvicorn.run("dc43.demo_app.server:app", host="0.0.0.0", port=8000)
