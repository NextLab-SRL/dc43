from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from importlib import resources

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape

from .contracts_api import set_active_version
from .contracts_records import (
    load_records,
    pop_flash,
    queue_flash,
    save_records,
    scenario_run_rows,
)
from .contracts_workspace import prepare_demo_workspace
from .scenarios import SCENARIOS

prepare_demo_workspace()

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CONTRACTS_TEMPLATE_DIR = resources.files("dc43_contracts_app") / "templates"
PIPELINE_TEMPLATE_DIR = BASE_DIR / "templates"

_env_loader = ChoiceLoader(
    [
        FileSystemLoader(str(PIPELINE_TEMPLATE_DIR)),
        FileSystemLoader(str(CONTRACTS_TEMPLATE_DIR)),
    ]
)

template_env = Environment(loader=_env_loader, autoescape=select_autoescape(["html", "xml"]))
templates = Jinja2Templates(env=template_env)

app = FastAPI(title="DC43 Demo Pipeline")

CONTRACTS_APP_URL = os.getenv("DC43_CONTRACTS_APP_URL")
@app.get("/")
async def redirect_to_pipeline() -> RedirectResponse:
    return RedirectResponse(url="/pipeline-runs", status_code=307)


@app.get("/contracts")
async def redirect_contracts() -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    return RedirectResponse(url=CONTRACTS_APP_URL, status_code=307)


@app.get("/datasets")
async def redirect_datasets() -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    return RedirectResponse(url=f"{CONTRACTS_APP_URL.rstrip('/')}/datasets", status_code=307)


@app.get("/contracts/{path:path}")
async def redirect_contract_pages(path: str) -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    url = f"{CONTRACTS_APP_URL.rstrip('/')}/contracts/{path}" if path else f"{CONTRACTS_APP_URL.rstrip('/')}/contracts"
    return RedirectResponse(url=url, status_code=307)


@app.get("/datasets/{path:path}")
async def redirect_dataset_pages(path: str) -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    url = f"{CONTRACTS_APP_URL.rstrip('/')}/datasets/{path}" if path else f"{CONTRACTS_APP_URL.rstrip('/')}/datasets"
    return RedirectResponse(url=url, status_code=307)


@app.get("/pipeline-runs", response_class=HTMLResponse)
async def list_pipeline_runs(request: Request) -> HTMLResponse:
    records = load_records()
    recs = [r.__dict__.copy() for r in records]
    scenario_rows = scenario_run_rows(records, SCENARIOS)
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
        "scenario_rows": scenario_rows,
        "message": flash_message,
        "error": flash_error,
    }
    return templates.TemplateResponse("pipeline_runs.html", context)


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(scenario: str = Form(...)) -> HTMLResponse:
    from .pipeline import run_pipeline

    cfg = SCENARIOS.get(scenario)
    if not cfg:
        params = urlencode({"error": f"Unknown scenario: {scenario}"})
        return RedirectResponse(url=f"/datasets?{params}", status_code=303)
    params_cfg = cfg["params"]
    for dataset, version in cfg.get("activate_versions", {}).items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue
    try:
        dataset_name, new_version = await asyncio.to_thread(
            run_pipeline,
            params_cfg.get("contract_id"),
            params_cfg.get("contract_version"),
            params_cfg.get("dataset_name"),
            params_cfg.get("dataset_version"),
            params_cfg.get("run_type", "infer"),
            params_cfg.get("collect_examples", False),
            params_cfg.get("examples_limit", 5),
            params_cfg.get("violation_strategy"),
            params_cfg.get("inputs"),
            params_cfg.get("output_adjustment"),
            scenario_key=scenario,
        )
        label = (
            dataset_name
            or params_cfg.get("dataset_name")
            or params_cfg.get("contract_id")
            or "dataset"
        )
        token = queue_flash(message=f"Run succeeded: {label} {new_version}")
        params_qs = urlencode({"flash": token})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        logger.exception("Pipeline run failed for scenario %s", scenario)
        token = queue_flash(error=str(exc))
        params_qs = urlencode({"flash": token})
    return RedirectResponse(url=f"/pipeline-runs?{params_qs}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    from .runner import main as _main

    _main()
