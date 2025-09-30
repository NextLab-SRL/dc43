from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape

from dc43_contracts_app import load_config as load_contracts_config
from dc43_contracts_app import server as contracts_server

try:  # pragma: no cover - compatibility path exercised in integration tests
    from dc43_contracts_app import configure_from_config as configure_contracts_from_config
except ImportError:  # pragma: no cover - fallback for older wheels
    try:
        from dc43_contracts_app.server import (
            configure_from_config as configure_contracts_from_config,
        )
    except ImportError:  # pragma: no cover - define a minimal fallback helper
        configure_contracts_from_config = None  # type: ignore[assignment]


if "configure_contracts_from_config" not in globals() or configure_contracts_from_config is None:
    try:
        from dc43_contracts_app.server import configure_backend as _contracts_configure_backend
    except ImportError:  # pragma: no cover - compatibility shim
        _contracts_configure_backend = None

    try:
        from dc43_contracts_app.server import configure_workspace as _contracts_configure_workspace
    except ImportError:  # pragma: no cover - compatibility shim
        _contracts_configure_workspace = None

    def configure_contracts_from_config(config):  # type: ignore[no-redef]
        """Best-effort configuration fallback for legacy contract app builds."""

        if config is None:
            return None

        workspace_cfg = getattr(config, "workspace", None)
        if workspace_cfg and _contracts_configure_workspace is not None:
            root = getattr(workspace_cfg, "root", None)
            if root:
                _contracts_configure_workspace(root)

        backend_cfg = getattr(config, "backend", None)
        if backend_cfg and _contracts_configure_backend is not None:
            mode = getattr(backend_cfg, "mode", None)
            if mode == "remote":
                base_url = getattr(backend_cfg, "base_url", None)
                if base_url:
                    _contracts_configure_backend(base_url=base_url)
            else:
                root = getattr(backend_cfg, "root", None)
                if root:
                    _contracts_configure_backend(root=root)

        return config
from .contracts_workspace import prepare_demo_workspace
from .scenarios import SCENARIOS

prepare_demo_workspace()

logger = logging.getLogger(__name__)

DatasetRecord = contracts_server.DatasetRecord
load_records = contracts_server.load_records
save_records = contracts_server.save_records
queue_flash = contracts_server.queue_flash
pop_flash = contracts_server.pop_flash
scenario_run_rows = contracts_server.scenario_run_rows
set_active_version = contracts_server.set_active_version
store = contracts_server.store
_dq_version_records = contracts_server._dq_version_records

BASE_DIR = Path(__file__).resolve().parent
CONTRACTS_TEMPLATE_DIR = contracts_server.BASE_DIR / "templates"
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


def _toml_string(value: str) -> str:
    """Return a TOML-safe representation of ``value``."""

    return json.dumps(value)


def _write_backend_config(path: Path, contracts_dir: Path, token: str | None) -> None:
    lines = [
        "[contract_store]",
        f"root = {_toml_string(contracts_dir.as_posix())}",
    ]
    if token:
        lines.extend(
            [
                "",
                "[auth]",
                f"token = {_toml_string(token)}",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_contracts_config(
    path: Path,
    workspace_root: Path,
    backend_host: str,
    backend_port: int,
    backend_url: str,
    backend_log_level: str | None,
) -> None:
    lines = [
        "[workspace]",
        f"root = {_toml_string(workspace_root.as_posix())}",
        "",
        "[backend]",
        "mode = \"remote\"",
        f"base_url = {_toml_string(backend_url)}",
        "",
        "[backend.process]",
        f"host = {_toml_string(backend_host)}",
        f"port = {backend_port}",
    ]
    if backend_log_level:
        lines.append(f"log_level = {_toml_string(backend_log_level)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    """Run the pipeline demo alongside the contracts app and backend."""

    import uvicorn

    backend_host = os.getenv("DC43_DEMO_BACKEND_HOST", "127.0.0.1")
    backend_port = int(os.getenv("DC43_DEMO_BACKEND_PORT", "8001"))
    backend_url = f"http://{backend_host}:{backend_port}"
    backend_log_level = os.getenv("DC43_DEMO_BACKEND_LOG")

    contracts_host = os.getenv("DC43_CONTRACTS_APP_HOST", "127.0.0.1")
    contracts_port = int(os.getenv("DC43_CONTRACTS_APP_PORT", "8002"))
    configured_contracts_url = os.getenv("DC43_CONTRACTS_APP_URL")
    contracts_url = configured_contracts_url or f"http://{contracts_host}:{contracts_port}"

    pipeline_host = os.getenv("DC43_DEMO_HOST", "0.0.0.0")
    pipeline_port = int(os.getenv("DC43_DEMO_PORT", "8000"))

    workspace = contracts_server.current_workspace()
    config_dir = workspace.root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    backend_token = os.getenv("DC43_BACKEND_TOKEN")
    backend_config_path = config_dir / "service_backends.toml"
    _write_backend_config(backend_config_path, workspace.contracts_dir, backend_token)

    contracts_config_path = config_dir / "contracts_app.toml"
    _write_contracts_config(
        contracts_config_path,
        workspace.root,
        backend_host,
        backend_port,
        backend_url,
        backend_log_level,
    )

    previous_contracts_config = os.getenv("DC43_CONTRACTS_APP_CONFIG")
    previous_backend_config = os.getenv("DC43_SERVICE_BACKENDS_CONFIG")
    previous_demo_backend_url = os.getenv("DC43_DEMO_BACKEND_URL")
    previous_demo_work_dir = os.getenv("DC43_DEMO_WORK_DIR")
    previous_contract_store = os.getenv("DC43_CONTRACT_STORE")

    os.environ["DC43_CONTRACTS_APP_CONFIG"] = str(contracts_config_path)
    os.environ["DC43_SERVICE_BACKENDS_CONFIG"] = str(backend_config_path)
    os.environ["DC43_DEMO_BACKEND_URL"] = backend_url
    os.environ["DC43_DEMO_WORK_DIR"] = str(workspace.root)
    os.environ.setdefault("DC43_CONTRACT_STORE", str(workspace.contracts_dir))

    contracts_config = load_contracts_config(contracts_config_path)
    configure_contracts_from_config(contracts_config)

    env = os.environ.copy()

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
    if backend_log_level:
        backend_cmd.extend(["--log-level", backend_log_level])

    backend_process = subprocess.Popen(backend_cmd, env=env)

    try:
        contracts_server._wait_for_backend(backend_url)
    except Exception:
        backend_process.terminate()
        with contextlib.suppress(Exception):
            backend_process.wait(timeout=5)
        raise

    contract_process: subprocess.Popen[bytes] | None = None
    if configured_contracts_url is None:
        contract_env = env.copy()
        contract_env["DC43_DEMO_BACKEND_URL"] = backend_url

        contract_cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "dc43_contracts_app.server:app",
            "--host",
            contracts_host,
            "--port",
            str(contracts_port),
        ]
        contracts_log_level = os.getenv("DC43_CONTRACTS_APP_LOG")
        if contracts_log_level:
            contract_cmd.extend(["--log-level", contracts_log_level])

        contract_process = subprocess.Popen(contract_cmd, env=contract_env)
        try:
            contracts_server._wait_for_backend(contracts_url)
        except Exception:
            contract_process.terminate()
            with contextlib.suppress(Exception):
                contract_process.wait(timeout=5)
            backend_process.terminate()
            with contextlib.suppress(Exception):
                backend_process.wait(timeout=5)
            raise

    global CONTRACTS_APP_URL
    CONTRACTS_APP_URL = contracts_url
    if configured_contracts_url is None:
        os.environ["DC43_CONTRACTS_APP_URL"] = contracts_url

    try:
        uvicorn.run("dc43.demo_app.server:app", host=pipeline_host, port=pipeline_port)
    finally:
        if configured_contracts_url is None:
            os.environ.pop("DC43_CONTRACTS_APP_URL", None)
        else:
            os.environ["DC43_CONTRACTS_APP_URL"] = configured_contracts_url
        CONTRACTS_APP_URL = configured_contracts_url

        if contract_process is not None:
            contract_process.terminate()
            with contextlib.suppress(Exception):
                contract_process.wait(timeout=5)

        backend_process.terminate()
        with contextlib.suppress(Exception):
            backend_process.wait(timeout=5)

        if previous_contracts_config is not None:
            os.environ["DC43_CONTRACTS_APP_CONFIG"] = previous_contracts_config
        else:
            os.environ.pop("DC43_CONTRACTS_APP_CONFIG", None)

        if previous_backend_config is not None:
            os.environ["DC43_SERVICE_BACKENDS_CONFIG"] = previous_backend_config
        else:
            os.environ.pop("DC43_SERVICE_BACKENDS_CONFIG", None)

        if previous_demo_backend_url is not None:
            os.environ["DC43_DEMO_BACKEND_URL"] = previous_demo_backend_url
        else:
            os.environ.pop("DC43_DEMO_BACKEND_URL", None)

        if previous_demo_work_dir is not None:
            os.environ["DC43_DEMO_WORK_DIR"] = previous_demo_work_dir
        else:
            os.environ.pop("DC43_DEMO_WORK_DIR", None)

        if previous_contract_store is not None:
            os.environ["DC43_CONTRACT_STORE"] = previous_contract_store
        else:
            os.environ.pop("DC43_CONTRACT_STORE", None)

        configure_contracts_from_config(load_contracts_config(previous_contracts_config))
