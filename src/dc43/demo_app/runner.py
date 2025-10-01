"""Standalone runner for the demo servers without importing backend internals."""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

from .contracts_workspace import current_workspace, prepare_demo_workspace


def _toml_string(value: str) -> str:
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


def _wait_for_backend(base_url: str, *, timeout: float = 30.0) -> None:
    endpoint = base_url.rstrip("/") + "/openapi.json"
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(endpoint, timeout=1.0)
        except Exception as exc:  # pragma: no cover - network errors
            last_exc = exc
            time.sleep(0.5)
            continue
        if response.status_code < 500:
            return
        time.sleep(0.5)
    if last_exc is not None:
        raise RuntimeError(f"Service at {base_url} did not start") from last_exc
    raise RuntimeError(f"Service at {base_url} is not responding")


def main() -> None:  # pragma: no cover - convenience runner
    """Run the pipeline demo alongside the contracts app and backend."""

    import uvicorn

    prepare_demo_workspace()

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

    workspace = current_workspace()
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
        _wait_for_backend(backend_url)
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
            _wait_for_backend(contracts_url)
        except Exception:
            contract_process.terminate()
            with contextlib.suppress(Exception):
                contract_process.wait(timeout=5)
            backend_process.terminate()
            with contextlib.suppress(Exception):
                backend_process.wait(timeout=5)
            raise

    if configured_contracts_url is None:
        os.environ["DC43_CONTRACTS_APP_URL"] = contracts_url

    try:
        uvicorn.run("dc43.demo_app.server:app", host=pipeline_host, port=pipeline_port)
    finally:
        if configured_contracts_url is None:
            os.environ.pop("DC43_CONTRACTS_APP_URL", None)
        else:
            os.environ["DC43_CONTRACTS_APP_URL"] = configured_contracts_url

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


__all__ = ["main"]

