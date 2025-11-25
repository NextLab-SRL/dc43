"""Standalone runner for the demo servers without importing backend internals."""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

import httpx

from dc43_contracts_app.config import (
    BackendConfig,
    BackendProcessConfig,
    ContractsAppConfig,
    DocsChatConfig,
    WorkspaceConfig,
    dump as dump_contracts_config,
    load_config as load_contracts_config,
)
from dc43_service_backends.config import (
    AuthConfig as BackendAuthConfig,
    ServiceBackendsConfig,
    dump as dump_service_backends_config,
)

from .contracts_workspace import current_workspace, prepare_demo_workspace


logger = logging.getLogger(__name__)


def _describe_docs_chat(config: DocsChatConfig) -> str:
    if not config.enabled:
        return "disabled"

    credentials_state = "present" if (config.api_key or config.api_key_env) else "missing"

    docs_path = config.docs_path.as_posix() if config.docs_path else "default"
    index_path = config.index_path.as_posix() if config.index_path else "workspace"

    embedding_provider = (config.embedding_provider or "huggingface").strip() or "huggingface"

    return (
        "enabled "
        f"provider={config.provider} "
        f"model={config.model} "
        f"embeddings={embedding_provider} "
        f"credentials={credentials_state} "
        f"docs_path={docs_path} "
        f"index_path={index_path}"
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dc43-demo",
        description="Launch the dc43 demo application with optional configuration overrides.",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="contracts_config",
        help="Path to a dc43-contracts-app TOML file that should be merged into the demo defaults.",
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        help="Load environment variables from a .env-style file before starting the services.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_env_file(path: str) -> None:
    file_path = Path(path).expanduser()
    try:
        text = file_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Environment file %s was not found; skipping.", file_path)
        return
    except OSError as exc:  # pragma: no cover - defensive logging around unreadable files
        logger.warning("Unable to read environment file %s (%s); skipping.", file_path, exc)
        return

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


def _write_backend_config(path: Path, contracts_dir: Path, token: str | None) -> None:
    config = ServiceBackendsConfig()
    config.contract_store.root = contracts_dir
    if token:
        config.auth = BackendAuthConfig(token=token)
    dump_service_backends_config(path, config)


def _build_contracts_config(
    workspace_root: Path,
    backend_host: str,
    backend_port: int,
    backend_url: str,
    backend_log_level: str | None,
    override_path: str | None,
) -> ContractsAppConfig:
    """Return the contracts app configuration for the demo run."""

    base_config: ContractsAppConfig
    if override_path:
        try:
            base_config = load_contracts_config(override_path)
        except Exception as exc:  # pragma: no cover - defensive guard around custom files
            logger.warning(
                "Failed to load user-supplied contracts app config %s (%s); falling back to defaults.",
                override_path,
                exc,
            )
            base_config = ContractsAppConfig()
        else:
            logger.info(
                "Merging user contracts app configuration from %s into demo defaults.",
                override_path,
            )
    else:
        logger.info("No user contracts app configuration supplied; using demo defaults.")
        base_config = ContractsAppConfig()

    base_config.workspace = WorkspaceConfig(root=workspace_root)
    base_config.backend = BackendConfig(
        mode="remote",
        base_url=backend_url,
        process=BackendProcessConfig(
            host=backend_host,
            port=backend_port,
            log_level=backend_log_level,
        ),
    )

    # Preserve docs chat toggles from the user configuration when provided.
    if override_path is None:
        base_config.docs_chat = DocsChatConfig()

    logger.info(
        "Prepared contracts app configuration (source=%s, docs_chat=%s)",
        override_path or "demo defaults",
        _describe_docs_chat(base_config.docs_chat),
    )

    return base_config


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


def main(argv: Sequence[str] | None = None) -> None:  # pragma: no cover - convenience runner
    """Run the pipeline demo alongside the contracts app and backend."""

    import uvicorn

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    args = _parse_args(argv)
    if args.env_file:
        _load_env_file(args.env_file)

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

    previous_contracts_config = os.getenv("DC43_CONTRACTS_APP_CONFIG")
    override_contracts_config = args.contracts_config or previous_contracts_config

    contracts_config_path = config_dir / "contracts_app.toml"
    contracts_config = _build_contracts_config(
        workspace.root,
        backend_host,
        backend_port,
        backend_url,
        backend_log_level,
        override_contracts_config,
    )
    dump_contracts_config(contracts_config_path, contracts_config)
    logger.info(
        "Contracts app configuration written to %s (%s)",
        contracts_config_path,
        _describe_docs_chat(contracts_config.docs_chat),
    )

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

    logger.info(
        "Starting demo stack (backend=%s:%s, contracts=%s:%s, ui=%s:%s)",
        backend_host,
        backend_port,
        contracts_host,
        contracts_port,
        pipeline_host,
        pipeline_port,
    )

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
        uvicorn.run("dc43_demo_app.server:app", host=pipeline_host, port=pipeline_port)
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


if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    main()


__all__ = ["main"]

