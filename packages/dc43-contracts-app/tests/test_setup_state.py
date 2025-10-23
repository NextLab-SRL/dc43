from __future__ import annotations

import sys
import tomllib
import zipfile
from pathlib import Path
from unittest import mock

from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[3]
SRC_DIRS = [
    ROOT / "packages" / "dc43-service-backends" / "src",
    ROOT / "packages" / "dc43-contracts-app" / "src",
]
for src_dir in SRC_DIRS:
    if src_dir.exists():
        str_path = str(src_dir)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)

from dc43_contracts_app import server
from dc43_contracts_app.setup_bundle import pipeline_stub


def test_delta_databricks_values_fill_unity_config() -> None:
    state = {
        "selected_options": {
            "contracts_backend": "delta_lake",
            "products_backend": "delta_lake",
            "governance_extensions": "none",
        },
        "configuration": {
            "contracts_backend": {
                "storage_path": "s3://contracts",  # optional but ensures delta path present
                "schema": "contracts",
                "workspace_url": "https://adb-123.example.net",
                "workspace_profile": "cli-profile",
                "workspace_token": "token-contract",
            },
            "products_backend": {
                "schema": "products",
                "workspace_url": "https://adb-products.example.net",
                "workspace_profile": "products-profile",
                "workspace_token": "token-products",
            },
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None
    assert config.contract_store.type == "delta"
    assert config.data_product_store.type == "delta"

    unity_cfg = config.unity_catalog
    assert unity_cfg.enabled is False
    assert unity_cfg.workspace_url == "https://adb-123.example.net"
    assert unity_cfg.workspace_profile == "cli-profile"
    assert unity_cfg.workspace_token == "token-contract"


def test_unity_hook_credentials_remain_authoritative() -> None:
    state = {
        "selected_options": {
            "contracts_backend": "delta_lake",
            "products_backend": "delta_lake",
            "governance_extensions": "unity_catalog",
        },
        "configuration": {
            "contracts_backend": {
                "schema": "contracts",
                "workspace_url": "https://adb-contracts.example.net",
                "workspace_profile": "contracts-profile",
                "workspace_token": "token-contract",
            },
            "products_backend": {
                "schema": "products",
                "workspace_profile": "products-profile",
            },
            "governance_extensions": {
                "workspace_url": "https://adb-governance.example.net",
                "workspace_profile": "governance-profile",
                "token": "token-governance",
            },
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None
    unity_cfg = config.unity_catalog
    assert unity_cfg.enabled is True
    assert unity_cfg.workspace_url == "https://adb-governance.example.net"
    assert unity_cfg.workspace_profile == "governance-profile"
    assert unity_cfg.workspace_token == "token-governance"


def test_remote_data_quality_backend_configuration() -> None:
    state = {
        "selected_options": {
            "data_quality": "remote_http",
        },
        "configuration": {
            "data_quality": {
                "base_url": "https://quality.example.com",
                "api_token": "secret-token",
                "token_header": "X-Api-Key",
                "token_scheme": "Token",
                "default_engine": "soda",
                "extra_headers": "X-Org=governance\nX-Region=emea",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    dq_cfg = config.data_quality
    assert dq_cfg.type == "http"
    assert dq_cfg.base_url == "https://quality.example.com"
    assert dq_cfg.token == "secret-token"
    assert dq_cfg.token_header == "X-Api-Key"
    assert dq_cfg.token_scheme == "Token"
    assert dq_cfg.default_engine == "soda"
    assert dq_cfg.headers == {"X-Org": "governance", "X-Region": "emea"}


def test_governance_store_filesystem_configuration() -> None:
    state = {
        "selected_options": {
            "governance_store": "filesystem",
        },
        "configuration": {
            "governance_store": {
                "storage_path": "/var/lib/dc43/governance",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "filesystem"
    assert str(store_cfg.root) == "/var/lib/dc43/governance"


def test_governance_store_delta_adds_databricks_credentials() -> None:
    state = {
        "selected_options": {
            "governance_store": "delta_lake",
        },
        "configuration": {
            "governance_store": {
                "workspace_url": "https://adb-governance.example.net",
                "workspace_profile": "governance-profile",
                "workspace_token": "token-governance",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "delta"

    unity_cfg = config.unity_catalog
    assert unity_cfg.workspace_url == "https://adb-governance.example.net"
    assert unity_cfg.workspace_profile == "governance-profile"
    assert unity_cfg.workspace_token == "token-governance"


def test_governance_store_http_configuration() -> None:
    state = {
        "selected_options": {
            "governance_store": "remote_http",
        },
        "configuration": {
            "governance_store": {
                "base_url": "https://governance.example.com",
                "api_token": "secret-token",
                "token_header": "X-Api-Key",
                "token_scheme": "Token",
                "timeout": "30",
                "extra_headers": "X-Org=governance,X-Team=quality",
            }
        },
    }

    config = server._service_backends_config_from_state(state)
    assert config is not None

    store_cfg = config.governance_store
    assert store_cfg.type == "http"
    assert store_cfg.base_url == "https://governance.example.com"
    assert store_cfg.token == "secret-token"
    assert store_cfg.token_header == "X-Api-Key"
    assert store_cfg.token_scheme == "Token"
    assert store_cfg.timeout == 30.0
    assert store_cfg.headers == {"X-Org": "governance", "X-Team": "quality"}


def test_service_backends_toml_emits_workspace_url() -> None:
    state = {
        "selected_options": {
            "contracts_backend": "delta_lake",
            "governance_extensions": "unity_catalog",
        },
        "configuration": {
            "contracts_backend": {
                "schema": "contracts",
                "workspace_url": "https://adb-contracts.example.net",
            },
            "governance_extensions": {
                "workspace_url": "https://adb-governance.example.net",
                "workspace_profile": "governance",
                "token": "uc-token",
            },
        },
    }

    toml_text = server._service_backends_toml(state)
    assert toml_text is not None
    parsed = tomllib.loads(toml_text)

    assert parsed["unity_catalog"]["workspace_url"] == "https://adb-governance.example.net"
    assert parsed["unity_catalog"]["workspace_profile"] == "governance"


def test_governance_store_module_sits_with_storage_foundations() -> None:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    state = server._default_setup_state()

    context = server._build_setup_context(request, state)
    groups = context.get("module_groups", [])
    storage_group = next((group for group in groups if group.get("key") == "storage_foundations"), None)

    assert storage_group is not None
    module_keys = [module.get("key") for module in storage_group.get("modules", [])]
    assert "governance_store" in module_keys


def test_pipeline_integration_group_is_present() -> None:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    state = server._default_setup_state()

    context = server._build_setup_context(request, state)
    groups = context.get("module_groups", [])
    pipeline_group = next((group for group in groups if group.get("key") == "pipeline_runtime"), None)

    assert pipeline_group is not None
    module_keys = [module.get("key") for module in pipeline_group.get("modules", [])]
    assert "pipeline_integration" in module_keys


def test_pipeline_bootstrap_script_for_spark_integration() -> None:
    state = {
        "selected_options": {
            "pipeline_integration": "spark",
        },
        "configuration": {
            "pipeline_integration": {
                "runtime": "databricks job",
                "workspace_url": "https://adb-123.example.net",
                "workspace_profile": "pipelines",
                "cluster_reference": "job:dc43",
            }
        },
    }

    script = server._pipeline_bootstrap_script(state)

    assert "def build_spark_context" in script
    assert "Spark runtime hint" in script
    assert "databricks job" in script
    assert "suite.contract" in script


def test_pipeline_bootstrap_script_for_dlt_integration() -> None:
    state = {
        "selected_options": {
            "pipeline_integration": "dlt",
        },
        "configuration": {
            "pipeline_integration": {
                "workspace_url": "https://adb-456.example.net",
                "pipeline_name": "dc43-contract-governance",
                "target_schema": "main.governance",
            }
        },
    }

    script = server._pipeline_bootstrap_script(state)

    assert "WorkspaceClient" in script
    assert "dc43-contract-governance" in script
    assert "DLT workspace host" in script


def test_pipeline_example_assets_for_spark_integration() -> None:
    state = {
        "selected_options": {
            "pipeline_integration": "spark",
            "contracts_backend": "delta_lake",
            "products_backend": "delta_lake",
            "data_quality": "native",
            "governance_store": "delta_lake",
        },
        "configuration": {
            "pipeline_integration": {
                "runtime": "databricks job",
                "workspace_url": "https://adb-123.example.net",
                "workspace_profile": "pipelines",
                "cluster_reference": "job:dc43",
            },
            "contracts_backend": {
                "storage_path": "/mnt/contracts",
                "schema": "governance",
            },
            "products_backend": {
                "storage_path": "/mnt/products",
            },
            "governance_store": {
                "storage_path": "/mnt/governance",
            },
        },
    }

    example = server._pipeline_example_assets(state)
    assert example.entrypoint_path == "examples/pipeline_stub.py"

    script = example.entrypoint_content

    assert "build_spark_context" in script
    assert "def review_contract_versions" in script
    assert "def sync_data_product_catalog" in script
    assert "replace-with-contract-id" in script
    assert (
        "- pipeline_integration (spark): cluster_reference=job:dc43, runtime=databricks job,"
        " workspace_profile=pipelines, workspace_url=https://adb-123.example.net"
        in script
    )


def test_pipeline_example_assets_for_dlt_integration() -> None:
    state = {
        "selected_options": {
            "pipeline_integration": "dlt",
            "governance_store": "remote_http",
        },
        "configuration": {
            "pipeline_integration": {
                "workspace_url": "https://adb-456.example.net",
                "workspace_profile": "dlt-admin",
                "pipeline_name": "dc43-contract-governance",
                "notebook_path": "/Repos/team/contracts/dc43_pipeline",
                "target_schema": "main.governance",
            },
            "governance_store": {
                "base_url": "https://governance.example/api",
            },
        },
    }

    example = server._pipeline_example_assets(state)
    assert example.entrypoint_path == "examples/pipeline_stub.py"

    script = example.entrypoint_content

    assert "build_dlt_context" in script
    assert "def publish_governance_updates" in script
    assert "dc43-contract-governance" in script
    assert (
        "- pipeline_integration (dlt): notebook_path=/Repos/team/contracts/dc43_pipeline,"
        " pipeline_name=dc43-contract-governance, target_schema=main.governance,"
        " workspace_profile=dlt-admin, workspace_url=https://adb-456.example.net"
        in script
    )

def test_pipeline_example_assets_use_integration_provider_hook() -> None:
    state = {
        "selected_options": {
            "pipeline_integration": "spark",
        },
        "configuration": {},
    }

    custom_stub = pipeline_stub._IntegrationStub(
        bootstrap_imports=("custom_context",),
        helper_functions=("def custom_helper():", "    return 'ok'", ""),
        main_lines=("    if integration:", "        print(custom_helper())"),
        tail_lines=("    # custom tail",),
        additional_imports=("from custom_project import pipeline",),
        project=pipeline_stub._IntegrationProject(
            root="custom_project",
            entrypoint="__init__.py",
            files=(
                pipeline_stub._ProjectFile(
                    path="__init__.py",
                    content="print('hello from project')\n",
                    executable=False,
                ),
            ),
        ),
    )

    with mock.patch.object(
        pipeline_stub, "_load_external_stub", return_value=custom_stub
    ) as load_stub:
        example = server._pipeline_example_assets(state)

    load_stub.assert_called_once()
    script = example.entrypoint_content
    assert "custom_context" in script
    assert "def custom_helper" in script
    assert "print(custom_helper())" in script
    assert "# custom tail" in script
    assert "from custom_project import pipeline" in script
    assert example.support_files
    assert {
        support.path
        for support in example.support_files
    } == {"examples/custom_project/__init__.py"}


def _minimal_setup_state(pipeline: str) -> dict[str, object]:
    return {
        "selected_options": {
            "pipeline_integration": pipeline,
        },
        "configuration": {
            "pipeline_integration": {},
        },
    }


def test_setup_bundle_includes_environment_bootstrap_files() -> None:
    buffer, payload = server._build_setup_bundle(_minimal_setup_state("spark"))
    assert payload["modules"]

    with zipfile.ZipFile(buffer) as archive:
        names = set(archive.namelist())
        assert "dc43-setup/requirements.txt" in names
        assert "dc43-setup/scripts/bootstrap_environment.sh" in names
        assert "dc43-setup/scripts/bootstrap_environment.ps1" in names

        requirements = archive.read("dc43-setup/requirements.txt").decode("utf-8")
        assert "dc43-contracts-app==" in requirements
        assert "boto3" in requirements

        bootstrap_sh = archive.read(
            "dc43-setup/scripts/bootstrap_environment.sh"
        ).decode("utf-8")
        assert "python3 -m venv" in bootstrap_sh
        assert "pip install -r" in bootstrap_sh


def test_setup_bundle_includes_docker_helpers() -> None:
    buffer, _ = server._build_setup_bundle(_minimal_setup_state("spark"))

    with zipfile.ZipFile(buffer) as archive:
        docker_app = archive.read(
            "dc43-setup/docker/contracts-app/Dockerfile"
        ).decode("utf-8")
        assert "dc43-contracts-app" in docker_app

        docker_backend = archive.read(
            "dc43-setup/docker/service-backends/Dockerfile"
        ).decode("utf-8")
        assert "dc43-service-backends" in docker_backend

        build_script = archive.read(
            "dc43-setup/scripts/build_docker_images.sh"
        ).decode("utf-8")
        assert "docker build -t \"dc43/contracts-app" in build_script

        publish_script = archive.read(
            "dc43-setup/scripts/publish_docker_images.py"
        ).decode("utf-8")
        assert "boto3" in publish_script
        assert "docker" in publish_script


def test_setup_bundle_requirements_include_dlt_dependencies() -> None:
    state = _minimal_setup_state("dlt")
    state["configuration"] = {
        "pipeline_integration": {
            "workspace_url": "https://adb.example.net",
            "pipeline_name": "demo",
        }
    }

    buffer, _ = server._build_setup_bundle(state)

    with zipfile.ZipFile(buffer) as archive:
        requirements = archive.read("dc43-setup/requirements.txt").decode("utf-8")
    assert "databricks-sdk" in requirements
    assert "databricks-dlt" in requirements
