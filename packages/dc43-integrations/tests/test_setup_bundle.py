"""Tests for integration-provided pipeline stub fragments."""

from __future__ import annotations

import json

from dc43_integrations.setup_bundle import get_pipeline_stub


def _json_literal(value: object | None) -> str:
    return json.dumps(value) if value else "None"


def test_spark_pipeline_stub_includes_runtime_hints() -> None:
    hints = {
        "key": "spark",
        "spark_runtime": "databricks job",
        "spark_workspace_url": "https://adb-123.example.net",
        "spark_workspace_profile": "pipelines",
        "spark_cluster": "job:dc43",
    }

    stub = get_pipeline_stub("spark", hints=hints, flags={}, json_literal=_json_literal)

    assert stub is not None
    assert "build_spark_context" in stub.bootstrap_imports
    rendered = "\n".join(stub.main_lines)
    assert "Spark session initialised" in rendered
    assert "databricks job" in rendered
    assert "https://adb-123.example.net" in rendered
    assert any(import_line.startswith("from spark_pipeline") for import_line in stub.additional_imports)
    assert stub.project is not None
    assert stub.project.root == "spark_pipeline"
    project_paths = {file.path for file in stub.project.files}
    assert {"README.md", "main.py", "io.py", "quality.py", "transformations.py", "governance.py"}.issubset(project_paths)
    main_file = next(file for file in stub.project.files if file.path == "main.py")
    assert "read_contract_dataset" in main_file.content
    assert "write_contract_outputs" in main_file.content


def test_dlt_pipeline_stub_exposes_workspace_details() -> None:
    hints = {
        "key": "dlt",
        "dlt_workspace_url": "https://adb-456.example.net",
        "dlt_workspace_profile": "dlt-admin",
        "dlt_pipeline_name": "dc43-contract-governance",
        "dlt_notebook_path": "/Repos/team/contracts/dc43_pipeline",
        "dlt_target_schema": "main.governance",
    }

    stub = get_pipeline_stub("dlt", hints=hints, flags={}, json_literal=_json_literal)

    assert stub is not None
    assert "build_dlt_context" in stub.bootstrap_imports
    rendered = "\n".join(stub.main_lines)
    assert "Workspace client initialised" in rendered
    assert "dc43-contract-governance" in rendered
    assert "main.governance" in rendered
    assert stub.project is not None
    assert stub.project.root == "dlt_pipeline"
    project_paths = {file.path for file in stub.project.files}
    assert {"README.md", "pipeline.py", "ops.py"}.issubset(project_paths)
    pipeline_module = next(file for file in stub.project.files if file.path == "pipeline.py")
    assert "@governed_table" in pipeline_module.content
    ops_module = next(file for file in stub.project.files if file.path == "ops.py")
    assert "register_output_port" in ops_module.content


def test_unknown_pipeline_stub_returns_none() -> None:
    stub = get_pipeline_stub("unknown", hints={}, flags={}, json_literal=_json_literal)
    assert stub is None

