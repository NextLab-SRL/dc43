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


def test_unknown_pipeline_stub_returns_none() -> None:
    stub = get_pipeline_stub("unknown", hints={}, flags={}, json_literal=_json_literal)
    assert stub is None

