"""Provide pipeline stub fragments tailored for Delta Live Tables."""

from __future__ import annotations

from typing import Callable, Mapping

from .. import PipelineStub, register_pipeline_stub


def _dlt_pipeline_stub(
    *,
    hints: Mapping[str, object],
    flags: Mapping[str, bool],
    json_literal: Callable[[object | None], str],
) -> PipelineStub:
    workspace_hint = json_literal(hints.get("dlt_workspace_url"))
    profile_hint = json_literal(hints.get("dlt_workspace_profile"))
    pipeline_name = json_literal(hints.get("dlt_pipeline_name"))
    notebook_hint = json_literal(hints.get("dlt_notebook_path"))
    target_hint = json_literal(hints.get("dlt_target_schema"))

    main_lines = (
        "    if integration == 'dlt':",
        "        context = build_dlt_context()",
        "        workspace = context.get('workspace')",
        "        if workspace is not None:",
        "            print(\"[dlt] Workspace client initialised:\", workspace)",
        f"        workspace_hint = {workspace_hint}",
        "        if workspace_hint and workspace_hint is not None:",
        "            print(\"[dlt] Workspace host:\", workspace_hint)",
        f"        profile_hint = {profile_hint}",
        "        if profile_hint and profile_hint is not None:",
        "            print(\"[dlt] CLI profile:\", profile_hint)",
        f"        pipeline_name = {pipeline_name}",
        "        if pipeline_name and pipeline_name is not None:",
        "            print(\"[dlt] Pipeline name:\", pipeline_name)",
        f"        notebook_hint = {notebook_hint}",
        "        if notebook_hint and notebook_hint is not None:",
        "            print(\"[dlt] Notebook path:\", notebook_hint)",
        f"        target_hint = {target_hint}",
        "        if target_hint and target_hint is not None:",
        "            print(\"[dlt] Target schema:\", target_hint)",
        "        contract_backend = context.get('contract_backend', contract_backend)",
        "        data_product_backend = context.get('data_product_backend', data_product_backend)",
        "        data_quality_backend = context.get('data_quality_backend', data_quality_backend)",
        "        governance_store = context.get('governance_store', governance_store)",
    )

    return PipelineStub(
        bootstrap_imports=("build_dlt_context",),
        main_lines=main_lines,
    )


register_pipeline_stub("dlt", _dlt_pipeline_stub)


__all__ = ["_dlt_pipeline_stub"]

