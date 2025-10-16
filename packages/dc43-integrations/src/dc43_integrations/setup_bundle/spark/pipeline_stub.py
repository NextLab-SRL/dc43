"""Provide pipeline stub fragments tailored for Spark runtimes."""

from __future__ import annotations

from typing import Callable, Mapping

from .. import PipelineStub, register_pipeline_stub


def _spark_pipeline_stub(
    *,
    hints: Mapping[str, object],
    flags: Mapping[str, bool],
    json_literal: Callable[[object | None], str],
) -> PipelineStub:
    runtime_hint = json_literal(hints.get("spark_runtime"))
    workspace_hint = json_literal(hints.get("spark_workspace_url"))
    profile_hint = json_literal(hints.get("spark_workspace_profile"))
    cluster_hint = json_literal(hints.get("spark_cluster"))

    main_lines = (
        "    if integration == 'spark':",
        "        context = build_spark_context(app_name=\"dc43-pipeline-example\")",
        "        spark = context.get('spark')",
        "        if spark is not None:",
        "            print(\"[spark] Spark session initialised:\", spark)",
        f"        runtime_hint = {runtime_hint}",
        "        if runtime_hint and runtime_hint is not None:",
        "            print(\"[spark] Runtime configured in setup:\", runtime_hint)",
        f"        workspace_hint = {workspace_hint}",
        "        if workspace_hint and workspace_hint is not None:",
        "            print(\"[spark] Workspace URL:\", workspace_hint)",
        f"        profile_hint = {profile_hint}",
        "        if profile_hint and profile_hint is not None:",
        "            print(\"[spark] CLI profile:\", profile_hint)",
        f"        cluster_hint = {cluster_hint}",
        "        if cluster_hint and cluster_hint is not None:",
        "            print(\"[spark] Cluster reference:\", cluster_hint)",
        "        contract_backend = context.get('contract_backend', contract_backend)",
        "        data_product_backend = context.get('data_product_backend', data_product_backend)",
        "        data_quality_backend = context.get('data_quality_backend', data_quality_backend)",
        "        governance_store = context.get('governance_store', governance_store)",
    )

    return PipelineStub(
        bootstrap_imports=("build_spark_context",),
        main_lines=main_lines,
    )


register_pipeline_stub("spark", _spark_pipeline_stub)


__all__ = ["_spark_pipeline_stub"]

