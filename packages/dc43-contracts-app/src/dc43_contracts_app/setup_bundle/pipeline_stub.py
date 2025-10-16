"""Render integration-aware pipeline stubs for the setup bundle."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any, Callable, Dict, List, Mapping, Sequence

CleanStr = Callable[[Any], str | None]


@dataclass(frozen=True)
class _ProjectFile:
    """Description of an additional example file provided by an integration."""

    path: str
    content: str
    executable: bool = False


@dataclass(frozen=True)
class _IntegrationProject:
    """Collection of support files shipped alongside the main stub."""

    root: str
    entrypoint: str
    files: Sequence[_ProjectFile]


@dataclass(frozen=True)
class _ModuleSelection:
    key: str
    option: str
    configuration: Mapping[str, Any]

    def summary(self, *, clean_str: CleanStr) -> str:
        details: List[str] = []
        for field_name in sorted(self.configuration.keys()):
            raw_value = self.configuration[field_name]
            if isinstance(raw_value, Mapping):
                nested: Dict[str, str] = {}
                for nested_key, nested_value in raw_value.items():
                    nested_name = clean_str(nested_key) or str(nested_key)
                    nested_text = clean_str(nested_value)
                    if nested_text is None:
                        if isinstance(nested_value, (int, float)):
                            nested_text = str(nested_value)
                        elif isinstance(nested_value, bool):
                            nested_text = "true" if nested_value else "false"
                        elif nested_value is None:
                            continue
                        else:
                            nested_text = str(nested_value)
                    if nested_name:
                        nested[nested_name] = nested_text
                if nested:
                    details.append(
                        f"{field_name}=" + json.dumps(nested, sort_keys=True)
                    )
                continue
            if isinstance(raw_value, (list, tuple, set)):
                sequence = [item for item in raw_value if item not in (None, "")]
                if sequence:
                    details.append(
                        f"{field_name}="
                        + json.dumps([str(item) for item in sequence], sort_keys=True)
                    )
                continue
            value_text = clean_str(raw_value)
            if value_text is None:
                if isinstance(raw_value, (int, float)):
                    value_text = str(raw_value)
                elif isinstance(raw_value, bool):
                    value_text = "true" if raw_value else "false"
            if value_text:
                details.append(f"{field_name}={value_text}")
        option_text = self.option or "unspecified"
        detail_text = ", ".join(details) if details else "no explicit settings"
        return f"- {self.key} ({option_text}): {detail_text}"


@dataclass(frozen=True)
class _IntegrationHints:
    key: str
    spark_runtime: str | None = None
    spark_workspace_url: str | None = None
    spark_workspace_profile: str | None = None
    spark_cluster: str | None = None
    dlt_workspace_url: str | None = None
    dlt_workspace_profile: str | None = None
    dlt_pipeline_name: str | None = None
    dlt_notebook_path: str | None = None
    dlt_target_schema: str | None = None

    @classmethod
    def from_state(
        cls,
        integration_key: str,
        integration_config: Mapping[str, Any],
        *,
        clean_str: CleanStr,
    ) -> "_IntegrationHints":
        def hint(field: str) -> str | None:
            return clean_str(integration_config.get(field))

        return cls(
            key=integration_key,
            spark_runtime=hint("runtime"),
            spark_workspace_url=hint("workspace_url"),
            spark_workspace_profile=hint("workspace_profile"),
            spark_cluster=hint("cluster_reference"),
            dlt_workspace_url=hint("workspace_url"),
            dlt_workspace_profile=hint("workspace_profile"),
            dlt_pipeline_name=hint("pipeline_name"),
            dlt_notebook_path=hint("notebook_path"),
            dlt_target_schema=hint("target_schema"),
        )

    @staticmethod
    def json_literal(value: str | None) -> str:
        return json.dumps(value) if value else "None"


def _normalise_mapping(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): value for key, value in raw.items()}


def _normalise_selected(
    raw_selected: Mapping[str, Any] | None,
    *,
    clean_str: CleanStr,
) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not isinstance(raw_selected, Mapping):
        return result
    for key, value in raw_selected.items():
        key_text = clean_str(key) or str(key)
        value_text = clean_str(value)
        if key_text and value_text:
            result[key_text] = value_text
    return result


def _module_selections(
    *,
    selected: Mapping[str, str],
    configuration: Mapping[str, Mapping[str, Any]],
) -> List[_ModuleSelection]:
    selections: List[_ModuleSelection] = []
    for key in sorted(selected.keys()):
        option = selected[key]
        module_config = configuration.get(key, {})
        if isinstance(module_config, Mapping):
            config_mapping: Mapping[str, Any] = module_config
        else:
            config_mapping = {}
        selections.append(
            _ModuleSelection(key=key, option=option, configuration=config_mapping)
        )
    return selections


def _integration_flags(selected: Mapping[str, str]) -> Dict[str, bool]:
    return {
        "contracts": bool(selected.get("contracts_backend")),
        "products": bool(selected.get("products_backend")),
        "quality": bool(selected.get("data_quality")),
        "governance": bool(selected.get("governance_store")),
    }


def _coerce_lines(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(item) for item in raw]
    return [str(raw)]


def _coerce_project_file(raw: Any) -> _ProjectFile | None:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        path = raw.get("path")
        content = raw.get("content")
        executable = bool(raw.get("executable"))
    else:
        path = getattr(raw, "path", None)
        content = getattr(raw, "content", None)
        executable = bool(getattr(raw, "executable", False))
    if not path or content is None:
        return None
    return _ProjectFile(path=str(path), content=str(content), executable=executable)


def _coerce_project(raw: Any) -> _IntegrationProject | None:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        root = raw.get("root")
        entrypoint = raw.get("entrypoint")
        files_raw = raw.get("files", [])
    else:
        root = getattr(raw, "root", None)
        entrypoint = getattr(raw, "entrypoint", None)
        files_raw = getattr(raw, "files", [])
    if not root or not entrypoint:
        return None
    files: List[_ProjectFile] = []
    for item in files_raw or []:
        file = _coerce_project_file(item)
        if file is not None:
            files.append(file)
    return _IntegrationProject(root=str(root), entrypoint=str(entrypoint), files=tuple(files))


def _normalise_stub(result: Any) -> _IntegrationStub | None:
    if result is None:
        return None
    if isinstance(result, _IntegrationStub):
        return result

    if isinstance(result, Mapping):
        bootstrap_imports = _coerce_lines(result.get("bootstrap_imports"))
        helper_functions = _coerce_lines(result.get("helper_functions"))
        main_lines = _coerce_lines(result.get("main_lines"))
        tail_lines = _coerce_lines(result.get("tail_lines"))
        additional_imports = _coerce_lines(result.get("additional_imports"))
        project = _coerce_project(result.get("project"))
        return _IntegrationStub(
            bootstrap_imports=tuple(bootstrap_imports),
            helper_functions=tuple(helper_functions),
            main_lines=tuple(main_lines),
            tail_lines=tuple(tail_lines),
            additional_imports=tuple(additional_imports),
            project=project,
        )

    bootstrap_imports = _coerce_lines(getattr(result, "bootstrap_imports", ()))
    helper_functions = _coerce_lines(getattr(result, "helper_functions", ()))
    main_lines = _coerce_lines(getattr(result, "main_lines", ()))
    tail_lines = _coerce_lines(getattr(result, "tail_lines", ()))
    additional_imports = _coerce_lines(getattr(result, "additional_imports", ()))
    project = _coerce_project(getattr(result, "project", None))
    return _IntegrationStub(
        bootstrap_imports=tuple(bootstrap_imports),
        helper_functions=tuple(helper_functions),
        main_lines=tuple(main_lines),
        tail_lines=tuple(tail_lines),
        additional_imports=tuple(additional_imports),
        project=project,
    )


def _load_external_stub(
    key: str,
    *,
    hints: _IntegrationHints,
    flags: Mapping[str, bool],
) -> _IntegrationStub | None:
    try:
        from dc43_integrations import setup_bundle as integration_setup_bundle  # type: ignore
    except Exception:
        return None

    get_pipeline_stub = getattr(integration_setup_bundle, "get_pipeline_stub", None)
    if not callable(get_pipeline_stub):
        return None

    hints_payload = asdict(hints)
    try:
        external = get_pipeline_stub(  # type: ignore[misc]
            key,
            hints=hints_payload,
            flags=dict(flags),
            json_literal=_IntegrationHints.json_literal,
        )
    except Exception:
        return None

    return _normalise_stub(external)


def _spark_stub(hints: _IntegrationHints) -> _IntegrationStub:
    runtime_hint = _IntegrationHints.json_literal(hints.spark_runtime)
    workspace_hint = _IntegrationHints.json_literal(hints.spark_workspace_url)
    profile_hint = _IntegrationHints.json_literal(hints.spark_workspace_profile)
    cluster_hint = _IntegrationHints.json_literal(hints.spark_cluster)

    lines = [
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
    ]

    return _IntegrationStub(
        bootstrap_imports=("build_spark_context",),
        main_lines=tuple(lines),
    )


def _dlt_stub(hints: _IntegrationHints) -> _IntegrationStub:
    workspace_hint = _IntegrationHints.json_literal(hints.dlt_workspace_url)
    profile_hint = _IntegrationHints.json_literal(hints.dlt_workspace_profile)
    pipeline_name = _IntegrationHints.json_literal(hints.dlt_pipeline_name)
    notebook_hint = _IntegrationHints.json_literal(hints.dlt_notebook_path)
    target_hint = _IntegrationHints.json_literal(hints.dlt_target_schema)

    lines = [
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
    ]

    return _IntegrationStub(
        bootstrap_imports=("build_dlt_context",),
        main_lines=tuple(lines),
    )


def _fallback_stub(
    key: str,
    *,
    hints: _IntegrationHints,
) -> _IntegrationStub:
    if key == "spark":
        return _spark_stub(hints)
    if key == "dlt":
        return _dlt_stub(hints)
    return _IntegrationStub()


def get_integration_stub(
    key: str,
    *,
    hints: _IntegrationHints,
    flags: Mapping[str, bool],
) -> _IntegrationStub:
    stub = _load_external_stub(key, hints=hints, flags=flags)
    if stub is not None:
        return stub
    return _fallback_stub(key, hints=hints)


def render_pipeline_stub(
    state: Mapping[str, Any],
    *,
    clean_str: CleanStr,
) -> PipelineExample:
    """Return the integration-aware pipeline example assets."""

    configuration_raw = state.get("configuration") if isinstance(state, Mapping) else {}
    selected_raw = state.get("selected_options") if isinstance(state, Mapping) else {}

    configuration = _normalise_mapping(configuration_raw)
    selected = _normalise_selected(selected_raw, clean_str=clean_str)

    integration_key = selected.get("pipeline_integration", "") or ""
    integration_config_raw = configuration.get("pipeline_integration", {})
    integration_config: Mapping[str, Any]
    if isinstance(integration_config_raw, Mapping):
        integration_config = integration_config_raw
    else:
        integration_config = {}
    hints = _IntegrationHints.from_state(
        integration_key,
        integration_config,
        clean_str=clean_str,
    )

    module_summaries = [
        selection.summary(clean_str=clean_str)
        for selection in _module_selections(
            selected=selected,
            configuration={
                key: value
                for key, value in configuration.items()
                if isinstance(value, Mapping)
            },
        )
    ]

    flags = _integration_flags(selected)
    stub = get_integration_stub(integration_key, hints=hints, flags=flags)

    contract_id_literal = json.dumps("replace-with-contract-id")
    contract_version_literal = json.dumps("replace-with-contract-version")
    data_product_id_literal = json.dumps("replace-with-data-product-id")
    dataset_version_literal = json.dumps("replace-with-dataset-version")
    output_port_literal = json.dumps("replace-with-output-port")

    docstring_lines = [
        '"""Example pipeline stub generated by the dc43 setup wizard.',
        "",
        "Selected modules recorded during export:",
    ]
    if module_summaries:
        docstring_lines.extend(f"    {line}" for line in module_summaries)
    else:
        docstring_lines.append("    (no module selections were recorded)")
    docstring_tail: List[str] = [""]
    if stub.project is not None:
        docstring_tail.extend(
            [
                "This stub wires in the integration-owned example project located under",
                f"`examples/{stub.project.root}`. Explore the modules in that folder to",
                "customise transformations, IO strategies, and service interactions.",
                "",
            ]
        )
    docstring_tail.extend(
        [
            "Update the placeholder identifiers inside :func:`main` before running the",
            "pipeline so that it targets your datasets and contracts.",
            '"""',
        ]
    )
    docstring_lines.extend(docstring_tail)

    lines: List[str] = ["#!/usr/bin/env python3", ""]
    lines.extend(docstring_lines)
    lines.extend(
        [
            "",
            "from __future__ import annotations",
            "",
            "import sys",
            "from pathlib import Path",
            "",
            "BUNDLE_ROOT = Path(__file__).resolve().parent.parent",
            "sys.path.insert(0, str(BUNDLE_ROOT / \"scripts\"))",
        ]
    )
    if stub.project is not None:
        lines.extend(
            [
                f"PIPELINE_ROOT = BUNDLE_ROOT / 'examples' / {json.dumps(stub.project.root)}",
                "sys.path.insert(0, str(PIPELINE_ROOT))",
            ]
        )
    lines.append("")

    if stub.additional_imports:
        lines.extend(stub.additional_imports)
        if lines[-1] != "":
            lines.append("")

    import_parts: List[str] = ["load_backends"]
    for name in stub.bootstrap_imports:
        if name not in import_parts:
            import_parts.append(name)
    lines.append(f"from bootstrap_pipeline import {', '.join(import_parts)}")
    lines.append("")

    if flags["contracts"]:
        lines.extend(
            [
                "",
                "def review_contract_versions(contract_backend) -> None:",
                "    \"\"\"Outline how to load contract revisions before running tasks.\"\"\"",
                "    print(\"[contracts] backend:\", contract_backend.__class__.__name__)",
                f"    contract_id = {contract_id_literal}",
                "    print(",
                f"        \"[contracts] Inspect available contracts with contract_backend.list_versions({contract_id_literal})\"",
                "    )",
                "    # versions = contract_backend.list_versions(contract_id)",
                "    # if versions:",
                "    #     latest_version = versions[-1]",
                "    #     contract = contract_backend.get(contract_id, latest_version)",
                "    #     print(\"Loaded contract title:\", contract.info.title.default)",
            ]
        )

    if flags["products"]:
        lines.extend(
            [
                "",
                "def sync_data_product_catalog(data_product_backend) -> None:",
                "    \"\"\"Guide registration of ports in the configured backend.\"\"\"",
                "    print(\"[data_products] backend:\", data_product_backend.__class__.__name__)",
                f"    data_product_id = {data_product_id_literal}",
                f"    output_port = {output_port_literal}",
                "    print(\"[data_products] Publish new versions with data_product_backend.register_output_port(...).\")",
                "    # from dc43_service_clients.odps import DataProductOutputPort",
                "    # data_product_backend.register_output_port(",
                "    #     data_product_id=data_product_id,",
                "    #     port=DataProductOutputPort(name=output_port, description=\"replace-with-description\"),",
                "    # )",
            ]
        )

    if flags["quality"]:
        lines.extend(
            [
                "",
                "def run_quality_checks(data_quality_backend, contract_backend) -> None:",
                "    \"\"\"Explain how to evaluate observations using stored contracts.\"\"\"",
                "    print(\"[data_quality] backend:\", data_quality_backend.__class__.__name__)",
                f"    contract_id = {contract_id_literal}",
                f"    contract_version = {contract_version_literal}",
                "    print(\"[data_quality] Load a contract before building observation payloads.\")",
                "    # contract = contract_backend.get(contract_id, contract_version)",
                "    # from dc43_service_clients.data_quality import ObservationPayload",
                "    # payload = ObservationPayload(dataset_id=contract_id, observations=[])",
                "    # result = data_quality_backend.evaluate(contract=contract, payload=payload)",
                "    # print(\"Validation status:\", result.status)",
            ]
        )

    if flags["governance"]:
        lines.extend(
            [
                "",
                "def publish_governance_updates(governance_store) -> None:",
                "    \"\"\"Persist validation status and pipeline activity metadata.\"\"\"",
                "    print(\"[governance] store:\", governance_store.__class__.__name__)",
                f"    contract_id = {contract_id_literal}",
                f"    contract_version = {contract_version_literal}",
                f"    dataset_id = {data_product_id_literal}",
                f"    dataset_version = {dataset_version_literal}",
                "    print(\"[governance] Link datasets and contracts with governance_store.link_dataset_contract(...).\")",
                "    # governance_store.link_dataset_contract(",
                "    #     dataset_id=dataset_id,",
                "    #     dataset_version=dataset_version,",
                "    #     contract_id=dataset_id,",
                "    #     contract_version=contract_version,",
                "    # )",
                "    # governance_store.record_pipeline_event(",
                "    #     dataset_id=dataset_id,",
                "    #     dataset_version=dataset_version,",
                "    #     contract_id=dataset_id,",
                "    #     contract_version=contract_version,",
                "    #     event={\"status\": \"replace-with-status\"},",
                "    # )",
            ]
        )

    if stub.helper_functions:
        if not lines or lines[-1] != "":
            lines.append("")
        lines.extend(stub.helper_functions)

    if not lines or lines[-1] != "":
        lines.append("")

    main_lines: List[str] = [
        "def main() -> None:",
        "    \"\"\"Entry-point for the generated pipeline stub.\"\"\"",
        "    suite = load_backends()",
        "    contract_backend = suite.contract",
        "    data_product_backend = suite.data_product",
        "    data_quality_backend = suite.data_quality",
        "    governance_store = suite.governance_store",
        f"    integration = {json.dumps(hints.key)}",
        "    print(\"[bundle] Configuration root:\", BUNDLE_ROOT)",
        "    if integration:",
        "        print(\"[bundle] Pipeline integration from setup:\", integration)",
        "    else:",
        "        print(\"[bundle] No pipeline integration was selected in the wizard.\")",
    ]

    if stub.main_lines:
        if main_lines[-1] != "":
            main_lines.append("")
        main_lines.extend(list(stub.main_lines))

    main_lines.extend(
        [
            "",
            "    print(\"[suite] Contract backend:\", contract_backend.__class__.__name__)",
            "    print(\"[suite] Data product backend:\", data_product_backend.__class__.__name__)",
            "    print(\"[suite] Data-quality backend:\", data_quality_backend.__class__.__name__)",
            "    print(\"[suite] Governance store:\", governance_store.__class__.__name__)",
            "    print(\"[next] Review the helper functions below and replace placeholders.\")",
        ]
    )

    if stub.tail_lines:
        if main_lines[-1] != "":
            main_lines.append("")
        main_lines.extend(list(stub.tail_lines))

    if flags["contracts"]:
        main_lines.append("    review_contract_versions(contract_backend)")
    if flags["products"]:
        main_lines.append("    sync_data_product_catalog(data_product_backend)")
    if flags["quality"]:
        main_lines.append("    run_quality_checks(data_quality_backend, contract_backend)")
    if flags["governance"]:
        main_lines.append("    publish_governance_updates(governance_store)")

    main_lines.extend(
        [
            "",
            "    print(\"[done] Stub completed. Replace the placeholders with real identifiers.\")",
        ]
    )

    lines.extend(main_lines)

    lines.extend(
        [
            "",
            "if __name__ == '__main__':",
            "    main()",
            "",
        ]
    )

    support_files: List[PipelineExampleFile] = []
    if stub.project is not None:
        for project_file in stub.project.files:
            relative = f"examples/{stub.project.root}/{project_file.path}".replace("//", "/")
            support_files.append(
                PipelineExampleFile(
                    path=relative,
                    content=project_file.content,
                    executable=project_file.executable,
                )
            )

    script_text = "\n".join(lines)
    return PipelineExample(
        entrypoint_path="examples/pipeline_stub.py",
        entrypoint_content=script_text,
        entrypoint_executable=True,
        support_files=tuple(support_files),
    )
@dataclass(frozen=True)
class _IntegrationStub:
    """Structured fragments contributed by integration providers."""

    bootstrap_imports: Sequence[str] = ()
    helper_functions: Sequence[str] = ()
    main_lines: Sequence[str] = ()
    tail_lines: Sequence[str] = ()
    additional_imports: Sequence[str] = ()
    project: _IntegrationProject | None = None


@dataclass(frozen=True)
class PipelineExampleFile:
    """File included in the setup bundle example project."""

    path: str
    content: str
    executable: bool = False


@dataclass(frozen=True)
class PipelineExample:
    """Entrypoint script and associated files for the example pipeline."""

    entrypoint_path: str
    entrypoint_content: str
    entrypoint_executable: bool = True
    support_files: Sequence[PipelineExampleFile] = ()

