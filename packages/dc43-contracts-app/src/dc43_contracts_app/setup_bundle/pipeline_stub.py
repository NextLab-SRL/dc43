"""Render integration-aware pipeline stubs for the setup bundle."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable, Dict, List, Mapping

CleanStr = Callable[[Any], str | None]


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


def render_pipeline_stub(
    state: Mapping[str, Any],
    *,
    clean_str: CleanStr,
) -> str:
    """Return an integration-aware pipeline stub script."""

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
    docstring_lines.extend(
        [
            "",
            "Update the placeholder identifiers inside :func:`main` before running the",
            "pipeline so that it targets your datasets and contracts.",
            '"""',
        ]
    )

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
            "",
        ]
    )

    import_parts: List[str] = ["load_backends"]
    if hints.key == "spark":
        import_parts.append("build_spark_context")
    elif hints.key == "dlt":
        import_parts.append("build_dlt_context")
    lines.append(f"from bootstrap_pipeline import {', '.join(import_parts)}")
    lines.append("")

    flags = _integration_flags(selected)

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

    lines.extend(
        [
            "",
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
            "",
            "    if integration == 'spark':",
            "        context = build_spark_context(app_name=\"dc43-pipeline-example\")",
            "        spark = context.get('spark')",
            "        if spark is not None:",
            "            print(\"[spark] Spark session initialised:\", spark)",
            f"        runtime_hint = {_IntegrationHints.json_literal(hints.spark_runtime)}",
            "        if runtime_hint and runtime_hint is not None:",
            "            print(\"[spark] Runtime configured in setup:\", runtime_hint)",
            f"        workspace_hint = {_IntegrationHints.json_literal(hints.spark_workspace_url)}",
            "        if workspace_hint and workspace_hint is not None:",
            "            print(\"[spark] Workspace URL:\", workspace_hint)",
            f"        profile_hint = {_IntegrationHints.json_literal(hints.spark_workspace_profile)}",
            "        if profile_hint and profile_hint is not None:",
            "            print(\"[spark] CLI profile:\", profile_hint)",
            f"        cluster_hint = {_IntegrationHints.json_literal(hints.spark_cluster)}",
            "        if cluster_hint and cluster_hint is not None:",
            "            print(\"[spark] Cluster reference:\", cluster_hint)",
            "        contract_backend = context.get('contract_backend', contract_backend)",
            "        data_product_backend = context.get('data_product_backend', data_product_backend)",
            "        data_quality_backend = context.get('data_quality_backend', data_quality_backend)",
            "        governance_store = context.get('governance_store', governance_store)",
            "    elif integration == 'dlt':",
            "        context = build_dlt_context()",
            "        workspace = context.get('workspace')",
            "        if workspace is not None:",
            "            print(\"[dlt] Workspace client initialised:\", workspace)",
            f"        workspace_hint = {_IntegrationHints.json_literal(hints.dlt_workspace_url)}",
            "        if workspace_hint and workspace_hint is not None:",
            "            print(\"[dlt] Workspace host:\", workspace_hint)",
            f"        profile_hint = {_IntegrationHints.json_literal(hints.dlt_workspace_profile)}",
            "        if profile_hint and profile_hint is not None:",
            "            print(\"[dlt] CLI profile:\", profile_hint)",
            f"        pipeline_name = {_IntegrationHints.json_literal(hints.dlt_pipeline_name)}",
            "        if pipeline_name and pipeline_name is not None:",
            "            print(\"[dlt] Pipeline name:\", pipeline_name)",
            f"        notebook_hint = {_IntegrationHints.json_literal(hints.dlt_notebook_path)}",
            "        if notebook_hint and notebook_hint is not None:",
            "            print(\"[dlt] Notebook path:\", notebook_hint)",
            f"        target_hint = {_IntegrationHints.json_literal(hints.dlt_target_schema)}",
            "        if target_hint and target_hint is not None:",
            "            print(\"[dlt] Target schema:\", target_hint)",
            "        contract_backend = context.get('contract_backend', contract_backend)",
            "        data_product_backend = context.get('data_product_backend', data_product_backend)",
            "        data_quality_backend = context.get('data_quality_backend', data_quality_backend)",
            "        governance_store = context.get('governance_store', governance_store)",
            "",
            "    print(\"[suite] Contract backend:\", contract_backend.__class__.__name__)",
            "    print(\"[suite] Data product backend:\", data_product_backend.__class__.__name__)",
            "    print(\"[suite] Data-quality backend:\", data_quality_backend.__class__.__name__)",
            "    print(\"[suite] Governance store:\", governance_store.__class__.__name__)",
            "    print(\"[next] Review the helper functions below and replace placeholders.\")",
        ]
    )

    if flags["contracts"]:
        lines.append("    review_contract_versions(contract_backend)")
    if flags["products"]:
        lines.append("    sync_data_product_catalog(data_product_backend)")
    if flags["quality"]:
        lines.append("    run_quality_checks(data_quality_backend, contract_backend)")
    if flags["governance"]:
        lines.append("    publish_governance_updates(governance_store)")

    lines.extend(
        [
            "",
            "    print(\"[done] Stub completed. Replace the placeholders with real identifiers.\")",
            "",
            "if __name__ == '__main__':",
            "    main()",
            "",
        ]
    )

    return "\n".join(lines)
