from __future__ import annotations

import json
from pathlib import Path

import pytest

from dc43_contracts_app import server


TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dc43_contracts_app"
    / "static"
    / "setup-wizard-template.json"
)


@pytest.fixture(scope="module")
def sample_template() -> dict[str, object]:
    return json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))


def _wizard_field_entries() -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    for module_key, module_meta in server.SETUP_MODULES.items():
        options = module_meta.get("options", {}) if isinstance(module_meta, dict) else {}
        for option_key, option_meta in options.items():
            fields = option_meta.get("fields", []) if isinstance(option_meta, dict) else []
            for field_meta in fields or []:
                name = str(field_meta.get("name") or "").strip()
                if not name:
                    continue
                entries.append((module_key, option_key, name))
    return entries


def test_sample_template_covers_runtime_wizard_fields(sample_template: dict[str, object]) -> None:
    modules = sample_template.get("modules", {})
    assert isinstance(modules, dict), "template modules payload must be a mapping"

    missing_entries: list[str] = []
    blank_entries: list[str] = []

    for module_key, option_key, field_name in _wizard_field_entries():
        module_data = modules.get(module_key)
        if not isinstance(module_data, dict):
            missing_entries.append(f"{module_key}")
            continue
        option_data = module_data.get(option_key)
        if not isinstance(option_data, dict):
            missing_entries.append(f"{module_key}:{option_key}")
            continue
        if field_name not in option_data:
            missing_entries.append(f"{module_key}:{option_key}:{field_name}")
            continue
        value = option_data[field_name]
        if not isinstance(value, str) or not value.strip():
            blank_entries.append(f"{module_key}:{option_key}:{field_name}")

    assert not missing_entries, f"Sample template missing wizard fields: {sorted(missing_entries)}"
    assert not blank_entries, f"Sample template includes empty values: {sorted(blank_entries)}"


def test_sample_template_includes_docs_assistant_configuration(sample_template: dict[str, object]) -> None:
    modules = sample_template.get("modules", {})
    assert isinstance(modules, dict)

    docs_module = modules.get("docs_assistant")
    assert isinstance(docs_module, dict), "docs_assistant entry missing from sample template"

    openai_template = docs_module.get("openai_embedded")
    assert isinstance(openai_template, dict), "docs_assistant openai_embedded sample missing"

    expected_fields = {
        "provider",
        "model",
        "embedding_model",
        "api_key_env",
        "docs_path",
        "index_path",
    }
    missing_fields = expected_fields.difference(openai_template.keys())

    assert not missing_fields, f"docs_assistant sample missing fields: {sorted(missing_fields)}"

    for field in expected_fields:
        value = openai_template[field]
        assert isinstance(value, str) and value.strip(), f"docs_assistant sample value blank for {field}"

