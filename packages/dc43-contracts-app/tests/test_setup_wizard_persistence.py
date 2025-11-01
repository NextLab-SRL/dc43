"""Verify setup wizard submissions persist every provided field."""

from __future__ import annotations

import io
import json
import zipfile

from typing import Iterable, Tuple

import pytest
import tomllib
from starlette.testclient import TestClient

from dc43_contracts_app import server
from dc43_contracts_app.config import config_to_mapping as contracts_config_to_mapping
from dc43_service_backends.config import (
    config_to_mapping as service_config_to_mapping,
)


def _option_fields(module_key: str, option_key: str) -> Tuple[str, ...]:
    option = server.SETUP_MODULES[module_key]["options"][option_key]
    fields: list[str] = []
    for field in option.get("fields", []) or []:
        name = field.get("name")
        if not name:
            continue
        fields.append(str(name))
    return tuple(fields)


def _wizard_cases() -> Iterable[object]:
    for module_key, module_meta in server.SETUP_MODULES.items():
        for option_key in module_meta.get("options", {}):
            field_names = _option_fields(module_key, option_key)
            if not field_names:
                continue
            yield pytest.param(
                module_key,
                option_key,
                field_names,
                id=f"{module_key}-{option_key}",
            )


WIZARD_CASES = tuple(_wizard_cases())


@pytest.fixture()
def setup_wizard_client(tmp_path, monkeypatch):
    """Return a TestClient backed by an isolated setup workspace."""

    monkeypatch.setenv("DC43_CONTRACTS_APP_STATE_DIR", str(tmp_path))
    server._ACTIVE_CONFIG = None  # type: ignore[attr-defined]
    server.configure_from_config()
    server.reset_setup_state()
    with TestClient(server.app) as client:
        yield client
    server._ACTIVE_CONFIG = None  # type: ignore[attr-defined]


@pytest.mark.parametrize("module_key, option_key, field_names", WIZARD_CASES)
def test_setup_step_two_persists_all_fields(
    setup_wizard_client: TestClient,
    module_key: str,
    option_key: str,
    field_names: Tuple[str, ...],
) -> None:
    """Posting step two should store every value included in the payload."""

    server.save_setup_state(
        {
            "current_step": 2,
            "selected_options": {module_key: option_key},
            "configuration": {},
            "completed": False,
        }
    )

    form_data = {"step": "2"}
    expected = {}
    for index, field_name in enumerate(field_names, start=1):
        value = f"value-{index}-{module_key}-{option_key}"
        form_data[f"config__{module_key}__{field_name}"] = value
        expected[field_name] = value

    response = setup_wizard_client.post("/setup", data=form_data)
    assert response.history, "expected a redirect after saving configuration"
    assert response.history[0].status_code == 303
    assert response.status_code == 200

    state = server.load_setup_state()
    configuration = state.get("configuration", {})
    module_config = configuration.get(module_key, {})

    for field_name, value in expected.items():
        assert module_config.get(field_name) == value
    assert state.get("current_step") == 3
    assert state.get("completed") is False

    export_response = setup_wizard_client.get("/setup/export")
    assert export_response.status_code == 200

    with zipfile.ZipFile(io.BytesIO(export_response.content), "r") as archive:
        payload = json.loads(archive.read("dc43-setup/configuration.json"))
        toml_files = {
            name: archive.read(name).decode("utf-8")
            for name in archive.namelist()
            if name.endswith(".toml")
        }

    modules = {
        module["key"]: module for module in payload.get("modules", []) if isinstance(module, dict)
    }
    assert module_key in modules, f"export missing module entry for {module_key}"

    exported_settings = modules[module_key].get("settings", {})
    assert isinstance(exported_settings, dict)
    for field_name, value in expected.items():
        assert (
            exported_settings.get(field_name) == value
        ), f"exported settings missing {field_name} for {module_key}"

    module_toml_path = f"dc43-setup/config/modules/{module_key.replace('/', '-')}.toml"
    assert module_toml_path in toml_files, f"missing TOML export for module {module_key}"
    module_toml_text = toml_files[module_toml_path]

    for field_name, value in expected.items():
        assert (
            value in module_toml_text
        ), f"module TOML missing value for {field_name} in {module_key}"

    for field_name, value in expected.items():
        assert any(
            value in contents for contents in toml_files.values()
        ), f"no TOML file retained {field_name} for {module_key}"

    service_toml_path = "dc43-setup/config/dc43-service-backends.toml"
    assert service_toml_path in toml_files
    contracts_app_toml_path = "dc43-setup/config/dc43-contracts-app.toml"
    assert contracts_app_toml_path in toml_files

    exported_service = tomllib.loads(toml_files[service_toml_path])
    exported_contracts = tomllib.loads(toml_files[contracts_app_toml_path])

    service_config = server._service_backends_config_from_state(state)
    assert service_config is not None
    expected_service_mapping = service_config_to_mapping(service_config)
    assert exported_service == expected_service_mapping

    contracts_config = server._contracts_app_config_from_state(state)
    assert contracts_config is not None
    expected_contracts_mapping = contracts_config_to_mapping(contracts_config)
    assert exported_contracts == expected_contracts_mapping
