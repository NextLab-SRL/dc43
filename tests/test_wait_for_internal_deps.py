"""Tests for the release dependency waiting helper."""

from __future__ import annotations

from importlib import util
from pathlib import Path


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "wait_for_internal_deps.py"
    )
    spec = util.spec_from_file_location("wait_for_internal_deps", module_path)
    assert spec and spec.loader
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def test_parse_simple_filenames_extracts_versions() -> None:
    module = _load_module()
    filenames = [
        "dc43_service_clients-0.7.0.0-py3-none-any.whl",
        "dc43-service-clients-0.7.0.0.tar.gz",
        "dc43_service_clients-0.2.1-py3-none-any.whl",
        "unrelated-1.0.0.tar.gz",
    ]

    versions = module._parse_simple_filenames("dc43-service-clients", filenames)

    assert [str(version) for version in versions] == ["0.2.1", "0.7.0.0"]
