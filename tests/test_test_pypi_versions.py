from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from packaging.version import Version

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "test_pypi_versions.py"
spec = importlib.util.spec_from_file_location("test_pypi_versions", MODULE_PATH)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_build_test_version_uses_release_segment():
    assert (
        module.build_test_version("0.31.0.0", stage="rc", identifier="42")
        == "0.31.0.0rc42"
    )
    assert (
        module.build_test_version("0.31.0.0rc1", stage="rc", identifier="007")
        == "0.31.0.0rc007"
    )


def test_build_test_version_dev_stage():
    assert (
        module.build_test_version("1.2.3", stage="dev", identifier="5") == "1.2.3.dev5"
    )


def test_determine_identifier_prefers_explicit(monkeypatch):
    assert module.determine_identifier(explicit="123", run_identifier="456") == "123"


@pytest.mark.parametrize("value", ["abc", "123abc", " 12 3 "])
def test_determine_identifier_rejects_non_numeric(value):
    with pytest.raises(ValueError):
        module.determine_identifier(explicit=value, run_identifier=None)


def test_determine_identifier_falls_back_to_timestamp(monkeypatch):
    monkeypatch.setattr(module, "_timestamp", lambda: "20240101010101")
    assert (
        module.determine_identifier(explicit=None, run_identifier=None)
        == "20240101010101"
    )


def test_determine_identifier_ignores_blank_inputs(monkeypatch):
    monkeypatch.setattr(module, "_timestamp", lambda: "20240102020202")
    assert (
        module.determine_identifier(explicit="  \t  ", run_identifier=None)
        == "20240102020202"
    )


def test_apply_test_version_rewrites_version_file(tmp_path, monkeypatch):
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.1.0\n", encoding="utf-8")

    with monkeypatch.context() as ctx:
        ctx.setitem(module.PACKAGES, "example", {"version_file": version_file})
        info = module.apply_test_version("example", stage="rc", identifier="9")

    assert info.package == "example"
    assert info.base_version == "0.1.0"
    assert info.test_version == "0.1.0rc9"
    assert version_file.read_text(encoding="utf-8") == "0.1.0rc9\n"
    assert info.dependency_rewrites == []


def test_apply_for_packages_returns_summary(tmp_path, monkeypatch):
    version_one = tmp_path / "VERSION.one"
    version_two = tmp_path / "VERSION.two"
    version_one.write_text("0.1.0\n", encoding="utf-8")
    version_two.write_text("1.0.0rc1\n", encoding="utf-8")

    with monkeypatch.context() as ctx:
        ctx.setitem(
            module.PACKAGES,
            "pkg-one",
            {"version_file": version_one},
        )
        ctx.setitem(
            module.PACKAGES,
            "pkg-two",
            {"version_file": version_two},
        )
        results = module.apply_for_packages(
            ["pkg-one", "pkg-two"], stage="rc", identifier="5"
        )

    assert [result.package for result in results] == ["pkg-one", "pkg-two"]
    assert version_one.read_text(encoding="utf-8") == "0.1.0rc5\n"
    assert version_two.read_text(encoding="utf-8") == "1.0.0rc5\n"


def test_apply_test_version_rewrites_internal_dependencies(tmp_path, monkeypatch):
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.31.0.0\n", encoding="utf-8")
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
dependencies = [
  "dc43-core>=0.31.0.0",
  "dc43-service-backends>=0.31.0.0",
  "something-else>=1.0",
]

[project.optional-dependencies]
spark = [
  "dc43-service-backends[spark]>=0.31.0.0",
]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with monkeypatch.context() as ctx:
        ctx.setitem(
            module.PACKAGES,
            "example",
            {"version_file": version_file, "pyproject": pyproject},
        )
        info = module.apply_test_version("example", stage="rc", identifier="5")

    content = pyproject.read_text(encoding="utf-8")
    assert "dc43-core>=0.31.0.0rc0" in content
    assert "dc43-service-backends>=0.31.0.0rc0" in content
    assert "dc43-service-backends[spark]>=0.31.0.0rc0" in content
    assert len(info.dependency_rewrites) == 3
    assert all(rewrite.path == pyproject for rewrite in info.dependency_rewrites)


def test_format_summary_rows(tmp_path, monkeypatch):
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.1.0\n", encoding="utf-8")

    with monkeypatch.context() as ctx:
        ctx.setitem(module.PACKAGES, "pkg", {"version_file": version_file})
        info = module.apply_test_version("pkg", stage="rc", identifier="10")

    summary = module._format_summary_rows([info])
    assert summary == [
        "### Test PyPI versions",
        "| Package | Base version | Test version |",
        "| --- | --- | --- |",
        "| pkg | 0.1.0 | 0.1.0rc10 |",
    ]


def test_rc_versions_sort_before_release():
    rc_version = module.build_test_version("0.31.0.0", stage="rc", identifier="1")
    assert Version(rc_version) < Version("0.31.0.0")


def test_dev_versions_sort_before_rc_and_release():
    dev_version = module.build_test_version("0.31.0.0", stage="dev", identifier="2")
    rc_version = module.build_test_version("0.31.0.0", stage="rc", identifier="1")
    assert Version(dev_version) < Version(rc_version) < Version("0.31.0.0")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__]))
