"""Helpers for retrieving versions of local internal packages."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - Python <3.11 fallback
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    import tomli as tomllib  # type: ignore

from _packages import PACKAGES


def _pyproject_path(package: str) -> Path:
    try:
        package_meta = PACKAGES[package]
    except KeyError as exc:  # pragma: no cover - defensive programming
        raise KeyError(f"Unknown internal package: {package}") from exc
    return package_meta["pyproject"]


def _load_version(pyproject: Path) -> str:
    data = tomllib.loads(pyproject.read_text("utf-8"))
    version = data.get("project", {}).get("version")
    if version is None:  # pragma: no cover - defensive programming
        raise KeyError(f"Missing project.version in {pyproject}")
    return version


def load_versions(packages: Iterable[str]) -> dict[str, str]:
    """Return the declared version for each internal package name provided."""

    versions: dict[str, str] = {}
    for package in packages:
        pyproject = _pyproject_path(package)
        versions[package] = _load_version(pyproject)
    return versions
