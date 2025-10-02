"""Helpers for retrieving versions of local internal packages."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from _packages import PACKAGES


def _pyproject_path(package: str) -> Path:
    try:
        package_meta = PACKAGES[package]
    except KeyError as exc:  # pragma: no cover - defensive programming
        raise KeyError(f"Unknown internal package: {package}") from exc
    return package_meta["pyproject"]


def _version_file(package: str) -> Path:
    try:
        package_meta = PACKAGES[package]
    except KeyError as exc:  # pragma: no cover - defensive programming
        raise KeyError(f"Unknown internal package: {package}") from exc
    try:
        return package_meta["version_file"]
    except KeyError as exc:  # pragma: no cover - defensive programming
        raise KeyError(f"Missing version file configuration for {package}") from exc


def _load_version(version_file: Path) -> str:
    try:
        return version_file.read_text("utf-8").strip()
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing version file: {version_file}") from exc


def load_versions(packages: Iterable[str]) -> dict[str, str]:
    """Return the declared version for each internal package name provided."""

    versions: dict[str, str] = {}
    for package in packages:
        # Access the pyproject to maintain parity with legacy configuration and
        # surface clearer errors if a package entry is misconfigured.
        _pyproject_path(package)
        # Intentionally read the version file directly so GitHub Actions and
        # packaging share a single source of truth without parsing pyproject.toml.
        version_path = _version_file(package)
        versions[package] = _load_version(version_path)
    return versions
