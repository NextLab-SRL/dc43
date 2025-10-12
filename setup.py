from __future__ import annotations

import os
from pathlib import Path
import sys

from setuptools import setup


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _internal_dependency_versions import load_versions


_INTERNAL_CORE_DEPENDENCIES = [
    "dc43-service-clients",
    "dc43-service-backends",
    "dc43-integrations",
    "dc43-contracts-app",
]

_OPTIONAL_INTERNAL_DEPENDENCIES = [
    "dc43-demo-app",
]

_ALL_INTERNAL_PACKAGES = _INTERNAL_CORE_DEPENDENCIES + _OPTIONAL_INTERNAL_DEPENDENCIES

_BUILD_DIST_COMMANDS = {"sdist", "bdist_wheel"}


def _building_distribution() -> bool:
    return any(arg in _BUILD_DIST_COMMANDS for arg in sys.argv[1:])


_LOCAL_FALLBACK_PACKAGES = set() if _building_distribution() else set(_ALL_INTERNAL_PACKAGES)


def _use_pypi_versions() -> bool:
    flag = os.getenv("DC43_REQUIRE_PYPI", "")
    return flag.lower() in {"1", "true", "yes", "on"}


def _local_package_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "packages" / name


def _dependency(name: str, *, extras: str | None = None) -> str:
    version = _PACKAGE_VERSIONS[name]
    suffix = f"[{extras}]" if extras else ""
    if name not in _LOCAL_FALLBACK_PACKAGES or _use_pypi_versions():
        return f"{name}{suffix}>={version}"
    candidate = _local_package_path(name)
    if candidate.exists():
        return f"{name}{suffix} @ {candidate.resolve().as_uri()}"
    return f"{name}{suffix}=={version}"

_PACKAGE_VERSIONS = load_versions(_ALL_INTERNAL_PACKAGES)


install_requires = [
    _dependency(name) for name in _INTERNAL_CORE_DEPENDENCIES
]
install_requires += [
    "packaging>=21.0",
    "open-data-contract-standard==3.0.2",
]

extras_require = {
    "spark": [
        _dependency("dc43-integrations", extras="spark")
    ],
    "test": [
        "pytest>=7.0",
        "pyspark>=3.4",
        "fastapi",
        "jinja2",
        "python-multipart",
        "httpx",
        _dependency("dc43-contracts-app", extras="spark"),
    ],
    "demo": [
        "fastapi",
        "uvicorn",
        "jinja2",
        "python-multipart",
        _dependency("dc43-contracts-app", extras="spark"),
        _dependency("dc43-integrations", extras="spark"),
        _dependency("dc43-demo-app"),
    ],
}

setup(install_requires=install_requires, extras_require=extras_require)
