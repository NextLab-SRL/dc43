"""Shared package metadata for release tooling and workflows."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PACKAGES = {
    "dc43": {
        "paths": [
            ROOT / "src" / "dc43",
            ROOT / "pyproject.toml",
        ],
        "pyproject": ROOT / "pyproject.toml",
        "version_file": ROOT / "VERSION",
        "pypi": "dc43",
        "tag_prefix": "dc43",
        "package_dir": ".",
    },
    "dc43-demo-app": {
        "paths": [ROOT / "packages" / "dc43-demo-app"],
        "pyproject": ROOT / "packages" / "dc43-demo-app" / "pyproject.toml",
        "version_file": ROOT / "packages" / "dc43-demo-app" / "VERSION",
        "pypi": "dc43-demo-app",
        "tag_prefix": "dc43-demo-app",
        "package_dir": "packages/dc43-demo-app",
    },
    "dc43-service-clients": {
        "paths": [ROOT / "packages" / "dc43-service-clients"],
        "pyproject": ROOT / "packages" / "dc43-service-clients" / "pyproject.toml",
        "version_file": ROOT / "packages" / "dc43-service-clients" / "VERSION",
        "pypi": "dc43-service-clients",
        "tag_prefix": "dc43-service-clients",
        "package_dir": "packages/dc43-service-clients",
    },
    "dc43-service-backends": {
        "paths": [ROOT / "packages" / "dc43-service-backends"],
        "pyproject": ROOT / "packages" / "dc43-service-backends" / "pyproject.toml",
        "version_file": ROOT / "packages" / "dc43-service-backends" / "VERSION",
        "pypi": "dc43-service-backends",
        "tag_prefix": "dc43-service-backends",
        "package_dir": "packages/dc43-service-backends",
    },
    "dc43-integrations": {
        "paths": [ROOT / "packages" / "dc43-integrations"],
        "pyproject": ROOT / "packages" / "dc43-integrations" / "pyproject.toml",
        "version_file": ROOT / "packages" / "dc43-integrations" / "VERSION",
        "pypi": "dc43-integrations",
        "tag_prefix": "dc43-integrations",
        "package_dir": "packages/dc43-integrations",
    },
    "dc43-contracts-app": {
        "paths": [ROOT / "packages" / "dc43-contracts-app"],
        "pyproject": ROOT / "packages" / "dc43-contracts-app" / "pyproject.toml",
        "version_file": ROOT / "packages" / "dc43-contracts-app" / "VERSION",
        "pypi": "dc43-contracts-app",
        "tag_prefix": "dc43-contracts-app",
        "package_dir": "packages/dc43-contracts-app",
    },
}

DEFAULT_RELEASE_ORDER = [
    "dc43",
    "dc43-service-clients",
    "dc43-service-backends",
    "dc43-integrations",
    "dc43-contracts-app",
    "dc43-demo-app",
]

INTERNAL_PACKAGE_NAMES = set(PACKAGES)
