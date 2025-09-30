from __future__ import annotations

from pathlib import Path
import sys

from setuptools import setup


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _internal_dependency_versions import load_versions


_INTERNAL_DEPENDENCIES = [
    "dc43-service-clients",
    "dc43-service-backends",
    "dc43-integrations",
    "dc43-contracts-app",
]

_PACKAGE_VERSIONS = load_versions(_INTERNAL_DEPENDENCIES)


install_requires = [
    f"{name}=={_PACKAGE_VERSIONS[name]}" for name in _INTERNAL_DEPENDENCIES
]
install_requires += [
    "packaging>=21.0",
    "open-data-contract-standard==3.0.2",
]

extras_require = {
    "spark": [
        f"dc43-integrations[spark]=={_PACKAGE_VERSIONS['dc43-integrations']}"
    ],
    "test": [
        "pytest>=7.0",
        "pyspark>=3.4",
        "fastapi",
        "jinja2",
        "python-multipart",
        "httpx",
        f"dc43-contracts-app[spark]=={_PACKAGE_VERSIONS['dc43-contracts-app']}",
    ],
    "demo": [
        "fastapi",
        "uvicorn",
        "jinja2",
        "python-multipart",
        f"dc43-contracts-app[spark]=={_PACKAGE_VERSIONS['dc43-contracts-app']}",
        f"dc43-integrations[spark]=={_PACKAGE_VERSIONS['dc43-integrations']}",
    ],
}

setup(install_requires=install_requires, extras_require=extras_require)
