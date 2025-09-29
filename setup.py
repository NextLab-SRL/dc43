from pathlib import Path
from typing import Dict

import tomllib
from setuptools import setup


def _load_version(package_name: str) -> str:
    """Read the version of a local package from its ``pyproject.toml`` file."""

    pyproject_path = (
        Path(__file__).resolve().parent
        / "packages"
        / package_name
        / "pyproject.toml"
    )

    with pyproject_path.open("rb") as file:
        data = tomllib.load(file)

    return data["project"]["version"]


PACKAGE_VERSIONS: Dict[str, str] = {
    name: _load_version(name)
    for name in (
        "dc43-service-clients",
        "dc43-service-backends",
        "dc43-integrations",
    )
}

install_requires = [
    f"dc43-service-clients=={PACKAGE_VERSIONS['dc43-service-clients']}",
    f"dc43-service-backends=={PACKAGE_VERSIONS['dc43-service-backends']}",
    f"dc43-integrations=={PACKAGE_VERSIONS['dc43-integrations']}",
    "packaging>=21.0",
    "open-data-contract-standard==3.0.2",
]

extras_require = {
    "spark": [
        f"dc43-integrations[spark]=={PACKAGE_VERSIONS['dc43-integrations']}"
    ],
    "test": [
        "pytest>=7.0",
        "pyspark>=3.4",
        "fastapi",
        "jinja2",
        "python-multipart",
        "httpx",
    ],
    "demo": [
        "fastapi",
        "uvicorn",
        "jinja2",
        "python-multipart",
        f"dc43-integrations[spark]=={PACKAGE_VERSIONS['dc43-integrations']}",
    ],
}

setup(install_requires=install_requires, extras_require=extras_require)
