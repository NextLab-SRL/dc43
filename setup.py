from pathlib import Path

from setuptools import setup

ROOT = Path(__file__).resolve().parent

clients = ROOT / "packages" / "dc43-service-clients"
backends = ROOT / "packages" / "dc43-service-backends"
integrations = ROOT / "packages" / "dc43-integrations"

install_requires = [
    f"dc43-service-clients @ {clients.as_uri()}",
    f"dc43-service-backends @ {backends.as_uri()}",
    f"dc43-integrations @ {integrations.as_uri()}",
    "packaging>=21.0",
    "open-data-contract-standard==3.0.2",
]

extras_require = {
    "spark": [f"dc43-integrations[spark] @ {integrations.as_uri()}"],
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
        f"dc43-integrations[spark] @ {integrations.as_uri()}",
    ],
}

setup(install_requires=install_requires, extras_require=extras_require)
