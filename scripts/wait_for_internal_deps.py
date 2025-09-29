#!/usr/bin/env python3
"""Poll PyPI until internal package dependencies are available."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    import tomli as tomllib  # type: ignore

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version, InvalidVersion

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _packages import DEFAULT_RELEASE_ORDER, INTERNAL_PACKAGE_NAMES
from _internal_dependency_versions import load_versions

DEFAULT_TIMEOUT = 600
DEFAULT_INTERVAL = 10


META_PACKAGE_NAME = "dc43"
META_PACKAGE_DEPENDENCIES = [
    name for name in DEFAULT_RELEASE_ORDER if name != META_PACKAGE_NAME
]


def _load_dependencies(pyproject: Path) -> list[str]:
    data = tomllib.loads(pyproject.read_text())
    dependencies = data.get("project", {}).get("dependencies") or []
    return list(dependencies)


def _default_internal_requirements(package: str) -> list[str]:
    if package == META_PACKAGE_NAME:
        return META_PACKAGE_DEPENDENCIES
    return []


def _internal_requirements(pyproject: Path, current: str) -> list[Requirement]:
    requirements: list[Requirement] = []
    specs = _load_dependencies(pyproject)
    if not specs:
        specs = _default_internal_requirements(current)
    for spec in specs:
        req = Requirement(spec)
        if req.name in INTERNAL_PACKAGE_NAMES and req.name != current:
            requirements.append(req)
    return requirements


def _fetch_versions(distribution: str) -> list[Version]:
    url = f"https://pypi.org/pypi/{distribution}/json"
    try:
        with urllib.request.urlopen(url) as response:  # nosec B310
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise
    releases = payload.get("releases", {})
    versions: list[Version] = []
    for version, files in releases.items():
        if not files:
            continue
        try:
            versions.append(Version(version))
        except InvalidVersion:
            continue
    return versions


def _specifier_satisfied(specifier: SpecifierSet, versions: Iterable[Version]) -> bool:
    if not specifier:
        return True if list(versions) else False
    for version in versions:
        if version in specifier:
            return True
    return False


def wait_for_dependencies(pyproject: Path, package: str, timeout: int, interval: int) -> None:
    requirements = _internal_requirements(pyproject, package)
    if not requirements:
        return

    deadline = time.monotonic() + timeout
    pending = {req.name: req for req in requirements}
    expected_versions: dict[str, Version] = {
        name: Version(version)
        for name, version in load_versions(pending).items()
    }

    while pending:
        resolved = []
        for name, requirement in pending.items():
            versions = _fetch_versions(name)
            expected = expected_versions.get(name)
            if expected is not None:
                if expected not in versions:
                    continue
                if requirement.specifier and expected not in requirement.specifier:
                    continue
                resolved.append(name)
                continue
            if _specifier_satisfied(requirement.specifier, versions):
                resolved.append(name)
        for name in resolved:
            pending.pop(name, None)
        if not pending:
            return
        if time.monotonic() >= deadline:
            missing = ", ".join(sorted(pending))
            raise TimeoutError(f"Timed out waiting for dependencies: {missing}")
        time.sleep(interval)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pyproject", type=Path, required=True, help="Path to the package pyproject")
    parser.add_argument("--package", required=True, help="Package name being released")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Seconds to wait before failing")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Polling interval in seconds")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        wait_for_dependencies(args.pyproject, args.package, args.timeout, args.interval)
    except TimeoutError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except urllib.error.HTTPError as exc:
        print(f"ERROR querying PyPI: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
