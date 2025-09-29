#!/usr/bin/env python3
"""Release helper CLI for dc43 packages.

This script automates tagging and pushing releases for one or more packages. It
figures out which packages changed since their last release, validates that the
version in each `pyproject.toml` is new, verifies that the version is not
already published on PyPI, and optionally creates and pushes annotated tags.

Usage examples:

* Preview which packages changed and would be released:

    python scripts/release.py

* Release the packages that changed, creating and pushing tags off the current
  HEAD commit:

    python scripts/release.py --apply --push

* Release a specific subset of packages from a particular commit:

    python scripts/release.py --packages dc43-service-clients dc43-integrations \
        --commit 0123abcd --apply

By default the script only prints the plan (dry run). Use ``--apply`` to create
local tags and ``--push`` to push them to ``origin``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

try:  # Python >=3.11
    import tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for older runtimes
    import tomli as tomllib  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = {
    "dc43": {
        "paths": [ROOT / "src" / "dc43", ROOT / "pyproject.toml"],
        "pyproject": ROOT / "pyproject.toml",
        "pypi": "dc43",
        "tag_prefix": "dc43",
    },
    "dc43-service-clients": {
        "paths": [ROOT / "packages" / "dc43-service-clients"],
        "pyproject": ROOT / "packages" / "dc43-service-clients" / "pyproject.toml",
        "pypi": "dc43-service-clients",
        "tag_prefix": "dc43-service-clients",
    },
    "dc43-service-backends": {
        "paths": [ROOT / "packages" / "dc43-service-backends"],
        "pyproject": ROOT / "packages" / "dc43-service-backends" / "pyproject.toml",
        "pypi": "dc43-service-backends",
        "tag_prefix": "dc43-service-backends",
    },
    "dc43-integrations": {
        "paths": [ROOT / "packages" / "dc43-integrations"],
        "pyproject": ROOT / "packages" / "dc43-integrations" / "pyproject.toml",
        "pypi": "dc43-integrations",
        "tag_prefix": "dc43-integrations",
    },
}


@dataclass
class ReleasePlan:
    package: str
    version: str
    tag: str
    commit: str
    changed_files: List[str] = field(default_factory=list)
    last_tag: Optional[str] = None
    pypi_exists: Optional[bool] = None
    will_tag: bool = False
    warnings: List[str] = field(default_factory=list)

    @property
    def needs_release(self) -> bool:
        if self.pypi_exists:
            return False
        if self.last_tag is None:
            return True
        if not self.changed_files:
            return False
        return True


class ReleaseError(RuntimeError):
    """Fatal error when preparing a release."""


def run_git(*args: str, check: bool = True) -> str:
    result = subprocess.run(["git", *args], cwd=ROOT, check=check, capture_output=True, text=True)
    return result.stdout.strip()


def ensure_clean_worktree() -> None:
    status = run_git("status", "--porcelain")
    if status:
        raise ReleaseError("Your working tree has uncommitted changes. Commit or stash them before releasing.")


def package_version(package: str) -> str:
    pyproject = PACKAGES[package]["pyproject"]
    try:
        data = tomllib.loads(pyproject.read_text())
    except FileNotFoundError as exc:  # pragma: no cover - guard for misconfiguration
        raise ReleaseError(f"Missing pyproject for {package}: {pyproject}") from exc
    try:
        return data["project"]["version"]
    except KeyError as exc:  # pragma: no cover
        raise ReleaseError(f"project.version missing in {pyproject}") from exc


def planned_tag(package: str, version: str) -> str:
    prefix = PACKAGES[package]["tag_prefix"]
    return f"{prefix}-v{version}"


def latest_tag(package: str) -> Optional[str]:
    prefix = PACKAGES[package]["tag_prefix"]
    pattern = f"{prefix}-v*"
    output = run_git("tag", "--list", pattern, "--sort=-creatordate")
    tags = [line.strip() for line in output.splitlines() if line.strip()]
    return tags[0] if tags else None


def changed_since(package: str, ref: Optional[str], commit: str) -> List[str]:
    rel_paths = [path.relative_to(ROOT) for path in PACKAGES[package]["paths"]]
    if not rel_paths:
        return []
    if ref is None:
        output = run_git("ls-tree", "--name-only", "-r", commit, *map(str, rel_paths))
    else:
        output = run_git("diff", "--name-only", f"{ref}..{commit}", *map(str, rel_paths))
    entries = {line.strip() for line in output.splitlines() if line.strip()}
    return sorted(entries)


def version_on_pypi(distribution: str, version: str) -> Optional[bool]:
    url = f"https://pypi.org/pypi/{distribution}/json"
    try:
        with urllib.request.urlopen(url) as response:  # nosec B310 - read-only GET
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise ReleaseError(f"Failed to query PyPI for {distribution}: {exc}") from exc
    releases = payload.get("releases", {})
    return version in releases and bool(releases[version])


def build_plan(packages: Iterable[str], commit: str, skip_pypi: bool) -> List[ReleasePlan]:
    plans: List[ReleasePlan] = []
    for package in packages:
        version = package_version(package)
        tag = planned_tag(package, version)
        last = latest_tag(package)
        changed = changed_since(package, last, commit)
        plan = ReleasePlan(
            package=package,
            version=version,
            tag=tag,
            commit=commit,
            changed_files=changed,
            last_tag=last,
        )
        try:
            existing = run_git("rev-parse", tag)
        except subprocess.CalledProcessError:
            existing = ""
        if existing:
            plan.warnings.append(f"Tag {tag} already exists (commit {existing}). Bump the version before releasing.")
        if not skip_pypi:
            try:
                exists = version_on_pypi(PACKAGES[package]["pypi"], version)
            except ReleaseError as exc:
                plan.warnings.append(str(exc))
                exists = None
            plan.pypi_exists = exists
            if exists:
                plan.warnings.append(f"Version {version} is already published on PyPI. Bump the version before releasing.")
        plans.append(plan)
    return plans


def apply_plan(plans: Iterable[ReleasePlan], push: bool) -> None:
    for plan in plans:
        if plan.warnings:
            raise ReleaseError(
                f"Cannot tag {plan.package}:\n" + "\n".join(f"  - {message}" for message in plan.warnings)
            )
        if not plan.needs_release:
            continue
        message = f"Release {plan.package} {plan.version}"
        subprocess.run(
            ["git", "tag", "-a", plan.tag, plan.commit, "-m", message],
            cwd=ROOT,
            check=True,
        )
        plan.will_tag = True
        if push:
            subprocess.run(["git", "push", "origin", plan.tag], cwd=ROOT, check=True)


def format_plan(plan: ReleasePlan) -> str:
    lines = [f"Package: {plan.package}"]
    lines.append(f"  version: {plan.version}")
    lines.append(f"  tag: {plan.tag}")
    lines.append(f"  commit: {plan.commit}")
    if plan.last_tag:
        lines.append(f"  last tag: {plan.last_tag}")
    if plan.pypi_exists is True:
        lines.append("  PyPI: already published")
    elif plan.pypi_exists is False:
        lines.append("  PyPI: available")
    elif plan.pypi_exists is None:
        lines.append("  PyPI: unknown (skipped)")
    if plan.changed_files:
        lines.append("  changed files:")
        lines.extend(f"    - {path}" for path in plan.changed_files)
    else:
        lines.append("  changed files: none detected")
    if plan.warnings:
        lines.append("  warnings:")
        lines.extend(f"    - {message}" for message in plan.warnings)
    lines.append(f"  needs release: {'yes' if plan.needs_release else 'no'}")
    if plan.will_tag:
        lines.append("  status: tagged")
    return "\n".join(lines)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--packages",
        nargs="+",
        choices=sorted(PACKAGES.keys()),
        help="Specific packages to evaluate. Defaults to all packages.",
    )
    parser.add_argument(
        "--commit",
        default="HEAD",
        help="Commit to tag. Defaults to HEAD.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create the annotated tags for packages that need a release.",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Push newly created tags to origin (implies --apply).",
    )
    parser.add_argument(
        "--skip-pypi",
        action="store_true",
        help="Skip PyPI version checks (useful when offline).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    packages = args.packages or sorted(PACKAGES.keys())
    if args.push:
        args.apply = True
    if args.apply:
        ensure_clean_worktree()
    plans = build_plan(packages, args.commit, skip_pypi=args.skip_pypi)
    for plan in plans:
        print(format_plan(plan))
        print()
    actionable = [plan for plan in plans if plan.needs_release]
    if not actionable:
        print("No packages require a release.")
    else:
        print("Packages requiring release:")
        for plan in actionable:
            print(f"  - {plan.package} ({plan.tag})")
    if args.apply:
        try:
            apply_plan(actionable, push=args.push)
        except ReleaseError as exc:
            print(f"\nERROR: {exc}", file=sys.stderr)
            return 1
        except subprocess.CalledProcessError as exc:
            print(f"\nERROR: {exc}", file=sys.stderr)
            return exc.returncode
        print("\nTagging complete.")
        if args.push:
            print("Tags pushed to origin.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
