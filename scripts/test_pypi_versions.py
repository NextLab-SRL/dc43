#!/usr/bin/env python3
"""Apply deterministic pre-release suffixes to package versions for Test PyPI.

The helper deliberately relies on :mod:`packaging.version` to emit identifiers
that follow the `PEP 440 <https://peps.python.org/pep-0440/>`_ ordering rules
understood by ``pip`` and the Python Package Index. This ensures that pre-release
artifacts remain sortable against their canonical release versions and that the
latest suffix (``rc`` or ``dev``) is always preferred by installers during
continuous-integration validation runs.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
from typing import Iterable, List, Sequence

from packaging.version import InvalidVersion, Version

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _packages import PACKAGES  # type: ignore


@dataclass
class DependencyRewrite:
    """Record describing a dependency constraint rewritten for Test PyPI."""

    path: Path
    before: str
    after: str


@dataclass
class TestVersion:
    """Container describing a rewritten package version."""

    package: str
    version_file: Path
    base_version: str
    test_version: str
    dependency_rewrites: List[DependencyRewrite] = field(default_factory=list)


VALID_STAGES = {"dev", "rc"}


def _timestamp() -> str:
    """Return a UTC timestamp suitable for constructing numeric suffixes."""

    return datetime.utcnow().strftime("%Y%m%d%H%M%S")


def determine_identifier(
    *,
    explicit: str | None = None,
    run_identifier: str | None = None,
) -> str:
    """Return a numeric identifier for the pre-release suffix."""

    candidates: Sequence[str | None] = (explicit, run_identifier)
    for candidate in candidates:
        if candidate is None:
            continue
        trimmed = candidate.strip()
        if not trimmed:
            continue
        if not trimmed.isdigit():
            raise ValueError("Test PyPI pre-release identifiers must be numeric")
        return trimmed
    return _timestamp()


def normalise_stage(stage: str) -> str:
    """Normalise the stage flag to one of the supported pre-release markers."""

    normalised = stage.strip().lower()
    if normalised not in VALID_STAGES:
        allowed = ", ".join(sorted(VALID_STAGES))
        raise ValueError(f"Unsupported pre-release stage '{stage}'. Expected one of: {allowed}")
    return normalised


def build_test_version(base_version: str, *, stage: str, identifier: str) -> str:
    """Construct a PEP 440 compliant pre-release version for Test PyPI."""

    if not identifier.isdigit():
        raise ValueError("Pre-release identifier must be numeric")
    try:
        parsed = Version(base_version)
    except InvalidVersion as exc:  # pragma: no cover - defensive programming
        raise ValueError(f"Invalid base version '{base_version}'") from exc
    release = parsed.base_version
    if stage == "dev":
        return f"{release}.dev{identifier}"
    return f"{release}{stage}{identifier}"


def update_version_file(version_file: Path, new_version: str) -> None:
    """Persist ``new_version`` to ``version_file`` with a trailing newline."""

    version_file.write_text(f"{new_version}\n", encoding="utf-8")


def _dependency_stage_floor(base_version: str, stage: str) -> str:
    """Return the lowest allowed version for internal dependencies during a stage."""

    parsed = Version(base_version)
    release = parsed.base_version
    if stage == "dev":
        return f"{release}.dev0"
    return f"{release}{stage}0"


_DEPENDENCY_PATTERN = "([\"']dc43[^\"']*>=){}"


def _rewrite_internal_dependencies(
    *, package: str, meta: dict, base_version: str, stage: str
) -> List[DependencyRewrite]:
    """Relax internal dependency floors to include the current pre-release stage."""

    if stage not in VALID_STAGES:
        return []

    pyproject: Path | None = meta.get("pyproject")
    if not pyproject:
        return []

    if not pyproject.exists():  # pragma: no cover - defensive guard
        raise FileNotFoundError(f"pyproject for package '{package}' not found at {pyproject}")

    release = Version(base_version).base_version
    dependency_floor = _dependency_stage_floor(base_version, stage)
    pattern = re.compile(_DEPENDENCY_PATTERN.format(re.escape(release)))
    content = pyproject.read_text(encoding="utf-8")
    matches = list(pattern.finditer(content))
    if not matches:
        return []

    updated = pattern.sub(lambda match: f"{match.group(1)}{dependency_floor}", content)
    pyproject.write_text(updated, encoding="utf-8")

    rewrites: List[DependencyRewrite] = []
    for match in matches:
        before = match.group(0)
        after = f"{match.group(1)}{dependency_floor}"
        rewrites.append(DependencyRewrite(path=pyproject, before=before, after=after))
    return rewrites


def apply_test_version(package: str, *, stage: str, identifier: str) -> TestVersion:
    """Rewrite ``package``'s version file with a Test PyPI pre-release suffix."""

    meta = PACKAGES.get(package)
    if meta is None:
        raise ValueError(f"Unknown package '{package}'")
    version_file = meta.get("version_file")
    if version_file is None:
        raise ValueError(f"Package '{package}' is missing a version_file entry")
    base_version = version_file.read_text(encoding="utf-8").strip()
    test_version = build_test_version(base_version, stage=stage, identifier=identifier)
    update_version_file(version_file, test_version)
    dependency_rewrites = _rewrite_internal_dependencies(
        package=package, meta=meta, base_version=base_version, stage=stage
    )
    return TestVersion(
        package=package,
        version_file=version_file,
        base_version=base_version,
        test_version=test_version,
        dependency_rewrites=dependency_rewrites,
    )


def apply_for_packages(packages: Iterable[str], *, stage: str, identifier: str) -> List[TestVersion]:
    """Rewrite every package in ``packages`` and return their version details."""

    results: List[TestVersion] = []
    for package in packages:
        results.append(apply_test_version(package, stage=stage, identifier=identifier))
    return results


def _format_summary_rows(rows: Sequence[TestVersion]) -> List[str]:
    if not rows:
        return []
    output = ["### Test PyPI versions", "| Package | Base version | Test version |", "| --- | --- | --- |"]
    for row in rows:
        output.append(f"| {row.package} | {row.base_version} | {row.test_version} |")
    return output


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", nargs="+", help="Packages to rewrite", required=True)
    parser.add_argument(
        "--stage",
        default="rc",
        help="Pre-release marker to apply (supported: rc, dev). Defaults to rc.",
    )
    parser.add_argument(
        "--identifier",
        help="Explicit numeric identifier for the pre-release suffix (overrides run identifier).",
    )
    parser.add_argument(
        "--run-identifier",
        help="Fallback numeric identifier (e.g. GitHub run number) when --identifier is omitted.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write JSON metadata about the rewritten versions.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    stage = normalise_stage(args.stage)
    identifier = determine_identifier(explicit=args.identifier, run_identifier=args.run_identifier)
    results = apply_for_packages(args.packages, stage=stage, identifier=identifier)
    if args.output:
        payload = [
            {
                "package": row.package,
                "version_file": str(row.version_file),
                "base_version": row.base_version,
                "test_version": row.test_version,
                "dependency_rewrites": [
                    {
                        "path": str(rewrite.path),
                        "before": rewrite.before,
                        "after": rewrite.after,
                    }
                    for rewrite in row.dependency_rewrites
                ],
            }
            for row in results
        ]
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    summary_rows = _format_summary_rows(results)
    if summary_rows:
        print("\n".join(summary_rows))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
