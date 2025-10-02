#!/usr/bin/env python3
"""Validate that a Git tag matches the version recorded for a package."""

from __future__ import annotations

import argparse
from pathlib import Path


def _read_version(path: Path) -> str:
    text = path.read_text("utf-8").strip()
    if not text:
        raise ValueError(f"Version file {path} is empty")
    return text


def validate(tag: str, prefix: str, version_file: Path) -> str:
    """Return the expected tag for ``version_file`` and raise if it mismatches."""

    version = _read_version(version_file)
    expected = f"{prefix}{version}"
    if tag != expected:
        raise ValueError(f"Tag {tag} does not match expected {expected}")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Tag name to validate")
    parser.add_argument("--prefix", required=True, help="Expected tag prefix, e.g. 'dc43-v'")
    parser.add_argument(
        "--version-file",
        required=True,
        type=Path,
        help="Path to the file that records the package version",
    )
    args = parser.parse_args()

    try:
        validate(args.tag, args.prefix, args.version_file)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc))
    except ValueError as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
