from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_tag_version.py"


@pytest.mark.parametrize(
    "tag,prefix,version",
    [
        ("dc43-v1.2.3", "dc43-v", "1.2.3"),
        ("pkg-2024.01", "pkg-", "2024.01"),
    ],
)
def test_check_tag_version_success(tmp_path: Path, tag: str, prefix: str, version: str) -> None:
    version_file = tmp_path / "VERSION"
    version_file.write_text(f"{version}\n", encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--tag",
            tag,
            "--prefix",
            prefix,
            "--version-file",
            str(version_file),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


def test_check_tag_version_mismatch(tmp_path: Path) -> None:
    version_file = tmp_path / "VERSION"
    version_file.write_text("1.0.0\n", encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--tag",
            "dc43-v1.0.1",
            "--prefix",
            "dc43-v",
            "--version-file",
            str(version_file),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "does not match expected" in result.stderr


def test_check_tag_version_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "MISSING"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--tag",
            "dc43-v1.0.0",
            "--prefix",
            "dc43-v",
            "--version-file",
            str(missing),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "MISSING" in result.stderr
