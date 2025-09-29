"""Shared fixtures and helpers for the dc43-integrations test suite."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _ensure_local_src_on_path() -> None:
    """Insert first-party ``src`` directories into ``sys.path`` for imports."""

    here = Path(__file__).resolve()
    project_root = here.parents[3]

    candidate_dirs = [here.parents[1] / "src", project_root / "src"]

    packages_root = project_root / "packages"
    if packages_root.exists():
        candidate_dirs.extend(
            src_dir for src_dir in packages_root.glob("*/src") if src_dir.is_dir()
        )

    for src_dir in candidate_dirs:
        src_str = str(src_dir)
        if src_dir.exists() and src_str not in sys.path:
            sys.path.insert(0, src_str)


_ensure_local_src_on_path()

if importlib.util.find_spec("pyspark") is None:
    pytest.skip("pyspark required for dc43 tests", allow_module_level=True)

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("dc43-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(
            "Spark not available (install Java/JDK and set JAVA_HOME). "
            f"Details: {e}"
        )
    else:
        yield spark
        spark.stop()
