from __future__ import annotations

"""Dataset helpers (version/id) for Spark/Delta."""

from typing import Optional

try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover
    SparkSession = object  # type: ignore


def get_delta_version(spark: SparkSession, *, table: Optional[str] = None, path: Optional[str] = None) -> Optional[str]:
    """Return the latest Delta table version as a string if available."""
    try:
        ref = table if table else f"delta.`{path}`"
        row = spark.sql(f"DESCRIBE HISTORY {ref} LIMIT 1").head(1)
        if not row:
            return None
        # versions column name can be 'version'
        v = row[0][0]
        return str(v)
    except Exception:
        return None


def dataset_id_from_ref(*, table: Optional[str] = None, path: Optional[str] = None) -> str:
    """Build a dataset id from a table name or path (``table:...``/``path:...``)."""
    if table:
        return f"table:{table}"
    if path:
        return f"path:{path}"
    return "unknown"
