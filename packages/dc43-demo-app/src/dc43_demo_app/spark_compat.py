"""Utilities to keep pyspark usable when databricks-connect is installed."""

from __future__ import annotations

def ensure_local_spark_builder() -> None:
    """Patch Spark builder helpers so local sessions keep working."""

    try:
        from pyspark.sql import session as spark_session
    except Exception:  # pragma: no cover - pyspark missing
        return

    try:
        setattr(spark_session, "_is_remote_only", False)
        setattr(spark_session, "is_remote_only", lambda: False)
    except Exception:  # pragma: no cover - safety guard
        pass

    builder = spark_session.SparkSession.Builder
    if getattr(builder.getOrCreate, "__dc43_local_patch__", False):
        return

    try:
        from pyspark.conf import SparkConf
        from pyspark.context import SparkContext
    except Exception:  # pragma: no cover - pyspark missing components
        return

    def _local_get_or_create(self: "spark_session.SparkSession.Builder") -> "spark_session.SparkSession":
        with self._lock:
            session = spark_session.SparkSession._instantiatedSession
            if session is None or getattr(session._sc, "_jsc", None) is None:
                conf = SparkConf()
                for key, value in self._options.items():
                    conf.set(key, value)
                sc = SparkContext.getOrCreate(conf)
                session = spark_session.SparkSession(sc, options=self._options)
                spark_session.SparkSession._instantiatedSession = session
            else:
                getattr(
                    getattr(session._jvm, "SparkSession$"), "MODULE$"
                ).applyModifiableSettings(session._jsparkSession, self._options)
            return session

    _local_get_or_create.__dc43_local_patch__ = True  # type: ignore[attr-defined]
    builder.getOrCreate = _local_get_or_create  # type: ignore[assignment]


__all__ = ["ensure_local_spark_builder"]
