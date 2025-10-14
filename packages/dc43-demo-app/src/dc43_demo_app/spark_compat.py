"""Utilities to keep pyspark usable when databricks-connect is installed."""

from __future__ import annotations

import stat
from pathlib import Path

from py4j.java_gateway import JavaPackage
from py4j.protocol import Py4JError


def _ensure_spark_binaries_executable() -> None:
    """Restore execute permissions on pyspark launcher scripts when needed."""

    try:
        import pyspark  # type: ignore
    except Exception:  # pragma: no cover - pyspark missing
        return

    try:
        bin_dir = Path(pyspark.__file__).resolve().parent / "bin"
    except Exception:  # pragma: no cover - resolution failure
        return

    if not bin_dir.exists():  # pragma: no cover - unexpected layout
        return

    for name in ("spark-submit", "spark-class", "pyspark", "spark-shell"):
        script = bin_dir / name
        if not script.exists():
            continue
        try:
            mode = script.stat().st_mode
        except OSError:  # pragma: no cover - permission denied
            continue
        if mode & stat.S_IXUSR:
            continue
        try:
            script.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        except OSError:  # pragma: no cover - permission denied
            continue


def _patch_cached_arrow_call() -> None:
    """Ensure missing Arrow cache servers are treated as disabled."""

    original_call = getattr(JavaPackage, "__call__", None)
    if original_call is None:
        return
    if getattr(original_call, "__dc43_cached_arrow_patch__", False):
        return

    def _patched_call(self: JavaPackage, *args, **kwargs):
        if getattr(self, "_fqn", "") == "org.apache.spark.api.python.CachedArrowBatchServer.isEnabled":
            return False
        return original_call(self, *args, **kwargs)

    _patched_call.__dc43_cached_arrow_patch__ = True  # type: ignore[attr-defined]
    JavaPackage.__call__ = _patched_call  # type: ignore[assignment]


def ensure_local_spark_builder() -> None:
    """Patch Spark builder helpers so local sessions keep working."""

    _ensure_spark_binaries_executable()
    _patch_cached_arrow_call()

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

    try:
        from pyspark import instrumentation_utils  # type: ignore
    except Exception:  # pragma: no cover - module absent
        instrumentation_utils = None  # type: ignore[assignment]

    hooks = getattr(SparkContext, "_after_init_hooks", None)
    if isinstance(hooks, list):
        patched_hooks = []
        for hook in hooks:
            if (
                callable(hook)
                and getattr(hook, "__module__", "") == "pyspark.instrumentation_utils"
                and hook.__name__ == "instrument_hook"
                and not getattr(hook, "__dc43_safe__", False)
            ):

                def _safe_hook(original_hook=hook) -> None:
                    try:
                        original_hook()
                    except Py4JError:
                        return None

                _safe_hook.__dc43_safe__ = True  # type: ignore[attr-defined]
                patched_hooks.append(_safe_hook)
            else:
                patched_hooks.append(hook)
        SparkContext._after_init_hooks = patched_hooks

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
