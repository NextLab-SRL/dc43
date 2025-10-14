"""Local helpers for exercising contract-decorated DLT assets with databricks-dlt."""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from types import ModuleType
from typing import Any, Callable, Dict, Mapping, TypeVar, TYPE_CHECKING

try:  # pragma: no cover - optional dependency guard
    import dlt as databricks_dlt
except Exception as exc:  # pragma: no cover - databricks-dlt not installed
    databricks_dlt = None  # type: ignore[assignment]
    _DLT_IMPORT_ERROR = exc
else:  # pragma: no cover - only executed when the import succeeds
    _DLT_IMPORT_ERROR = None


def _build_stub_dlt_module() -> ModuleType:
    """Return a minimal ``dlt`` facsimile for environments without the package."""

    module = ModuleType("dc43_integrations.stub_dlt")
    module.__dc43_is_stub__ = True  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Runtime toggles
    # ------------------------------------------------------------------
    def enable_local_execution() -> None:  # pragma: no cover - no-op
        return None

    module.enable_local_execution = enable_local_execution  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Expectation decorators
    # ------------------------------------------------------------------
    def _noop_decorator(fn: F) -> F:  # pragma: no cover - trivial
        return fn

    def expect_all(predicates: Mapping[str, str]) -> Callable[[F], F]:
        return _noop_decorator

    def expect_all_or_drop(predicates: Mapping[str, str]) -> Callable[[F], F]:
        return _noop_decorator

    module.expect_all = expect_all  # type: ignore[attr-defined]
    module.expect_all_or_drop = expect_all_or_drop  # type: ignore[attr-defined]

    # ------------------------------------------------------------------
    # Asset decorators
    # ------------------------------------------------------------------
    def table(query_function: F | None = None, **kwargs: Any) -> Callable[[F], F]:
        if query_function is not None and callable(query_function):
            return query_function

        def decorator(fn: F) -> F:
            return fn

        return decorator

    def view(query_function: F | None = None, **kwargs: Any) -> Callable[[F], F]:
        if query_function is not None and callable(query_function):
            return query_function

        def decorator(fn: F) -> F:
            return fn

        return decorator

    module.table = table  # type: ignore[attr-defined]
    module.view = view  # type: ignore[attr-defined]

    return module


_STUB_DLT_MODULE: Any | None = None


def ensure_dlt_module(*, allow_stub: bool = False) -> Any:
    """Return the ``dlt`` module or a stub replacement when requested."""

    if databricks_dlt is not None:
        return databricks_dlt
    if not allow_stub:
        raise RuntimeError(
            "databricks-dlt package is required for DLT helpers"
        ) from _DLT_IMPORT_ERROR

    global _STUB_DLT_MODULE
    if _STUB_DLT_MODULE is None:
        _STUB_DLT_MODULE = _build_stub_dlt_module()
    return _STUB_DLT_MODULE

try:  # pragma: no cover - optional dependency guard
    from pyspark.errors import AnalysisException
    from pyspark.sql.functions import expr
except Exception as exc:  # pragma: no cover - pyspark not installed
    AnalysisException = RuntimeError  # type: ignore[assignment]
    expr = None  # type: ignore[assignment]
    _PYSPARK_IMPORT_ERROR = exc
else:  # pragma: no cover - only executed when pyspark is present
    _PYSPARK_IMPORT_ERROR = None

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession
else:  # pragma: no cover - runtime placeholder
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]


F = TypeVar("F", bound=Callable[..., "DataFrame"])


@dataclass(frozen=True)
class ExpectationReport:
    """Single expectation evaluation emitted during local DLT execution."""

    asset: str
    rule: str
    predicate: str
    action: str
    failed_rows: int
    run_id: int

    @property
    def status(self) -> str:
        """Return ``"passed"`` when ``failed_rows`` equals zero."""

        return "passed" if self.failed_rows == 0 else "failed"


class LocalDLTHarness:
    """Patch ``databricks-dlt`` so contract-decorated assets can run locally.

    The upstream `databricks-dlt` package only toggles a ``LOCAL_EXECUTION_MODE``
    flag; the expectation and table decorators still return a stub ``__outer``
    function that yields ``None``.  Tests therefore need a thin shim that
    registers the decorated callables, executes them against a Spark session and
    records the expectation verdicts.  ``LocalDLTHarness`` fills that gap while
    reusing the official decorators so notebooks remain source-compatible with
    Databricks deployments.
    """

    def __init__(self, spark: "SparkSession", *, module: Any | None = None) -> None:
        if expr is None:  # pragma: no cover - exercised when pyspark is missing
            raise RuntimeError(
                "LocalDLTHarness requires pyspark; install pyspark to run local DLT tests"
            ) from _PYSPARK_IMPORT_ERROR
        if module is None:
            module = ensure_dlt_module(allow_stub=True)
        self.spark = spark
        self.module = module
        self._using_stub = bool(getattr(module, "__dc43_is_stub__", False))
        self.expectation_reports: list[ExpectationReport] = []
        self.table_options: Dict[str, Mapping[str, Any]] = {}
        self.view_options: Dict[str, Mapping[str, Any]] = {}
        self._tables: Dict[str, Callable[[], "DataFrame"]] = {}
        self._views: Dict[str, Callable[[], "DataFrame"]] = {}
        self._current_asset: str | None = None
        self._run_sequence = 0
        self._originals: Dict[str, Any] = {}
        self._active = False

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------
    def activate(self) -> None:
        """Patch the underlying ``dlt`` module so decorators use the harness."""

        if self._active:
            return
        enable = getattr(self.module, "enable_local_execution", None)
        if callable(enable):  # pragma: no branch - trivial guard
            enable()
        self._originals = {
            name: getattr(self.module, name)
            for name in ("expect_all", "expect_all_or_drop", "table", "view")
        }
        setattr(self.module, "expect_all", self._expect_all)
        setattr(self.module, "expect_all_or_drop", self._expect_all_or_drop)
        setattr(self.module, "table", self._table)
        setattr(self.module, "view", self._view)
        self._active = True

    def deactivate(self) -> None:
        """Restore the original ``dlt`` decorators."""

        if not self._active:
            return
        for name, original in self._originals.items():
            setattr(self.module, name, original)
        self._originals.clear()
        self._active = False

    def __enter__(self) -> "LocalDLTHarness":  # pragma: no cover - simple guard
        self.activate()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - simple guard
        self.deactivate()

    # ------------------------------------------------------------------
    # Expectation decorators
    # ------------------------------------------------------------------
    def _expect_all(self, predicates: Mapping[str, str]) -> Callable[[F], F]:
        return self._build_expectation_decorator(predicates, action="warn", drop=False)

    def _expect_all_or_drop(self, predicates: Mapping[str, str]) -> Callable[[F], F]:
        return self._build_expectation_decorator(predicates, action="drop", drop=True)

    def _build_expectation_decorator(
        self,
        predicates: Mapping[str, str],
        *,
        action: str,
        drop: bool,
    ) -> Callable[[F], F]:
        predicate_map = dict(predicates)

        def decorator(fn: F) -> F:
            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> "DataFrame":
                df = fn(*args, **kwargs)
                if df is None:
                    raise RuntimeError("DLT asset returned None; expected a DataFrame")
                asset_name = self._resolve_asset_name(wrapper, fn)
                for key, predicate in predicate_map.items():
                    try:
                        invalid = df.filter(~expr(predicate))
                        failures = invalid.count()
                    except AnalysisException:
                        failures = df.count()
                    self._record(asset_name, key, predicate, action, failures)
                return df

            return wrapper  # type: ignore[return-value]

        return decorator

    # ------------------------------------------------------------------
    # Asset registration
    # ------------------------------------------------------------------
    def _table(self, query_function: F | None = None, **kwargs: Any) -> Callable[[F], F]:
        if query_function is not None and callable(query_function):
            return self._register_asset(query_function, kwargs, self._tables, self.table_options)

        def decorator(fn: F) -> F:
            return self._register_asset(fn, kwargs, self._tables, self.table_options)

        return decorator

    def _view(self, query_function: F | None = None, **kwargs: Any) -> Callable[[F], F]:
        if query_function is not None and callable(query_function):
            return self._register_asset(query_function, kwargs, self._views, self.view_options)

        def decorator(fn: F) -> F:
            return self._register_asset(fn, kwargs, self._views, self.view_options)

        return decorator

    def _register_asset(
        self,
        fn: F,
        options: Mapping[str, Any],
        registry: Dict[str, Callable[[], "DataFrame"]],
        option_store: Dict[str, Mapping[str, Any]],
    ) -> F:
        name = str(options.get("name") or getattr(fn, "__name__", "asset"))
        setattr(fn, "__dlt_asset_name__", name)
        registry[name] = fn
        option_store[name] = dict(options)
        return fn

    # ------------------------------------------------------------------
    # Execution helpers
    # ------------------------------------------------------------------
    def run_asset(self, name: str) -> "DataFrame":
        """Execute a registered table or view and return the resulting DataFrame."""

        if name in self._tables:
            fn = self._tables[name]
        elif name in self._views:
            fn = self._views[name]
        else:
            raise KeyError(f"unknown DLT asset: {name}")

        previous_asset = self._current_asset
        self._current_asset = name
        self._run_sequence += 1
        try:
            result = fn()
        finally:
            self._current_asset = previous_asset
        if result is None:
            raise RuntimeError(f"DLT asset {name} returned None; expected a DataFrame")
        return result

    def run_all(self) -> Dict[str, "DataFrame"]:
        """Execute all registered tables and return a mapping of results."""

        return {name: self.run_asset(name) for name in self._tables}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_asset_name(self, wrapper: Callable[..., Any], original: Callable[..., Any]) -> str:
        name = getattr(wrapper, "__dlt_asset_name__", None)
        if not name:
            name = getattr(original, "__dlt_asset_name__", None)
        return str(name or getattr(original, "__name__", "asset"))

    def _record(self, asset: str, rule: str, predicate: str, action: str, failures: int) -> None:
        report = ExpectationReport(
            asset=asset,
            rule=rule,
            predicate=predicate,
            action=action,
            failed_rows=failures,
            run_id=self._run_sequence,
        )
        self.expectation_reports.append(report)


__all__ = ["ExpectationReport", "LocalDLTHarness", "ensure_dlt_module"]
