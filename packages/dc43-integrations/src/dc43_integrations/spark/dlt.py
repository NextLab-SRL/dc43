"""Delta Live Tables helpers built around contract-derived expectations."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)

from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance import GovernancePublicationMode, resolve_publication_mode
from dc43_service_clients.governance.models import GovernanceReadContext
if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from typing_extensions import TypeAlias


@dataclass(frozen=True, slots=True)
class DLTExpectations:
    """Container for DLT decorators derived from contract expectations.

    The class keeps two groups of predicates:

    * ``enforced`` – mapped to ``dlt.expect_all_or_drop`` to reproduce the
      contract's required expectations.
    * ``observed`` – mapped to ``dlt.expect_all`` for optional expectations
      whose violations should only emit warnings.

    Use :meth:`apply` to register expectations imperatively inside a pipeline
    function, or :meth:`decorators` to retrieve callables that can decorate a
    ``@dlt.table``/``@dlt.view`` definition.
    """

    enforced: Mapping[str, str]
    observed: Mapping[str, str]

    def __post_init__(self) -> None:  # pragma: no cover - exercised via factory methods
        enforced = MappingProxyType(dict(self.enforced))
        observed = MappingProxyType(dict(self.observed))
        object.__setattr__(self, "enforced", enforced)
        object.__setattr__(self, "observed", observed)

    def __bool__(self) -> bool:  # pragma: no cover - trivial
        return bool(self.enforced or self.observed)

    def apply(self, dlt_module: Any) -> None:
        """Register expectations through the provided ``dlt`` module."""

        if self.enforced:
            dlt_module.expect_all_or_drop(dict(self.enforced))
        if self.observed:
            dlt_module.expect_all(dict(self.observed))

    def decorators(self, dlt_module: Any) -> tuple[Any, ...]:
        """Return DLT decorators matching the stored expectations."""

        decorators: list[Any] = []
        if self.enforced:
            decorators.append(dlt_module.expect_all_or_drop(dict(self.enforced)))
        if self.observed:
            decorators.append(dlt_module.expect_all(dict(self.observed)))
        return tuple(decorators)

    @classmethod
    def from_predicates(cls, predicates: Mapping[str, str], *, drop: bool = False) -> "DLTExpectations":
        """Build an expectation set from raw predicate mappings."""

        mapping = dict(predicates)
        if drop:
            return cls(enforced=mapping, observed={})
        return cls(enforced={}, observed=mapping)

    @classmethod
    def from_expectation_plan(
        cls,
        plan: Iterable[Mapping[str, Any]],
        *,
        fallback_predicates: Mapping[str, str] | None = None,
    ) -> "DLTExpectations":
        """Create an expectation set from a contract expectation plan.

        Parameters
        ----------
        plan:
            Iterable of expectation descriptors as produced by the data-quality
            service. Each descriptor may contain ``key``, ``predicate`` and the
            ``optional`` flag.
        fallback_predicates:
            Optional mapping used when a plan entry lacks the ``predicate``
            attribute. The helper looks up the predicate by key.
        """

        enforced: MutableMapping[str, str] = {}
        observed: MutableMapping[str, str] = {}
        fallback = dict(fallback_predicates or {})
        for descriptor in plan:
            key = descriptor.get("key")
            predicate = descriptor.get("predicate")
            if not isinstance(key, str):
                continue
            if not isinstance(predicate, str):
                predicate = fallback.get(key)
                if not isinstance(predicate, str):
                    continue
            target = observed if bool(descriptor.get("optional")) else enforced
            target[key] = predicate
        return cls(enforced=enforced, observed=observed)


@dataclass(frozen=True, slots=True)
class DLTContractBinding:
    """Metadata recorded on DLT assets bound to a contract."""

    contract_id: str
    contract_version: str
    expectation_plan: Tuple[Mapping[str, Any], ...]
    expectations: DLTExpectations
    publication_mode: GovernancePublicationMode


F = TypeVar("F", bound=Callable[..., Any])


if TYPE_CHECKING:  # pragma: no cover - typing-only alias
    GovernanceReadContextLike: "TypeAlias" = GovernanceReadContext | Mapping[str, object]


def _ensure_read_context(request: "GovernanceReadContextLike") -> GovernanceReadContext:
    """Normalise mappings into :class:`GovernanceReadContext` instances."""

    if isinstance(request, GovernanceReadContext):
        return request
    if isinstance(request, Mapping):
        return GovernanceReadContext(**dict(request))
    raise TypeError("context must be a GovernanceReadContext or mapping")


def _attach_binding(target: Any, binding: DLTContractBinding) -> None:
    """Expose contract metadata on the decorated callable."""

    setattr(target, "__dc43_contract__", (binding.contract_id, binding.contract_version))
    setattr(target, "__dc43_contract_binding__", binding)


def apply_dlt_expectations(
    dlt_module: Any,
    expectations: Mapping[str, str] | DLTExpectations,
    *,
    drop: bool = False,
) -> None:
    """Apply expectations using a provided ``dlt`` module inside a pipeline function."""

    expectation_set = (
        expectations
        if isinstance(expectations, DLTExpectations)
        else DLTExpectations.from_predicates(expectations, drop=drop)
    )
    expectation_set.apply(dlt_module)


def expectation_decorators(
    dlt_module: Any,
    expectations: Mapping[str, str] | DLTExpectations,
    *,
    drop: bool = False,
) -> tuple[Any, ...]:
    """Return decorators that can be stacked on top of DLT pipeline definitions."""

    expectation_set = (
        expectations
        if isinstance(expectations, DLTExpectations)
        else DLTExpectations.from_predicates(expectations, drop=drop)
    )
    return expectation_set.decorators(dlt_module)


def expectations_from_validation_details(details: Mapping[str, Any]) -> DLTExpectations:
    """Extract DLT expectations from a ``ValidationResult.details`` mapping."""

    plan = details.get("expectation_plan")
    predicates = details.get("expectation_predicates")
    predicate_map: Mapping[str, str] = {}
    if isinstance(predicates, Mapping):
        predicate_map = predicates
    if isinstance(plan, Sequence):
        return DLTExpectations.from_expectation_plan(plan, fallback_predicates=predicate_map)
    if predicate_map:
        return DLTExpectations.from_predicates(predicate_map)
    return DLTExpectations(enforced={}, observed={})


def _freeze_expectation_plan(
    plan: Iterable[Mapping[str, Any]],
) -> Tuple[Mapping[str, Any], ...]:
    frozen: list[Mapping[str, Any]] = []
    for descriptor in plan:
        if isinstance(descriptor, Mapping):
            frozen.append(MappingProxyType(dict(descriptor)))
    return tuple(frozen)


def _prepare_contract_binding(
    *,
    governance_service: GovernanceServiceClient,
    context: GovernanceReadContext,
    expectation_predicates: Mapping[str, str] | None,
    publication_mode: GovernancePublicationMode | str | None = None,
) -> DLTContractBinding:
    plan = governance_service.resolve_read_context(context=context)
    expectation_plan = _freeze_expectation_plan(
        governance_service.describe_expectations(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
        )
    )
    expectations = DLTExpectations.from_expectation_plan(
        expectation_plan,
        fallback_predicates=expectation_predicates,
    )
    resolved_mode = resolve_publication_mode(explicit=publication_mode)
    return DLTContractBinding(
        contract_id=plan.contract_id,
        contract_version=plan.contract_version,
        expectation_plan=expectation_plan,
        expectations=expectations,
        publication_mode=resolved_mode,
    )


def governed_expectations(
    dlt_module: Any,
    *,
    context: "GovernanceReadContextLike",
    governance_service: GovernanceServiceClient,
    expectation_predicates: Mapping[str, str] | None = None,
) -> Callable[[F], F]:
    """Return a decorator binding a DLT asset to contract expectations.

    The decorator mirrors :func:`~dc43_integrations.spark.io.read_with_governance`
    by sourcing every contract lookup through ``governance_service``; no
    secondary contract or data-quality clients are required.
    """

    binding = _prepare_contract_binding(
        governance_service=governance_service,
        context=_ensure_read_context(context),
        expectation_predicates=expectation_predicates,
    )

    def decorator(fn: F) -> F:
        decorated: Any = fn
        for dlt_decorator in binding.expectations.decorators(dlt_module):
            decorated = dlt_decorator(decorated)
        _attach_binding(decorated, binding)
        return cast(F, decorated)

    return decorator


def governed_table(
    dlt_module: Any,
    *,
    context: "GovernanceReadContextLike",
    governance_service: GovernanceServiceClient,
    expectation_predicates: Mapping[str, str] | None = None,
    **table_kwargs: Any,
) -> Callable[[F], F]:
    """Return a decorator producing a governance-aware ``@dlt.table`` asset.

    Contract resolution, expectation plans, and registration flow solely through
    the supplied governance service, keeping annotations aligned with the
    ``read_with_governance``/``write_with_governance`` helpers.
    """

    binding = _prepare_contract_binding(
        governance_service=governance_service,
        context=_ensure_read_context(context),
        expectation_predicates=expectation_predicates,
    )
    table_decorator = dlt_module.table(**table_kwargs)

    def decorator(fn: F) -> F:
        decorated: Any = fn
        for dlt_decorator in binding.expectations.decorators(dlt_module):
            decorated = dlt_decorator(decorated)
        decorated = table_decorator(decorated)
        _attach_binding(decorated, binding)
        return cast(F, decorated)

    return decorator


def governed_view(
    dlt_module: Any,
    *,
    context: "GovernanceReadContextLike",
    governance_service: GovernanceServiceClient,
    expectation_predicates: Mapping[str, str] | None = None,
    **view_kwargs: Any,
) -> Callable[[F], F]:
    """Return a decorator producing a governance-aware ``@dlt.view`` asset.

    Just like the read/write wrappers, the annotation only depends on the
    governance client; it resolves contracts and fetches expectation plans
    without explicit contract or data-quality services.
    """

    binding = _prepare_contract_binding(
        governance_service=governance_service,
        context=_ensure_read_context(context),
        expectation_predicates=expectation_predicates,
    )
    view_decorator = dlt_module.view(**view_kwargs)

    def decorator(fn: F) -> F:
        decorated: Any = fn
        for dlt_decorator in binding.expectations.decorators(dlt_module):
            decorated = dlt_decorator(decorated)
        decorated = view_decorator(decorated)
        _attach_binding(decorated, binding)
        return cast(F, decorated)

    return decorator


__all__ = [
    "DLTExpectations",
    "DLTContractBinding",
    "apply_dlt_expectations",
    "expectation_decorators",
    "expectations_from_validation_details",
    "governed_expectations",
    "governed_table",
    "governed_view",
]
