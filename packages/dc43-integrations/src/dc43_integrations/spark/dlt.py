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

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.core.odcs import contract_identity
from dc43_service_backends.core.versioning import SemVer
if TYPE_CHECKING:  # pragma: no cover - typing-only imports
    from dc43_service_clients.contracts.client.interface import ContractServiceClient
    from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
else:  # pragma: no cover - runtime stubs avoid optional deps
    ContractServiceClient = Any  # type: ignore[assignment]
    DataQualityServiceClient = Any  # type: ignore[assignment]


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


F = TypeVar("F", bound=Callable[..., Any])


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


def _select_version(versions: Sequence[str], minimum: str) -> str:
    """Return the highest version satisfying ``>= minimum``."""

    base = SemVer.parse(minimum)
    best: tuple[int, int, int] | None = None
    best_value: str | None = None
    for candidate in versions:
        try:
            parsed = SemVer.parse(candidate)
        except ValueError:
            if candidate == minimum:
                return candidate
            continue
        key = (parsed.major, parsed.minor, parsed.patch)
        if key < (base.major, base.minor, base.patch):
            continue
        if best is None or key > best:
            best = key
            best_value = candidate
    if best_value is None:
        raise ValueError(f"No versions found satisfying >= {minimum}")
    return best_value


def _resolve_contract(
    *,
    contract_id: str,
    expected_version: str | None,
    service: ContractServiceClient,
) -> OpenDataContractStandard:
    """Fetch a contract respecting version selectors."""

    if expected_version is None:
        contract = service.latest(contract_id)
        if contract is None:
            raise ValueError(f"No versions available for contract {contract_id}")
        return contract

    if expected_version.startswith("=="):
        return service.get(contract_id, expected_version[2:])

    if expected_version.startswith(">="):
        base = expected_version[2:]
        version = _select_version(list(service.list_versions(contract_id)), base)
        return service.get(contract_id, version)

    return service.get(contract_id, expected_version)


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
    contract_id: str,
    expected_contract_version: str | None,
    contract_service: ContractServiceClient,
    data_quality_service: DataQualityServiceClient,
    expectation_predicates: Mapping[str, str] | None,
) -> DLTContractBinding:
    contract = _resolve_contract(
        contract_id=contract_id,
        expected_version=expected_contract_version,
        service=contract_service,
    )
    contract_id_value, contract_version = contract_identity(contract)
    plan = _freeze_expectation_plan(
        data_quality_service.describe_expectations(contract=contract)
    )
    expectations = DLTExpectations.from_expectation_plan(
        plan,
        fallback_predicates=expectation_predicates,
    )
    return DLTContractBinding(
        contract_id=contract_id_value,
        contract_version=contract_version,
        expectation_plan=plan,
        expectations=expectations,
    )


def contract_expectations(
    dlt_module: Any,
    *,
    contract_id: str,
    contract_service: ContractServiceClient,
    data_quality_service: DataQualityServiceClient,
    expected_contract_version: str | None = None,
    expectation_predicates: Mapping[str, str] | None = None,
) -> Callable[[F], F]:
    """Return a decorator binding a DLT asset to contract expectations."""

    binding = _prepare_contract_binding(
        contract_id=contract_id,
        expected_contract_version=expected_contract_version,
        contract_service=contract_service,
        data_quality_service=data_quality_service,
        expectation_predicates=expectation_predicates,
    )

    def decorator(fn: F) -> F:
        decorated: Any = fn
        for dlt_decorator in binding.expectations.decorators(dlt_module):
            decorated = dlt_decorator(decorated)
        _attach_binding(decorated, binding)
        return cast(F, decorated)

    return decorator


def contract_table(
    dlt_module: Any,
    *,
    contract_id: str,
    contract_service: ContractServiceClient,
    data_quality_service: DataQualityServiceClient,
    expected_contract_version: str | None = None,
    expectation_predicates: Mapping[str, str] | None = None,
    **table_kwargs: Any,
) -> Callable[[F], F]:
    """Return a decorator producing a contract-aware ``@dlt.table`` asset."""

    binding = _prepare_contract_binding(
        contract_id=contract_id,
        expected_contract_version=expected_contract_version,
        contract_service=contract_service,
        data_quality_service=data_quality_service,
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


def contract_view(
    dlt_module: Any,
    *,
    contract_id: str,
    contract_service: ContractServiceClient,
    data_quality_service: DataQualityServiceClient,
    expected_contract_version: str | None = None,
    expectation_predicates: Mapping[str, str] | None = None,
    **view_kwargs: Any,
) -> Callable[[F], F]:
    """Return a decorator producing a contract-aware ``@dlt.view`` asset."""

    binding = _prepare_contract_binding(
        contract_id=contract_id,
        expected_contract_version=expected_contract_version,
        contract_service=contract_service,
        data_quality_service=data_quality_service,
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
    "contract_expectations",
    "contract_table",
    "contract_view",
]
