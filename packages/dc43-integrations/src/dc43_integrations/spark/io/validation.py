from __future__ import annotations

import copy
import logging
import warnings
from dataclasses import is_dataclass, replace
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

from dc43_service_backends.core.versioning import SemVer, version_key
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore

if TYPE_CHECKING:
    from dc43_service_clients.contracts.client.interface import ContractServiceClient
    from dc43_service_clients.data_products import (
        DataProductServiceClient,
        OpenDataProductStandard,
        DataProductInputBinding,
    )
    from dc43_service_clients.governance.client.interface import GovernanceServiceClient
    from dc43_integrations.spark.io.strategies import SupportsDataProductStatusPolicy, SupportsContractStatusValidation, SupportsDataProductStatusValidation

logger = logging.getLogger(__name__)


def _paths_compatible(provided: str, contract_path: str) -> bool:
    """Return ``True`` when ``provided`` is consistent with ``contract_path``."""

    try:
        actual = Path(provided).resolve()
        expected = Path(contract_path).resolve()
    except OSError:
        return False

    if actual == expected:
        return True

    base = expected.parent / expected.stem if expected.suffix else expected
    if actual == base:
        return True

    return base in actual.parents


def _select_version(versions: list[str], minimum: str) -> str:
    """Return the highest version satisfying ``>= minimum``."""

    try:
        base = SemVer.parse(minimum)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid minimum version: {minimum}") from exc

    best: tuple[int, int, int] | None = None
    best_value: Optional[str] = None
    for candidate in versions:
        try:
            parsed = SemVer.parse(candidate)
        except ValueError:
            # Fallback to string comparison when candidate matches exactly.
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
    expected_version: Optional[str],
    service: ContractServiceClient | None,
    governance: GovernanceServiceClient | None,
) -> OpenDataContractStandard:
    """Fetch a contract from the configured service respecting version hints."""

    if service is None and governance is None:
        raise ValueError(
            "A contract service or governance service is required when contract_id is provided",
        )

    def _resolve_version(candidate: Optional[str]) -> str:
        if not candidate:
            if governance is not None:
                contract = governance.latest_contract(contract_id=contract_id)
            else:
                contract = service.latest(contract_id)  # type: ignore[union-attr]
            if contract is None:
                raise ValueError(f"No versions available for contract {contract_id}")
            return contract.version

        if candidate.startswith("=="):
            return candidate[2:]

        if candidate.startswith(">="):
            base = candidate[2:]
            versions: Sequence[str]
            if governance is not None:
                versions = tuple(governance.list_contract_versions(contract_id=contract_id))
            else:
                versions = tuple(service.list_versions(contract_id))  # type: ignore[union-attr]
            selected = _select_version(list(versions), base)
            return selected

        return candidate

    version = _resolve_version(expected_version)

    if governance is not None:
        return governance.get_contract(contract_id=contract_id, contract_version=version)

    return service.get(contract_id, version)  # type: ignore[union-attr]


def _enforce_contract_status(
    *,
    handler: object,
    contract: OpenDataContractStandard,
    enforce: bool,
    operation: str,
) -> None:
    """Apply a contract status policy defined by ``handler``."""

    # Using duck typing or Protocol check if imported
    if hasattr(handler, "validate_contract_status") and callable(handler.validate_contract_status): # type: ignore
         handler.validate_contract_status( # type: ignore
            contract=contract,
            enforce=enforce,
            operation=operation,
        )
         return

    _validate_contract_status(
        contract=contract,
        enforce=enforce,
        operation=operation,
    )


def _validate_contract_status(
    *,
    contract: OpenDataContractStandard,
    enforce: bool,
    operation: str,
    allowed_statuses: Iterable[str] | None = None,
    allow_missing: bool = True,
    case_insensitive: bool = True,
    failure_message: str | None = None,
) -> None:
    """Check the contract status against an allowed set."""

    raw_status = contract.status
    if raw_status is None:
        if allow_missing:
            return
        status_value = ""
    else:
        status_value = str(raw_status).strip()
        if not status_value and allow_missing:
            return

    if not status_value:
        message = (
            failure_message
            or "Contract {contract_id}:{contract_version} status {status!r} "
            "is not allowed for {operation} operations"
        ).format(
            contract_id=str(contract.id or ""),
            contract_version=str(contract.version or ""),
            status=status_value,
            operation=operation,
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return

    options = allowed_statuses or ("active",)
    allowed = {status.lower() if case_insensitive else status for status in options}
    candidate = status_value.lower() if case_insensitive else status_value
    if candidate in allowed:
        return

    message = (
        failure_message
        or "Contract {contract_id}:{contract_version} status {status!r} "
        "is not allowed for {operation} operations"
    ).format(
        contract_id=str(contract.id or ""),
        contract_version=str(contract.version or ""),
        status=status_value,
        operation=operation,
    )
    if enforce:
        raise ValueError(message)
    logger.warning(message)


def _normalise_version_spec(spec: Optional[str]) -> Optional[str]:
    """Return a normalised version constraint or ``None`` when unset."""

    if spec is None:
        return None
    value = str(spec).strip()
    if not value:
        return None
    if value.startswith("=="):
        return value[2:].strip() or None
    return value


def _check_contract_version(
    expected: Optional[str],
    actual: Optional[str],
) -> None:
    """Raise ValueError if actual does not satisfy the expected version constraint."""
    if expected is None or not expected.strip():
        return
    requirement = expected.strip()

    if not actual:
        raise ValueError(f"Actual contract version is missing; expected {expected}")

    if requirement.startswith("=="):
        target = requirement[2:].strip()
        if actual != target:
            raise ValueError(f"Contract version {actual} does not satisfy {expected}")
        return

    if requirement.startswith(">="):
        target = requirement[2:].strip()
        if not target:
            return
        
        try:
            if version_key(actual) < version_key(target):
                raise ValueError(f"Contract version {actual} does not satisfy {expected}")
        except Exception as exc:
            raise ValueError(f"Unable to compare versions {actual!r} and {target!r}: {exc}") from exc
        return

    if actual != requirement:
        raise ValueError(f"Contract version {actual} does not satisfy {expected}")


def _check_data_product_version(
    *,
    expected: Optional[str],
    actual: Optional[str],
    data_product_id: str,
    subject: str,
    enforce: bool,
) -> bool:
    """Return ``True`` when ``actual`` satisfies the optional ``expected`` constraint."""

    if expected is None or not expected.strip():
        return True
    if not actual:
        message = (
            f"{subject} version for data product {data_product_id} is unknown; expected {expected}"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return False

    requirement = expected.strip()
    if requirement.startswith("=="):
        target = requirement[2:].strip()
        if actual != target:
            message = (
                f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
            )
            if enforce:
                raise ValueError(message)
            logger.warning(message)
            return False
        return True
    if requirement.startswith(">="):
        target = requirement[2:].strip()
        if not target:
            return True
        try:
            if version_key(actual) < version_key(target):
                message = (
                    f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
                )
                if enforce:
                    raise ValueError(message)
                logger.warning(message)
                return False
        except Exception as exc:  # pragma: no cover - defensive against malformed versions
            message = (
                f"Unable to compare versions {actual!r} and {target!r} for data product {data_product_id}: {exc}"
            )
            if enforce:
                raise ValueError(message) from exc
            logger.warning(message)
            return False
        return True
    if actual != requirement:
        message = (
            f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return False
    return True


def _validate_data_product_status(
    *,
    data_product: OpenDataProductStandard,
    enforce: bool,
    operation: str,
    allowed_statuses: Iterable[str] | None = None,
    allow_missing: bool = True,
    case_insensitive: bool = True,
    failure_message: str | None = None,
) -> None:
    """Check the data product status against an allowed set."""

    raw_status = data_product.status
    product_id = str(data_product.id or "")
    product_version = str(data_product.version or "")
    if raw_status is None:
        if allow_missing:
            return
        status_value = ""
    else:
        status_value = str(raw_status).strip()
        if not status_value and allow_missing:
            return

    if not status_value:
        message = (
            failure_message
            or "Data product {data_product_id}@{data_product_version} status {status!r} "
            "is not allowed for {operation} operations"
        ).format(
            data_product_id=product_id,
            data_product_version=product_version,
            status=status_value,
            operation=operation,
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return

    options = allowed_statuses or ("active",)
    allowed = {status.lower() if case_insensitive else status for status in options}
    candidate = status_value.lower() if case_insensitive else status_value
    if candidate in allowed:
        return

    message = (
        failure_message
        or "Data product {data_product_id}@{data_product_version} status {status!r} "
        "is not allowed for {operation} operations"
    ).format(
        data_product_id=product_id,
        data_product_version=product_version,
        status=status_value,
        operation=operation,
    )
    if enforce:
        raise ValueError(message)
    logger.warning(message)


def _clone_status_handler(handler: object, overrides: Mapping[str, Any]) -> object:
    """Return ``handler`` updated with ``overrides`` without mutating the input."""

    if not overrides:
        return handler
    if is_dataclass(handler):
        try:
            return replace(handler, **overrides) # type: ignore
        except TypeError:
            pass
    try:
        clone = copy.copy(handler)
    except Exception:  # pragma: no cover - fallback when cloning fails
        clone = handler
    for key, value in overrides.items():
        try:
            object.__getattribute__(clone, key)
        except AttributeError:
            continue
        setattr(clone, key, value)
    return clone


def _apply_plan_data_product_policy(
    handler: object,
    plan: Any | None,
    *,
    default_enforce: bool,
) -> tuple[object, bool]:
    """Return a handler/enforcement pair honouring plan-defined policies."""

    if plan is None:
        return handler, default_enforce

    overrides: Dict[str, Any] = {}
    policy = getattr(plan, "policy", None)
    source = policy if policy is not None else plan

    try:
        allowed_statuses = source.allowed_data_product_statuses  # type: ignore[attr-defined]
    except AttributeError:
        allowed_statuses = None
    if (
        allowed_statuses is not None
        and hasattr(handler, "allowed_data_product_statuses") # Duck type
    ):
        overrides["allowed_data_product_statuses"] = tuple(allowed_statuses)

    try:
        allow_missing = source.allow_missing_data_product_status  # type: ignore[attr-defined]
    except AttributeError:
        allow_missing = None
    if (
        allow_missing is not None
        and hasattr(handler, "allow_missing_data_product_status") # Duck type
    ):
        overrides["allow_missing_data_product_status"] = bool(allow_missing)

    try:
        case_insensitive = source.data_product_status_case_insensitive  # type: ignore[attr-defined]
    except AttributeError:
        case_insensitive = None
    if (
        case_insensitive is not None
        and hasattr(handler, "data_product_status_case_insensitive") # Duck type
    ):
        overrides["data_product_status_case_insensitive"] = bool(case_insensitive)

    try:
        failure_message = source.data_product_status_failure_message  # type: ignore[attr-defined]
    except AttributeError:
        failure_message = None
    if (
        failure_message is not None
        and hasattr(handler, "data_product_status_failure_message") # Duck type
    ):
        overrides["data_product_status_failure_message"] = failure_message

    handler = _clone_status_handler(handler, overrides)

    try:
        enforce_override = source.enforce_data_product_status  # type: ignore[attr-defined]
    except AttributeError:
        enforce_override = None
    if enforce_override is None:
        return handler, default_enforce
    return handler, bool(enforce_override)


def _enforce_data_product_status(
    *,
    handler: object,
    data_product: OpenDataProductStandard,
    enforce: bool,
    operation: str,
) -> None:
    """Apply a data product status policy defined by ``handler``."""

    if hasattr(handler, "validate_data_product_status") and callable(handler.validate_data_product_status): # type: ignore
        handler.validate_data_product_status( # type: ignore
            data_product=data_product,
            enforce=enforce,
            operation=operation,
        )
        return

    _validate_data_product_status(
        data_product=data_product,
        enforce=enforce,
        operation=operation,
    )


def _select_data_product(
    *,
    service: DataProductServiceClient,
    data_product_id: str,
    version_spec: Optional[str],
    handler: object,
    enforce: bool,
    operation: str,
    status_enforce: Optional[bool] = None,
) -> Optional[OpenDataProductStandard]:
    """Return a data product respecting ``handler`` status policies."""

    requirement = version_spec.strip() if isinstance(version_spec, str) else ""
    direct_version = None
    policy_enforce = enforce if status_enforce is None else status_enforce
    if requirement and not requirement.startswith(">="):
        direct_version = _normalise_version_spec(requirement)
    if direct_version:
        try:
            product = service.get(data_product_id, direct_version)
        except Exception:
            if enforce:
                raise
            logger.warning(
                "Data product %s version %s could not be retrieved",
                data_product_id,
                direct_version,
            )
            return None
        _enforce_data_product_status(
            handler=handler,
            data_product=product,
            enforce=policy_enforce,
            operation=operation,
        )
        if not _check_data_product_version(
            expected=requirement,
            actual=product.version,
            data_product_id=data_product_id,
            subject="Data product",
            enforce=enforce,
        ):
            return None
        return product

    latest: Optional[OpenDataProductStandard] = None
    try:
        latest = service.latest(data_product_id)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to resolve latest data product %s", data_product_id)

    candidates: list[tuple[Optional[str], Optional[OpenDataProductStandard]]] = []
    if latest is not None:
        candidates.append((latest.version, latest))

    versions: Iterable[str] = ()
    try:
        versions = service.list_versions(data_product_id)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to list versions for data product %s", data_product_id)

    seen_versions: set[str] = set()
    if latest and latest.version:
        seen_versions.add(latest.version)

    sorted_versions = sorted(
        (version for version in versions if version),
        key=version_key,
        reverse=True,
    )
    for version in sorted_versions:
        if version in seen_versions:
            continue
        seen_versions.add(version)
        candidates.append((version, None))

    errors: list[str] = []
    for version, product in candidates:
        candidate = product
        if candidate is None and version:
            try:
                candidate = service.get(data_product_id, version)
            except Exception:
                logger.exception(
                    "Failed to load data product %s version %s", data_product_id, version
                )
                continue
        if candidate is None:
            continue
        try:
            _enforce_data_product_status(
                handler=handler,
                data_product=candidate,
                enforce=policy_enforce,
                operation=operation,
            )
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if requirement:
            matches = _check_data_product_version(
                expected=requirement,
                actual=candidate.version,
                data_product_id=data_product_id,
                subject="Data product",
                enforce=enforce,
            )
            if not matches:
                continue
        return candidate

    if errors:
        message = (
            f"Data product {data_product_id} does not have an allowed version for {operation} operations"
        )
        if enforce:
            detail = "; ".join(dict.fromkeys(errors))
            raise ValueError(f"{message}: {detail}")
        logger.warning("%s: %s", message, "; ".join(dict.fromkeys(errors)))
        return None

    if requirement:
        message = (
            f"Data product {data_product_id} has no versions available for {operation} operations"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
    return None


def _load_binding_product_version(
    *,
    service: DataProductServiceClient,
    data_product_id: str,
    version_spec: Optional[str],
    enforce: bool,
    operation: str,
) -> tuple[Optional[OpenDataProductStandard], bool]:
    """Return the exact product version requested by a binding when available."""

    requirement = _normalise_version_spec(version_spec)
    if not requirement or requirement.startswith(">="):
        return None, False
    try:
        product = service.get(data_product_id, requirement)
    except Exception as exc:
        message = (
            f"Data product {data_product_id} version {requirement} is not available "
            f"for {operation} registration"
        )
        if enforce:
            raise ValueError(message) from exc
        logger.warning(message)
        return None, False
    return product, True
