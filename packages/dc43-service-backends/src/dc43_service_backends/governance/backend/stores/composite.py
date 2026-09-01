"""Composite governance store orchestrating fan-out and role-based routing across multiple backends."""

from __future__ import annotations

import logging
from typing import Any, Mapping, Optional, Sequence

from dc43_service_clients.data_quality import ValidationResult

from .interface import GovernanceStore

logger = logging.getLogger(__name__)


class CompositeGovernanceStore(GovernanceStore):
    """Orchestrate fan-out and role-based routing across multiple governance store backends."""

    def __init__(
        self,
        backends: Mapping[str, GovernanceStore],
        *,
        routes: Mapping[str, Sequence[str] | str] | None = None,
    ) -> None:
        if not backends:
            raise ValueError("CompositeGovernanceStore requires at least one backend.")
        self._backends: dict[str, GovernanceStore] = dict(backends)
        self._routes: dict[str, list[str]] = {}
        if routes:
            for key, val in routes.items():
                if isinstance(val, str):
                    self._routes[str(key).strip().lower()] = [
                        item.strip() for item in val.split(",") if item.strip()
                    ]
                elif isinstance(val, (list, tuple, set)):
                    self._routes[str(key).strip().lower()] = [
                        str(item).strip() for item in val if str(item).strip()
                    ]

    # ------------------------------------------------------------------
    # Routing Resolution Helpers
    # ------------------------------------------------------------------

    def _resolve_backends(self, route_key: str) -> list[GovernanceStore]:
        """Return the sequence of backends registered for ``route_key``."""
        normalised_key = route_key.strip().lower()
        backend_names = self._routes.get(normalised_key)

        if not backend_names:
            # Fallback catch-all keys: "all", "*", "default", "catch_all"
            for fallback_key in ("all", "*", "default", "catch_all"):
                if fallback_key in self._routes and self._routes[fallback_key]:
                    backend_names = self._routes[fallback_key]
                    break

        if not backend_names:
            # If no routes or catch-all configured, broadcast across all declared backends
            return list(self._backends.values())

        resolved: list[GovernanceStore] = []
        for name in backend_names:
            store = self._backends.get(name)
            if store is not None:
                resolved.append(store)
            else:
                logger.warning(
                    "CompositeGovernanceStore route '%s' references unknown backend '%s'. Available backends: %s",
                    route_key,
                    name,
                    list(self._backends.keys()),
                )
        return resolved or list(self._backends.values())

    # ------------------------------------------------------------------
    # GovernanceStore Protocol Implementation
    # ------------------------------------------------------------------

    def save_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        status: ValidationResult | None,
    ) -> None:
        """Persist validation status across routed status backends."""
        targets = self._resolve_backends("status")
        for store in targets:
            try:
                store.save_status(
                    contract_id=contract_id,
                    contract_version=contract_version,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    status=status,
                )
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to save status in composite store backend %s", store)

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        """Return the stored validation status from the primary responsive status backend."""
        targets = self._resolve_backends("status")
        for store in targets:
            try:
                result = store.load_status(
                    contract_id=contract_id,
                    contract_version=contract_version,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                )
                if result is not None:
                    return result
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to load status from composite store backend %s", store)
        return None

    def load_status_matrix_entries(
        self,
        *,
        dataset_id: str,
        dataset_versions: Sequence[str] | None = None,
        contract_ids: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return persisted status rows aggregated across routed status backends."""
        targets = self._resolve_backends("status")
        for store in targets:
            try:
                entries = store.load_status_matrix_entries(
                    dataset_id=dataset_id,
                    dataset_versions=dataset_versions,
                    contract_ids=contract_ids,
                )
                if entries:
                    return entries
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to load status matrix entries from composite store backend %s", store)
        return ()

    def record_pipeline_event(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        event: Mapping[str, object],
        lineage_event: Mapping[str, object] | None = None,
    ) -> None:
        """Append event metadata to the pipeline activity log across routed activity backends."""
        targets = self._resolve_backends("activity")
        for store in targets:
            try:
                store.record_pipeline_event(
                    contract_id=contract_id,
                    contract_version=contract_version,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    event=event,
                    lineage_event=lineage_event,
                )
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to record pipeline event in composite store backend %s", store)

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return pipeline activity entries from the primary responsive activity backend."""
        targets = self._resolve_backends("activity")
        for store in targets:
            try:
                activity = store.load_pipeline_activity(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                )
                if activity:
                    return activity
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to load pipeline activity from composite store backend %s", store)
        return ()

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        """Persist an association between the dataset version and contract across routed link backends."""
        targets = self._resolve_backends("links")
        for store in targets:
            try:
                store.link_dataset_contract(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    contract_id=contract_id,
                    contract_version=contract_version,
                )
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to link dataset contract in composite store backend %s", store)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        """Return the contract reference linked to the dataset from the primary link backend."""
        targets = self._resolve_backends("links")
        for store in targets:
            try:
                linked = store.get_linked_contract_version(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                )
                if linked is not None:
                    return linked
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to get linked contract version from composite store backend %s", store)
        return None

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return stored metric entries from the primary responsive metrics backend."""
        targets = self._resolve_backends("metrics")
        for store in targets:
            try:
                metrics = store.load_metrics(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    contract_id=contract_id,
                    contract_version=contract_version,
                )
                if metrics:
                    return metrics
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to load metrics from composite store backend %s", store)
        return ()

    def list_datasets(self) -> Sequence[str]:
        """Return unique dataset identifiers recorded across all available backends."""
        targets = self._resolve_backends("datasets")
        seen: set[str] = set()
        for store in targets:
            try:
                for ds in store.list_datasets():
                    if ds:
                        seen.add(str(ds))
            except (NotImplementedError, AttributeError):
                continue
            except Exception:
                logger.exception("Failed to list datasets from composite store backend %s", store)
        return sorted(seen)


__all__ = ["CompositeGovernanceStore"]
