"""Models describing Open Data Lineage events exchanged with governance APIs."""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, MutableMapping, Sequence
from uuid import NAMESPACE_DNS, UUID, uuid5

try:
    import attr
except ModuleNotFoundError:
    try:  # pragma: no cover - import-time fallback for environments with ``attrs`` only
        import attrs as attr  # type: ignore[assignment]
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(
            "The 'attrs' dependency is required for OpenLineage governance support."
        ) from exc
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState

DEFAULT_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    payload: list[Mapping[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            payload.append(dict(item))
    return tuple(payload)


def _ensure_run_state(value: str) -> RunState:
    text = value.strip()
    if not text:
        raise ValueError("lineage event requires an eventType field")
    try:
        return RunState(text)
    except ValueError:
        try:
            return RunState[text.upper()]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"unknown lineage event state: {value}") from exc


def _ensure_uuid(value: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError("lineage event requires a runId field")
    try:
        return str(UUID(text))
    except (ValueError, AttributeError):
        return str(uuid5(NAMESPACE_DNS, text))


def _build_datasets(entries: Any) -> list[Dataset]:
    datasets: list[Dataset] = []
    for entry in _as_sequence(entries):
        namespace = str(entry.get("namespace") or "").strip()
        name = str(entry.get("name") or "").strip()
        if not namespace or not name:
            continue
        facets = dict(_as_mapping(entry.get("facets")))
        datasets.append(Dataset(namespace=namespace, name=name, facets=facets or None))
    return datasets


def encode_lineage_event(event: RunEvent) -> Mapping[str, Any]:
    """Serialise ``event`` into a mapping suitable for transport."""

    def _serialise(instance: object, attribute: attr.Attribute[Any], value: Any) -> Any:  # noqa: ANN001
        if isinstance(value, Enum):
            return value.value
        return value

    payload = attr.asdict(event, value_serializer=_serialise)

    def _prune(value: Any) -> Any:
        if isinstance(value, dict):
            pruned = {k: _prune(v) for k, v in value.items() if _should_keep(v)}
            return pruned
        if isinstance(value, list):
            pruned_list = [_prune(item) for item in value]
            return [item for item in pruned_list if _should_keep(item)]
        if isinstance(value, tuple):
            pruned_items = tuple(_prune(item) for item in value)
            return tuple(item for item in pruned_items if _should_keep(item))
        return value

    def _should_keep(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, dict):
            return bool(value)
        if isinstance(value, (list, tuple, set)):
            return True
        return True

    return _prune(payload)


def decode_lineage_event(raw: Mapping[str, Any] | None) -> RunEvent | None:
    """Convert ``raw`` payloads into :class:`RunEvent` instances."""

    if raw is None:
        return None

    event_type = _ensure_run_state(str(raw.get("eventType") or raw.get("event_type") or ""))
    event_time = str(raw.get("eventTime") or raw.get("event_time") or "").strip()
    if not event_time:
        raise ValueError("lineage event requires an eventTime field")
    producer = str(raw.get("producer") or "").strip()
    schema_url = str(raw.get("schemaURL") or raw.get("schemaUrl") or "").strip()

    run_payload = dict(_as_mapping(raw.get("run")))
    run_id = str(run_payload.get("runId") or run_payload.get("run_id") or "").strip()
    run_facets = dict(_as_mapping(run_payload.get("facets")))
    run = Run(runId=_ensure_uuid(run_id), facets=run_facets or None)

    job_payload = dict(_as_mapping(raw.get("job")))
    namespace = str(
        job_payload.get("namespace")
        or job_payload.get("jobNamespace")
        or job_payload.get("namespace_name")
        or ""
    ).strip()
    name = str(job_payload.get("name") or job_payload.get("jobName") or "").strip()
    if not namespace or not name:
        raise ValueError("lineage event requires job namespace and name")
    job_facets = dict(_as_mapping(job_payload.get("facets")))
    job = Job(namespace=namespace, name=name, facets=job_facets or None)

    inputs = _build_datasets(raw.get("inputs"))
    outputs = _build_datasets(raw.get("outputs"))

    kwargs: MutableMapping[str, Any] = {}
    if schema_url:
        kwargs["schemaURL"] = schema_url
    else:
        kwargs["schemaURL"] = DEFAULT_SCHEMA_URL

    return RunEvent(
        eventType=event_type,
        eventTime=event_time,
        run=run,
        job=job,
        producer=producer,
        inputs=inputs,
        outputs=outputs,
        **kwargs,
    )


OpenDataLineageEvent = RunEvent

__all__ = [
    "OpenDataLineageEvent",
    "decode_lineage_event",
    "encode_lineage_event",
]
