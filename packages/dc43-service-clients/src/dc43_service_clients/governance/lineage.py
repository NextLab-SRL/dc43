"""Models describing Open Data Lineage events exchanged with governance APIs."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_DNS, UUID, uuid5

if TYPE_CHECKING:  # pragma: no cover - typing aid only
    from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
    from openlineage.client.run import RunEvent as OpenDataLineageEvent
else:  # pragma: no cover - runtime typing fallback only
    try:
        from openlineage.client.run import RunEvent as OpenDataLineageEvent
    except ImportError:  # Optional dependency is not installed
        OpenDataLineageEvent = object

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


@lru_cache(maxsize=1)
def _openlineage_models():
    try:
        from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
    except ImportError as exc:  # pragma: no cover - optional dependency guard
        raise ModuleNotFoundError(
            "OpenLineage support requires the optional 'lineage' extra; install "
            "dc43-service-clients[lineage] to use governance lineage helpers."
        ) from exc
    return Dataset, Job, Run, RunEvent, RunState


def _ensure_run_state(value: str) -> "RunState":
    RunState = _openlineage_models()[-1]
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


def _build_datasets(entries: Any) -> list["Dataset"]:
    Dataset, *_ = _openlineage_models()
    datasets: list[Dataset] = []
    for entry in _as_sequence(entries):
        namespace = str(entry.get("namespace") or "").strip()
        name = str(entry.get("name") or "").strip()
        if not namespace or not name:
            continue
        facets = dict(_as_mapping(entry.get("facets")))
        datasets.append(Dataset(namespace=namespace, name=name, facets=facets or None))
    return datasets


def encode_lineage_event(event: "RunEvent") -> Mapping[str, Any]:
    """Serialise ``event`` into a mapping suitable for transport."""

    Dataset, Job, Run, RunEvent, _ = _openlineage_models()

    def _convert(value: object) -> object:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, (RunEvent, Run, Job, Dataset)):
            # Attrs-based models expose their data via ``__dict__`` so a shallow
            # copy is sufficient before recursing into individual fields.
            return {key: _convert(item) for key, item in value.__dict__.items()}
        if isinstance(value, Mapping):
            return {str(key): _convert(item) for key, item in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [_convert(item) for item in value]
        return value

    def _prune(value: object) -> object | None:
        if value is None:
            return None
        if isinstance(value, dict):
            cleaned: MutableMapping[str, object] = {}
            for key, item in value.items():
                normalised = _prune(item)
                if normalised is None:
                    continue
                if isinstance(normalised, dict) and not normalised:
                    continue
                cleaned[str(key)] = normalised
            return dict(cleaned)
        if isinstance(value, list):
            cleaned_list = []
            for item in value:
                normalised = _prune(item)
                if normalised is None:
                    continue
                if isinstance(normalised, dict) and not normalised:
                    continue
                cleaned_list.append(normalised)
            return cleaned_list
        return value

    payload = _convert(event)
    if isinstance(payload, dict):
        payload.setdefault("schemaURL", DEFAULT_SCHEMA_URL)
    pruned = _prune(payload)
    return pruned if isinstance(pruned, Mapping) else {}


def decode_lineage_event(raw: Mapping[str, Any] | None) -> "RunEvent" | None:
    """Convert ``raw`` payloads into :class:`RunEvent` instances."""

    Dataset, Job, Run, RunEvent, _ = _openlineage_models()

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


__all__ = [
    "OpenDataLineageEvent",
    "decode_lineage_event",
    "encode_lineage_event",
]
