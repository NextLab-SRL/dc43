"""Integration-provided helpers for setup bundle pipeline stubs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Mapping, Protocol, Sequence


class PipelineStubProvider(Protocol):
    """Protocol describing callables that produce stub fragments."""

    def __call__(
        self,
        *,
        hints: Mapping[str, object],
        flags: Mapping[str, bool],
        json_literal: Callable[[object | None], str],
    ) -> "PipelineStub":
        ...


@dataclass(frozen=True)
class PipelineFile:
    """Description of an example file contributed to the bundle."""

    path: str
    content: str
    executable: bool = False


@dataclass(frozen=True)
class PipelineProject:
    """A miniature project shipped alongside the setup bundle."""

    root: str
    entrypoint: str
    files: Sequence[PipelineFile]


@dataclass(frozen=True)
class PipelineStub:
    """Structured fragments for composing pipeline example scripts."""

    bootstrap_imports: Sequence[str] = ()
    additional_imports: Sequence[str] = ()
    helper_functions: Sequence[str] = ()
    main_lines: Sequence[str] = ()
    tail_lines: Sequence[str] = ()
    project: PipelineProject | None = None


PIPELINE_STUB_PROVIDERS: Dict[str, PipelineStubProvider] = {}


def register_pipeline_stub(key: str, provider: PipelineStubProvider) -> None:
    """Register a provider that contributes code fragments for ``key``."""

    PIPELINE_STUB_PROVIDERS[key] = provider


def get_pipeline_stub(
    key: str,
    *,
    hints: Mapping[str, object],
    flags: Mapping[str, bool],
    json_literal: Callable[[object | None], str],
) -> PipelineStub | None:
    """Return the registered stub for ``key`` if one is available."""

    provider = PIPELINE_STUB_PROVIDERS.get(key)
    if provider is None:
        return None
    return provider(hints=hints, flags=flags, json_literal=json_literal)


__all__ = [
    "PIPELINE_STUB_PROVIDERS",
    "PipelineFile",
    "PipelineProject",
    "PipelineStub",
    "get_pipeline_stub",
    "register_pipeline_stub",
]


# Import providers so they self-register with the registry above.
from . import dlt as _dlt  # noqa: E402,F401
from . import spark as _spark  # noqa: E402,F401

