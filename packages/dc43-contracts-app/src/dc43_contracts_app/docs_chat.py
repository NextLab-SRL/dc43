from __future__ import annotations

"""Documentation-driven chat assistant for the dc43 app."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence, Tuple, Literal
import json
import logging
import os
import threading

from .config import DocsChatConfig
from .workspace import ContractsAppWorkspace

logger = logging.getLogger(__name__)

__all__ = [
    "DocsChatConfig",
    "DocsChatError",
    "DocsChatReply",
    "DocsChatStatus",
    "configure",
    "generate_reply",
    "mount_gradio_app",
    "status",
    "GRADIO_MOUNT_PATH",
]


@dataclass(slots=True)
class DocsChatStatus:
    """Current readiness information for the documentation assistant."""

    enabled: bool
    ready: bool
    message: str | None = None
    ui_available: bool = False


@dataclass(slots=True)
class DocsChatReply:
    """Normalised response returned by the assistant."""

    answer: str
    sources: List[str]

    def render_markdown(self) -> str:
        """Return a Markdown representation of the reply including sources."""

        if not self.sources:
            return self.answer
        lines = [self.answer.rstrip(), "", "**Sources**:"]
        lines.extend(f"- {source}" for source in self.sources)
        return "\n".join(lines)


class DocsChatError(RuntimeError):
    """Raised when the docs assistant cannot fulfil a request."""


@dataclass(slots=True)
class _ContentSource:
    root: Path
    kind: Literal["docs", "code"]


@dataclass(slots=True)
class _DocsChatRuntime:
    config: DocsChatConfig
    docs_root: Path
    index_dir: Path
    manifest: Mapping[str, object]
    chain: object
    embeddings_model: str
    content_sources: tuple[_ContentSource, ...]


_GRADIO_MOUNT_PATH = "/docs-chat/assistant"
_INSTALL_EXTRA_HINT = (
    "Install the docs-chat extra (pip install --no-cache-dir -e \".[demo]\" from a source checkout, "
    "or pip install 'dc43-contracts-app[docs-chat]' from PyPI) to use the assistant. Avoid combining both commands in the same "
    "environment—pip will treat them as conflicting installs."
)
_INSTALL_GRADIO_HINT = (
    "Install Gradio via the docs-chat extra (pip install --no-cache-dir -e \".[demo]\" or "
    "pip install 'dc43-contracts-app[docs-chat]') to use the embedded UI."
)

_DEFAULT_CODE_DIR_NAMES = ("src", "packages")
_CODE_FILE_PATTERNS = (
    "*.py",
    "*.pyi",
    "*.ts",
    "*.tsx",
    "*.js",
    "*.jsx",
    "*.sql",
    "*.scala",
    "*.yaml",
    "*.yml",
    "*.json",
    "*.toml",
)
_EXCLUDED_DIR_NAMES = {
    ".git",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "dist",
    "build",
    "__snapshots__",
}
_EXCLUDED_GLOBS = tuple(f"**/{name}/**" for name in _EXCLUDED_DIR_NAMES)

_CONFIG: DocsChatConfig | None = None
_WORKSPACE: ContractsAppWorkspace | None = None
_RUNTIME: _DocsChatRuntime | None = None
_RUNTIME_LOCK = threading.Lock()

_EMBEDDING_BATCH_SIZE = 32

_QA_PROMPT_TEMPLATE = """
You are the documentation assistant for the dc43 platform. Use the Markdown
and source code context provided below to answer the user's question with
practical guidance, explicit references to relevant files or headings, and
concrete next steps.

- Only answer questions that relate to the dc43 platform, its setup, usage,
  architecture, integrations, or deployment. If a request is unrelated, politely
  decline and remind the user that you only support dc43 topics.
- Always ground your reply in the supplied context snippets. Quote or summarise
  the most relevant passages so the reader understands how to proceed.
- Mention the filename (for example `docs/implementations/spark.md` or
  `packages/dc43-contracts-app/src/...`) or heading when you cite instructions
  from the context.
- When the context does not directly answer the question, acknowledge the gap
  and point to the closest matching guidance instead of replying with “I don't
  know”.

Conversation so far:
{chat_history}

Context snippets:
{context}

User question:
{question}

Answer:
"""

_OUT_OF_SCOPE_MESSAGE = (
    "I can help with dc43 setup, architecture, and usage questions only. "
    "Please share a dc43-specific task or topic so I can look up the right guidance."
)


def _candidate_docs_roots() -> list[Path]:
    """Return possible documentation directories ordered by preference."""

    candidates: list[Path] = []
    seen: set[Path] = set()

    def _remember(path: Path) -> None:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    def _extend_from(base: Path) -> None:
        for parent in (base,) + tuple(base.parents):
            if parent.name == "docs":
                _remember(parent)
            else:
                _remember(parent / "docs")

    module_base = Path(__file__).resolve().parent
    _extend_from(module_base)

    cwd = Path.cwd()
    _extend_from(cwd)

    try:
        import dc43  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - optional dependency
        pass
    else:
        _extend_from(Path(dc43.__file__).resolve().parent)  # type: ignore[attr-defined]

    return candidates


def _candidate_code_paths() -> list[Path]:
    """Return likely source directories that should be indexed."""

    candidates: list[Path] = []
    seen: set[Path] = set()

    def _remember(path: Path) -> None:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen:
            return
        if not path.exists() or not path.is_dir():
            return
        seen.add(key)
        candidates.append(path)

    def _extend_from(base: Path) -> None:
        for parent in (base,) + tuple(base.parents):
            for name in _DEFAULT_CODE_DIR_NAMES:
                _remember(parent / name)

    module_base = Path(__file__).resolve().parent
    _extend_from(module_base)

    cwd = Path.cwd()
    _extend_from(cwd)

    try:
        import dc43  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - optional dependency
        pass
    else:
        _extend_from(Path(dc43.__file__).resolve().parent)  # type: ignore[attr-defined]

    return candidates


def _resolve_code_paths(config: DocsChatConfig) -> list[Path]:
    if config.code_paths:
        resolved: list[Path] = []
        for path in config.code_paths:
            if path and path.exists() and path.is_dir():
                resolved.append(path)
        return resolved
    return _candidate_code_paths()


def _resolve_content_sources(config: DocsChatConfig) -> list[_ContentSource]:
    docs_root = _resolve_docs_root(config)
    sources: list[_ContentSource] = [_ContentSource(root=docs_root, kind="docs")]
    seen: set[Path] = set()
    try:
        seen.add(docs_root.resolve())
    except OSError:
        seen.add(docs_root)
    for path in _resolve_code_paths(config):
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen:
            continue
        seen.add(key)
        sources.append(_ContentSource(root=path, kind="code"))
    return sources


def configure(config: DocsChatConfig, workspace: ContractsAppWorkspace) -> None:
    """Store the active configuration and reset cached state."""

    global _CONFIG, _WORKSPACE, _RUNTIME
    with _RUNTIME_LOCK:
        _CONFIG = config
        _WORKSPACE = workspace
        _RUNTIME = None


def status() -> DocsChatStatus:
    """Return readiness information for the documentation assistant."""

    config = _CONFIG
    if not config or not config.enabled:
        return DocsChatStatus(
            enabled=False,
            ready=False,
            message="Enable docs_chat in the dc43 app configuration to activate the documentation assistant.",
            ui_available=False,
        )

    docs_root = _resolve_docs_root(config)
    if not docs_root.exists():
        return DocsChatStatus(
            enabled=True,
            ready=False,
            message=f"Documentation directory not found: {docs_root}",
            ui_available=False,
        )

    if config.code_paths:
        missing_code_dirs = [path for path in config.code_paths if not path.exists()]
        if len(missing_code_dirs) == len(config.code_paths):
            missing = ", ".join(str(path) for path in config.code_paths)
            return DocsChatStatus(
                enabled=True,
                ready=False,
                message=f"Code directories not found: {missing}",
                ui_available=_check_ui_dependencies()[0],
            )
        if missing_code_dirs:
            logger.warning("Skipping missing docs chat code directories: %s", ", ".join(str(path) for path in missing_code_dirs))

    if config.provider.lower() != "openai":
        return DocsChatStatus(
            enabled=True,
            ready=False,
            message="Only the OpenAI provider is supported by the bundled docs chat helper.",
            ui_available=_check_ui_dependencies()[0],
        )

    core_ready, dependency_message = _check_core_dependencies()
    if not core_ready:
        return DocsChatStatus(
            enabled=True,
            ready=False,
            message=dependency_message,
            ui_available=False,
        )

    api_key = _resolve_api_key(config)
    if not api_key:
        return DocsChatStatus(
            enabled=True,
            ready=False,
            message=_missing_api_key_message(config),
            ui_available=_check_ui_dependencies()[0],
        )

    ui_ready, _ = _check_ui_dependencies()
    return DocsChatStatus(enabled=True, ready=True, message=None, ui_available=ui_ready)


def generate_reply(message: str, history: Sequence[Tuple[str, str]] | Sequence[Mapping[str, str]]) -> DocsChatReply:
    """Return an assistant response for ``message`` using ``history`` for context."""

    if not message.strip():
        raise DocsChatError("Provide a question so the assistant can look up matching documentation snippets.")

    runtime = _ensure_runtime()
    chat_history = _normalise_history(history)
    try:
        result = runtime.chain({"question": message, "chat_history": chat_history})
    except Exception as exc:  # pragma: no cover - defensive guard around provider errors
        raise DocsChatError(str(exc)) from exc

    sources = _extract_sources(result, runtime)
    if not sources:
        answer_text = _OUT_OF_SCOPE_MESSAGE
    else:
        answer_text = _extract_answer_text(result)
    return DocsChatReply(answer=answer_text, sources=sources)


def mount_gradio_app(app: "FastAPI", path: str = _GRADIO_MOUNT_PATH) -> bool:
    """Mount the Gradio UI when dependencies and credentials are available."""

    status_payload = status()
    if not status_payload.enabled or not status_payload.ready or not status_payload.ui_available:
        return False

    try:
        import gradio as gr
    except ModuleNotFoundError:  # pragma: no cover - guarded by ``status``
        logger.warning("Gradio is not installed; the docs chat UI will not be mounted.")
        return False

    def _respond(message: str, history: list[tuple[str, str]]) -> str:
        try:
            reply = generate_reply(message, history)
            return reply.render_markdown()
        except DocsChatError as exc:
            return f"⚠️ {exc}"

    interface = gr.ChatInterface(
        fn=_respond,
        title="dc43 docs assistant",
        description=(
            "Ask questions about the dc43 platform, architecture, deployment, and integration guides. "
            "Answers cite the Markdown sources that power the assistant."
        ),
        examples=[
            "How do I configure the contracts backend for a remote deployment?",
            "Where can I find the setup wizard automation instructions?",
            "Which guides describe the Spark integration helpers?",
        ],
        cache_examples=False,
    )

    try:
        from gradio import mount_gradio_app as gr_mount
    except ImportError:  # pragma: no cover - compatibility for older Gradio releases
        gr_mount = None

    if gr_mount is None:  # pragma: no cover - fallback path
        app.mount(path, interface)  # type: ignore[arg-type]
    else:
        gr_mount(app, interface, path=path)

    return True


def _ensure_runtime() -> _DocsChatRuntime:
    status_payload = status()
    if not status_payload.enabled:
        raise DocsChatError(status_payload.message or "Docs chat is disabled in the current configuration.")
    if not status_payload.ready:
        raise DocsChatError(status_payload.message or "Docs chat is not ready yet.")

    with _RUNTIME_LOCK:
        global _RUNTIME
        runtime = _RUNTIME
        if runtime is not None and _manifest_matches(runtime):
            return runtime

        runtime = _build_runtime()
        _RUNTIME = runtime
        return runtime


def _build_runtime() -> _DocsChatRuntime:
    config = _CONFIG
    workspace = _WORKSPACE
    if config is None or workspace is None:
        raise DocsChatError("Docs chat has not been initialised with a workspace.")

    content_sources = _resolve_content_sources(config)
    docs_root = content_sources[0].root
    index_dir = _resolve_index_dir(config, workspace)
    index_dir.mkdir(parents=True, exist_ok=True)

    manifest = _current_manifest_payload(config, content_sources)
    manifest_path = index_dir / "manifest.json"
    if manifest_path.exists() and (index_dir / "index.faiss").exists():
        stored = _load_manifest(manifest_path)
        if stored == manifest:
            vectorstore = _load_vectorstore(index_dir, config)
            chain = _build_chain(config, vectorstore)
            return _DocsChatRuntime(
                config=config,
                docs_root=docs_root,
                index_dir=index_dir,
                manifest=manifest,
                chain=chain,
                embeddings_model=config.embedding_model,
                content_sources=tuple(content_sources),
            )

    documents = _load_documents(content_sources)
    vectorstore = _build_vectorstore(config, documents)
    _save_vectorstore(index_dir, vectorstore)
    _write_manifest(manifest_path, manifest)
    chain = _build_chain(config, vectorstore)
    return _DocsChatRuntime(
        config=config,
        docs_root=docs_root,
        index_dir=index_dir,
        manifest=manifest,
        chain=chain,
        embeddings_model=config.embedding_model,
        content_sources=tuple(content_sources),
    )


def _build_chain(config: DocsChatConfig, vectorstore: object) -> object:
    try:
        from langchain.chains import ConversationalRetrievalChain
        from langchain_core.prompts import PromptTemplate
        from langchain_openai import ChatOpenAI
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    api_key = _resolve_api_key(config)
    if not api_key:
        raise DocsChatError(_missing_api_key_message(config))

    llm_kwargs: dict[str, object] = {
        "model": config.model,
        "openai_api_key": api_key,
        "temperature": 0.2,
    }
    if config.reasoning_effort:
        llm_kwargs["model_kwargs"] = {"reasoning": {"effort": config.reasoning_effort}}
    llm = ChatOpenAI(**llm_kwargs)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 6})
    qa_prompt = PromptTemplate.from_template(_QA_PROMPT_TEMPLATE)
    chain = ConversationalRetrievalChain.from_llm(
        llm,
        retriever=retriever,
        return_source_documents=True,
    )
    _apply_prompt_override(chain, qa_prompt)
    return chain


def _apply_prompt_override(chain: object, prompt: object) -> None:
    """Best-effort override of the QA prompt for the retrieval chain."""

    combine_chain = getattr(chain, "combine_docs_chain", None)
    if combine_chain is None:
        return

    llm_chain = getattr(combine_chain, "llm_chain", None)
    if llm_chain is not None and hasattr(llm_chain, "prompt"):
        llm_chain.prompt = prompt  # type: ignore[assignment]
        return

    if hasattr(combine_chain, "prompt"):
        combine_chain.prompt = prompt  # type: ignore[assignment]


def _load_vectorstore(index_dir: Path, config: DocsChatConfig) -> object:
    try:
        from langchain_community.vectorstores import FAISS
        from langchain_openai import OpenAIEmbeddings
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    api_key = _resolve_api_key(config)
    if not api_key:
        raise DocsChatError(_missing_api_key_message(config))

    embeddings = OpenAIEmbeddings(model=config.embedding_model, openai_api_key=api_key)
    return FAISS.load_local(
        str(index_dir),
        embeddings,
        allow_dangerous_deserialization=True,
    )


def _build_vectorstore(config: DocsChatConfig, documents: Sequence[object]) -> object:
    try:
        from langchain_community.vectorstores import FAISS
        from langchain_openai import OpenAIEmbeddings
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)
    splits = splitter.split_documents(documents)
    api_key = _resolve_api_key(config)
    if not api_key:
        raise DocsChatError(_missing_api_key_message(config))

    embeddings = OpenAIEmbeddings(model=config.embedding_model, openai_api_key=api_key)
    if not splits:
        raise DocsChatError(
            "No documentation content was loaded; confirm docs_chat paths point to Markdown or code."
        )

    vectorstore = None
    for start in range(0, len(splits), _EMBEDDING_BATCH_SIZE):
        batch = splits[start : start + _EMBEDDING_BATCH_SIZE]
        if not batch:
            continue
        if vectorstore is None:
            vectorstore = FAISS.from_documents(batch, embeddings)
        else:
            vectorstore.add_documents(batch)

    if vectorstore is None:
        raise DocsChatError(
            "Failed to build the documentation index after batching embeddings."
        )

    return vectorstore


def _save_vectorstore(index_dir: Path, vectorstore: object) -> None:
    try:
        vectorstore.save_local(str(index_dir))  # type: ignore[call-arg]
    except Exception as exc:  # pragma: no cover - defensive guard
        raise DocsChatError(f"Failed to persist the documentation index: {exc}") from exc


def _load_documents(content_sources: Sequence[_ContentSource]) -> Sequence[object]:
    try:
        from langchain_community.document_loaders import DirectoryLoader, TextLoader
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    documents: list[object] = []

    for source in content_sources:
        if not source.root.exists():
            continue

        if source.kind == "docs":
            loader = DirectoryLoader(
                str(source.root),
                glob="**/*.md",
                loader_cls=TextLoader,
                show_progress=True,
                use_multithreading=True,
                exclude=_EXCLUDED_GLOBS,
                loader_kwargs={"autodetect_encoding": True},
            )
            loaded = loader.load()
        else:
            loaded = []
            for pattern in _CODE_FILE_PATTERNS:
                loader = DirectoryLoader(
                    str(source.root),
                    glob=f"**/{pattern}",
                    loader_cls=TextLoader,
                    show_progress=False,
                    use_multithreading=True,
                    exclude=_EXCLUDED_GLOBS,
                    loader_kwargs={"autodetect_encoding": True},
                )
                loaded.extend(loader.load())

        for document in loaded:
            metadata = getattr(document, "metadata", None)
            if isinstance(metadata, dict):
                metadata.setdefault("root_path", str(source.root))
                metadata.setdefault("source_kind", source.kind)
                source_path = metadata.get("source")
                relative_value: str | None = None
                if isinstance(source_path, str):
                    try:
                        relative_path = Path(source_path).resolve().relative_to(source.root.resolve())
                    except Exception:
                        try:
                            relative_path = Path(source_path).relative_to(source.root)
                        except Exception:
                            relative_path = Path(source_path).name
                    relative_value = str(relative_path).replace(os.sep, "/")
                if relative_value:
                    metadata.setdefault("relative_path", relative_value)
            documents.append(document)

    return documents


def _current_manifest_payload(
    config: DocsChatConfig, content_sources: Sequence[_ContentSource]
) -> Mapping[str, object]:
    roots_payload: list[Mapping[str, object]] = []

    for source in content_sources:
        files: list[tuple[str, float]] = []
        patterns = ["*.md"] if source.kind == "docs" else list(_CODE_FILE_PATTERNS)
        for pattern in patterns:
            for path in sorted(source.root.rglob(pattern)):
                if _is_excluded(path):
                    continue
                try:
                    timestamp = path.stat().st_mtime
                except OSError:
                    continue
                try:
                    relative = path.relative_to(source.root)
                except ValueError:
                    relative = Path(path.name)
                files.append((str(relative).replace(os.sep, "/"), float(timestamp)))
        roots_payload.append(
            {
                "kind": source.kind,
                "path": str(source.root),
                "files": files,
            }
        )

    return {
        "content_roots": roots_payload,
        "provider": config.provider,
        "model": config.model,
        "embedding_model": config.embedding_model,
    }


def _load_manifest(path: Path) -> Mapping[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):  # pragma: no cover - defensive
        return {}


def _write_manifest(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _manifest_matches(runtime: _DocsChatRuntime) -> bool:
    current = _current_manifest_payload(runtime.config, list(runtime.content_sources))
    return current == runtime.manifest


def _is_excluded(path: Path) -> bool:
    return any(part in _EXCLUDED_DIR_NAMES for part in path.parts)


def _resolve_docs_root(config: DocsChatConfig) -> Path:
    if config.docs_path:
        return Path(config.docs_path).expanduser()

    for candidate in _candidate_docs_roots():
        if candidate.exists():
            return candidate

    package_root = Path(__file__).resolve().parents[4]
    return package_root / "docs"


def _resolve_index_dir(config: DocsChatConfig, workspace: ContractsAppWorkspace) -> Path:
    if config.index_path:
        return Path(config.index_path).expanduser()
    return workspace.root / "docs_chat" / "index"


def _missing_api_key_message(config: DocsChatConfig) -> str:
    env_name = config.api_key_env.strip() if config.api_key_env else ""
    if env_name:
        return (
            "Provide an API key via docs_chat.api_key or set the "
            f"{env_name} environment variable before retrying."
        )
    return "Provide an API key via docs_chat.api_key before retrying."


def _resolve_api_key(config: DocsChatConfig) -> str | None:
    if config.api_key:
        return config.api_key.strip() or None
    if not config.api_key_env:
        return None
    value = os.getenv(config.api_key_env)
    if value:
        return value.strip() or None
    return None


def _normalise_history(history: Sequence[Tuple[str, str]] | Sequence[Mapping[str, str]]) -> List[Tuple[str, str]]:
    normalised: List[Tuple[str, str]] = []
    for item in history:
        if isinstance(item, Mapping):
            user = str(item.get("user") or item.get("human") or "")
            assistant = str(item.get("assistant") or item.get("ai") or "")
            normalised.append((user, assistant))
        elif isinstance(item, Sequence) and len(item) == 2:
            user = "" if item[0] is None else str(item[0])
            assistant = "" if item[1] is None else str(item[1])
            normalised.append((user, assistant))
    return normalised


def _extract_answer_text(result: Mapping[str, object]) -> str:
    answer = result.get("answer") or result.get("result")
    if isinstance(answer, str) and answer.strip():
        return answer
    return "I could not find a relevant answer in the documentation."  # pragma: no cover - fallback path


def _extract_sources(result: Mapping[str, object], runtime: _DocsChatRuntime) -> List[str]:
    raw_sources = result.get("source_documents")
    if not isinstance(raw_sources, Iterable):
        return []
    seen: set[str] = set()
    sources: List[str] = []
    for item in raw_sources:
        try:
            metadata = getattr(item, "metadata", {})
        except Exception:  # pragma: no cover - defensive fallback
            metadata = {}
        if isinstance(metadata, Mapping):
            display = _source_display_from_metadata(metadata, runtime)
            if display and display not in seen:
                seen.add(display)
                sources.append(display)
                continue
        source_path = metadata.get("source") if isinstance(metadata, Mapping) else None
        if not isinstance(source_path, str):
            continue
        path = Path(source_path)
        value = _source_display_from_path(path, runtime.content_sources)
        if value not in seen:
            seen.add(value)
            sources.append(value)
    return sources


def _source_display_from_metadata(metadata: Mapping[str, object], runtime: _DocsChatRuntime) -> str | None:
    root_hint = metadata.get("root_path")
    relative_hint = metadata.get("relative_path")
    if isinstance(root_hint, str) and isinstance(relative_hint, str):
        return _format_source_display(Path(root_hint), relative_hint)
    return None


def _source_display_from_path(path: Path, content_sources: Sequence[_ContentSource]) -> str:
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    for source in content_sources:
        try:
            relative = resolved.relative_to(source.root.resolve())
        except Exception:
            try:
                relative = resolved.relative_to(source.root)
            except Exception:
                continue
        return _format_source_display(source.root, str(relative))
    return path.name


def _format_source_display(root: Path, relative: str) -> str:
    clean_relative = relative.replace("\\", "/").lstrip("./")
    prefix = root.name or root.as_posix()
    if clean_relative:
        return f"{prefix}/{clean_relative}"
    return prefix


def _check_core_dependencies() -> tuple[bool, str | None]:
    try:
        import langchain  # noqa: F401
        import langchain_community  # noqa: F401
        import langchain_openai  # noqa: F401
        import langchain_text_splitters  # noqa: F401
    except ModuleNotFoundError:
        return (False, _INSTALL_EXTRA_HINT)
    return True, None


def _check_ui_dependencies() -> tuple[bool, str | None]:
    try:
        import gradio  # noqa: F401
    except ModuleNotFoundError:
        return (False, _INSTALL_GRADIO_HINT)
    return True, None


GRADIO_MOUNT_PATH = _GRADIO_MOUNT_PATH
