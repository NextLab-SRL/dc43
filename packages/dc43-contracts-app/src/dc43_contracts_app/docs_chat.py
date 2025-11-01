from __future__ import annotations

"""Documentation-driven chat assistant for the dc43 app."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, List, Mapping, Sequence, Tuple, Literal
import json
import logging
import os
import math
from queue import Empty, Queue
import threading

from .config import DocsChatConfig

logger = logging.getLogger(__name__)

__all__ = [
    "DocsChatConfig",
    "DocsChatConfigurationSummary",
    "DocsChatError",
    "DocsChatReply",
    "DocsChatStatus",
    "configure",
    "describe_configuration",
    "generate_reply",
    "mount_gradio_app",
    "status",
    "warm_up",
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
    steps: List[str] = field(default_factory=list)

    def render_markdown(self, include_steps: bool = True) -> str:
        """Return a Markdown representation of the reply including sources."""

        lines = [self.answer.rstrip()]
        if self.sources:
            lines.extend(("", "**Sources**:"))
            lines.extend(f"- {source}" for source in self.sources)
        if include_steps and self.steps:
            lines.extend(("", "**Progress**:"))
            lines.extend(f"- {step}" for step in self.steps)
        return "\n".join(lines)


class DocsChatError(RuntimeError):
    """Raised when the docs assistant cannot fulfil a request."""


@dataclass(slots=True)
class DocsChatConfigurationSummary:
    """Normalised view of the configured documentation assistant."""

    docs_root: Path
    code_paths: tuple[Path, ...]
    index_dir: Path
    embedding_provider: str
    embedding_model: str


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
    embeddings_provider: str
    embeddings_model: str
    content_sources: tuple[_ContentSource, ...]


_GRADIO_MOUNT_PATH = "/docs-chat/assistant"
_INSTALL_EXTRA_HINT = (
    "Install the docs-chat extra (pip install --no-cache-dir -e \".[demo]\" from a source checkout, "
    "or pip install 'dc43-contracts-app[docs-chat]' from PyPI) to use the assistant. Avoid combining both commands in the same "
    "environment—pip will treat them as conflicting installs."
)
_INSTALL_HUGGINGFACE_HINT = (
    "Install langchain-huggingface and sentence-transformers via the docs-chat extra "
    "(pip install --no-cache-dir -e \".[demo]\" or pip install 'dc43-contracts-app[docs-chat]') "
    "to use Hugging Face embeddings."
)
_INSTALL_GRADIO_HINT = (
    "Install Gradio via the docs-chat extra (pip install --no-cache-dir -e \".[demo]\" or "
    "pip install 'dc43-contracts-app[docs-chat]') to use the embedded UI."
)

_SUPPORTED_EMBEDDING_PROVIDERS = {"openai", "huggingface"}
_DEFAULT_HUGGINGFACE_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

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

def _detect_repository_root() -> Path | None:
    module_path = Path(__file__).resolve()
    for parent in module_path.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    return None


_REPOSITORY_ROOT = _detect_repository_root()


def _coerce_message_content(content: object) -> str:
    """Return a string representation of chat message content."""

    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, (bytes, bytearray)):
        try:
            return content.decode()
        except Exception:
            return ""
    if isinstance(content, Sequence):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str):
            return text
    return str(content)


def _coerce_chat_prompt(message: object) -> str:
    """Extract the user prompt from ChatInterface inputs."""

    if isinstance(message, str):
        return message
    role = getattr(message, "role", None)
    content = getattr(message, "content", None)
    if role is not None or content is not None:
        return _coerce_message_content(content)
    if isinstance(message, dict):
        return _coerce_message_content(message.get("content"))
    return _coerce_message_content(message)


def _coerce_chat_history(history: Sequence[object]) -> list[tuple[str, str]]:
    """Convert ChatInterface history payloads into (user, assistant) tuples."""

    if not history:
        return []

    pairs: list[tuple[str, str]] = []
    pending_user: str | None = None

    for entry in history:
        if isinstance(entry, (tuple, list)) and len(entry) == 2:
            user_message = _coerce_message_content(entry[0])
            assistant_message = _coerce_message_content(entry[1])
            pairs.append((user_message, assistant_message))
            pending_user = None
            continue

        role = getattr(entry, "role", None)
        content = getattr(entry, "content", None)
        if isinstance(entry, dict):
            role = entry.get("role", role)
            content = entry.get("content", content)

        if role == "user":
            pending_user = _coerce_message_content(content)
        elif role == "assistant":
            assistant_text = _coerce_message_content(content)
            if pending_user is None:
                pending_user = ""
            pairs.append((pending_user, assistant_text))
            pending_user = None

    return pairs

_CONFIG: DocsChatConfig | None = None
_BASE_DIR: Path | None = None
_RUNTIME: _DocsChatRuntime | None = None
_RUNTIME_LOCK = threading.Lock()
_WARMUP_GUARD = threading.Lock()
_WARMUP_THREAD: threading.Thread | None = None
_WARMUP_MESSAGES: Queue[object] | None = None
_WARMUP_SENTINEL = object()
_WARMUP_COMPLETED = False

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
- When a question covers integrating dc43 with an orchestration or runtime
  (for example a Spark pipeline), call out the concrete helper APIs available in
  the context—such as `read_with_governance`, `write_with_governance`,
  enforcement or status strategies—and outline the minimal steps or code needed
  to drop them into the workflow.
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


def _emit_progress(callback: ProgressCallback | None, message: str) -> None:
    if not callback:
        return
    try:
        callback(message)
    except Exception:  # pragma: no cover - progress is best-effort
        logger.debug("Docs chat progress callback failed", exc_info=True)


def _render_progress_update(step: str, count: int) -> str:
    suffix = "step" if count == 1 else "steps"
    return f"**Working…**\n\n{step}\n\n_{count} {suffix} logged so far._"


def _build_progress_summary(steps: Sequence[str]) -> str:
    bullet_lines = "\n".join(f"- {entry}" for entry in steps)
    summary = (
        "**Processing log**\n\n"
        "<details><summary>Expand processing steps</summary>\n\n"
        f"{bullet_lines}\n\n"
        "</details>"
    )
    return summary


def _consume_warmup_messages(
    queue: Queue[object] | None,
    progress: ProgressCallback | None,
    *,
    block: bool = False,
) -> bool:
    """Forward queued warm-up progress messages to ``progress``."""

    if queue is None:
        return False

    consumed = False
    timeout = 0.2 if block else 0.0
    while True:
        try:
            item = queue.get(timeout=timeout)
        except Empty:
            break
        consumed = True
        if item is _WARMUP_SENTINEL:
            break
        if isinstance(item, str):
            _emit_progress(progress, item)
        timeout = 0.0  # Subsequent reads should not block within the same drain cycle.
    return consumed


def _candidate_docs_roots() -> list[Path]:
    """Return possible documentation directories ordered by preference."""

    candidates: list[Path] = []
    seen: set[Path] = set()

    def _within_repository(path: Path) -> bool:
        if _REPOSITORY_ROOT is None:
            return True
        try:
            path.resolve().relative_to(_REPOSITORY_ROOT.resolve())
        except Exception:
            return False
        return True

    def _remember(path: Path) -> None:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen:
            return
        if not _within_repository(path):
            return
        seen.add(key)
        candidates.append(path)

    def _extend_from(base: Path) -> None:
        for depth, parent in enumerate((base,) + tuple(base.parents)):
            if _REPOSITORY_ROOT is None and depth > 3:
                break
            if not _within_repository(parent):
                break
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

    def _within_repository(path: Path) -> bool:
        if _REPOSITORY_ROOT is None:
            return True
        try:
            path.resolve().relative_to(_REPOSITORY_ROOT.resolve())
        except Exception:
            return False
        return True

    def _remember(path: Path) -> None:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen:
            return
        if not path.exists() or not path.is_dir():
            return
        if not _within_repository(path):
            return
        seen.add(key)
        candidates.append(path)

    def _extend_from(base: Path) -> None:
        for depth, parent in enumerate((base,) + tuple(base.parents)):
            if _REPOSITORY_ROOT is None and depth > 3:
                break
            if not _within_repository(parent):
                break
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


ProgressCallback = Callable[[str], None]


def configure(config: DocsChatConfig, *, base_dir: Path | None = None) -> None:
    """Store the active configuration and reset cached state."""

    global _CONFIG, _BASE_DIR, _RUNTIME, _WARMUP_THREAD, _WARMUP_MESSAGES, _WARMUP_COMPLETED
    with _RUNTIME_LOCK:
        _CONFIG = config
        _BASE_DIR = base_dir
        _RUNTIME = None
    with _WARMUP_GUARD:
        _WARMUP_THREAD = None
        _WARMUP_MESSAGES = None
        _WARMUP_COMPLETED = False


def warm_up(block: bool = False, progress: ProgressCallback | None = None) -> None:
    """Ensure the runtime is ready, optionally warming the cache asynchronously."""

    def _build() -> None:
        global _WARMUP_COMPLETED
        try:
            _ensure_runtime(progress=progress)
        except DocsChatError as exc:
            logger.warning("Docs chat warm-up skipped: %s", exc)
            _WARMUP_COMPLETED = False
        else:
            _WARMUP_COMPLETED = True

    status_payload = status()
    if not status_payload.enabled or not status_payload.ready:
        return

    global _WARMUP_THREAD, _WARMUP_MESSAGES, _WARMUP_COMPLETED

    if _WARMUP_MESSAGES is None:
        _WARMUP_COMPLETED = False

    if _WARMUP_COMPLETED and not block:
        return

    if block:
        _build()
        return

    with _WARMUP_GUARD:
        existing = _WARMUP_THREAD
        if existing and existing.is_alive():
            return
        queue: Queue[object] = Queue()

        def _relay(detail: str) -> None:
            queue.put(detail)
            _emit_progress(progress, detail)

        def _run() -> None:
            global _WARMUP_THREAD, _WARMUP_COMPLETED
            try:
                _ensure_runtime(progress=_relay)
            except DocsChatError as exc:
                warning = f"⚠️ Docs chat warm-up skipped: {exc}"
                queue.put(warning)
                logger.warning("Docs chat warm-up skipped: %s", exc)
                _WARMUP_COMPLETED = False
            else:
                _WARMUP_COMPLETED = True
            finally:
                queue.put(_WARMUP_SENTINEL)
                with _WARMUP_GUARD:
                    if _WARMUP_THREAD is threading.current_thread():
                        _WARMUP_THREAD = None
        _WARMUP_MESSAGES = queue
        thread = threading.Thread(target=_run, name="dc43-docs-chat-warmup", daemon=True)
        _WARMUP_THREAD = thread
        thread.start()


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

    embedding_provider = _normalise_embedding_provider(config)
    if embedding_provider not in _SUPPORTED_EMBEDDING_PROVIDERS:
        supported = ", ".join(sorted(_SUPPORTED_EMBEDDING_PROVIDERS))
        return DocsChatStatus(
            enabled=True,
            ready=False,
            message=f"Unsupported docs chat embedding provider: {config.embedding_provider}. Supported values: {supported}.",
            ui_available=_check_ui_dependencies()[0],
        )

    core_ready, dependency_message = _check_core_dependencies(embedding_provider)
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


def describe_configuration() -> DocsChatConfigurationSummary:
    """Return a summary of the configured documentation assistant."""

    if not _CONFIG:
        raise DocsChatError(
            "Configure the documentation assistant before describing its settings."
        )

    docs_root = _resolve_docs_root(_CONFIG)
    sources = _resolve_content_sources(_CONFIG)
    code_paths = tuple(source.root for source in sources if source.kind == "code")
    index_dir = _resolve_index_dir(_CONFIG, _BASE_DIR)
    provider = _normalise_embedding_provider(_CONFIG)
    model = _resolve_embedding_model(_CONFIG)

    return DocsChatConfigurationSummary(
        docs_root=docs_root,
        code_paths=code_paths,
        index_dir=index_dir,
        embedding_provider=provider,
        embedding_model=model,
    )


def generate_reply(
    message: str,
    history: Sequence[Tuple[str, str]] | Sequence[Mapping[str, str]],
    progress: ProgressCallback | None = None,
) -> DocsChatReply:
    """Return an assistant response for ``message`` using ``history`` for context."""

    if not message.strip():
        raise DocsChatError("Provide a question so the assistant can look up matching documentation snippets.")

    steps: list[str] = []

    def _step(detail: str) -> None:
        steps.append(detail)
        _emit_progress(progress, detail)

    _step("🔄 Preparing the dc43 documentation assistant…")

    runtime = _ensure_runtime(progress=_step)
    chat_history = _normalise_history(history)
    _step("🔎 Retrieving the most relevant dc43 guides…")
    try:
        result = runtime.chain({"question": message, "chat_history": chat_history})
    except Exception as exc:  # pragma: no cover - defensive guard around provider errors
        raise DocsChatError(str(exc)) from exc

    _step("🧠 Generating an answer with cited documentation…")
    sources = _extract_sources(result, runtime)
    if not sources:
        answer_text = _OUT_OF_SCOPE_MESSAGE
    else:
        answer_text = _extract_answer_text(result)
    _step("✅ Response ready.")
    return DocsChatReply(answer=answer_text, sources=sources, steps=steps)


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

    def _respond(message: object, history: list[object]):
        try:
            from gradio import ChatMessage as GradioChatMessage  # type: ignore
        except ImportError:  # pragma: no cover - optional dependency guard
            GradioChatMessage = None  # type: ignore[assignment]

        prompt = _coerce_chat_prompt(message)
        normalised_history = _coerce_chat_history(history)

        progress_queue: "Queue[tuple[str, object | None]]" = Queue()
        progress_entries: list[str] = []

        def _progress(step: str) -> None:
            progress_queue.put(("progress", step))

        def _worker() -> None:
            try:
                reply = generate_reply(prompt, normalised_history, progress=_progress)
            except DocsChatError as exc:
                progress_queue.put(("error", str(exc)))
            except Exception as exc:  # pragma: no cover - defensive provider guard
                progress_queue.put(("error", str(exc)))
            else:
                progress_queue.put(("result", reply))
            finally:
                progress_queue.put(("done", None))

        threading.Thread(target=_worker, name="dc43-docs-chat-query", daemon=True).start()

        def _build_message(
            content: str,
            *,
            variant: str | None = None,
            title: str | None = None,
        ) -> object:
            metadata: dict[str, str] | None = None
            if variant or title:
                metadata = {}
                if variant:
                    metadata["variant"] = variant
                if title:
                    metadata["title"] = title
            if GradioChatMessage is not None:
                return GradioChatMessage(
                    role="assistant",
                    content=content,
                    metadata=metadata or None,
                )
            if metadata:
                return {"content": content, "metadata": metadata}
            return content

        while True:
            kind, payload = progress_queue.get()
            if kind == "progress":
                progress_entries.append(str(payload))
                step = progress_entries[-1]
                yield _render_progress_update(step, len(progress_entries))
                continue
            if kind == "error":
                yield _build_message(f"⚠️ {payload}", variant="secondary", title="Status update")
                continue
            if kind == "result":
                reply = payload
                if isinstance(reply, DocsChatReply):
                    final_markdown = reply.render_markdown(include_steps=False)
                else:  # pragma: no cover - defensive fallback
                    final_markdown = str(payload)

                if progress_entries:
                    summary_markdown = _build_progress_summary(progress_entries)
                    yield _build_message(
                        summary_markdown,
                        variant="secondary",
                        title="Processing log",
                    )

                yield _build_message(final_markdown)
                continue
            if kind == "done":
                break

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
        type="messages",
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


def _ensure_runtime(progress: ProgressCallback | None = None) -> _DocsChatRuntime:
    global _WARMUP_MESSAGES
    status_payload = status()
    if not status_payload.enabled:
        raise DocsChatError(status_payload.message or "Docs chat is disabled in the current configuration.")
    if not status_payload.ready:
        raise DocsChatError(status_payload.message or "Docs chat is not ready yet.")

    current_thread = threading.current_thread()
    while True:
        with _WARMUP_GUARD:
            warm_thread = _WARMUP_THREAD
            queue = _WARMUP_MESSAGES

        if not warm_thread or not warm_thread.is_alive() or warm_thread is current_thread:
            _consume_warmup_messages(queue, progress, block=False)
            break

        _emit_progress(progress, "⏳ Waiting for the documentation index warm-up to finish…")
        _consume_warmup_messages(queue, progress, block=True)
        warm_thread.join(timeout=0.2)

    with _WARMUP_GUARD:
        queue = _WARMUP_MESSAGES
        thread_stopped = not _WARMUP_THREAD or not _WARMUP_THREAD.is_alive()
        if thread_stopped and queue is not None:
            _WARMUP_MESSAGES = None

    _consume_warmup_messages(queue, progress, block=False)

    with _RUNTIME_LOCK:
        global _RUNTIME
        runtime = _RUNTIME
        if runtime is not None and _manifest_matches(runtime):
            _emit_progress(progress, "📦 Reusing cached documentation index.")
            return runtime

        runtime = _build_runtime(progress=progress)
        _RUNTIME = runtime
        return runtime


def _build_runtime(progress: ProgressCallback | None = None) -> _DocsChatRuntime:
    config = _CONFIG
    if config is None:
        raise DocsChatError("Docs chat has not been initialised.")

    content_sources = _resolve_content_sources(config)
    docs_root = content_sources[0].root
    index_dir = _resolve_index_dir(config, _BASE_DIR)
    index_dir.mkdir(parents=True, exist_ok=True)

    manifest = _current_manifest_payload(config, content_sources)
    manifest_path = index_dir / "manifest.json"
    if manifest_path.exists() and (index_dir / "index.faiss").exists():
        stored = _load_manifest(manifest_path)
        if stored == manifest:
            _emit_progress(progress, "📚 Loaded existing documentation manifest.")
            vectorstore = _load_vectorstore(index_dir, config)
            chain = _build_chain(config, vectorstore, progress=progress)
            return _DocsChatRuntime(
                config=config,
                docs_root=docs_root,
                index_dir=index_dir,
                manifest=manifest,
                chain=chain,
                embeddings_provider=_normalise_embedding_provider(config),
                embeddings_model=_resolve_embedding_model(config),
                content_sources=tuple(content_sources),
            )

    _emit_progress(progress, "📚 Indexing dc43 documentation and source code…")
    documents = _load_documents(content_sources, progress=progress)
    _emit_progress(progress, f"🧾 Loaded {len(documents)} documents from the configured sources.")
    vectorstore = _build_vectorstore(config, documents, progress=progress)
    _emit_progress(progress, "🗂️ Persisting the refreshed documentation index…")
    _save_vectorstore(index_dir, vectorstore)
    _write_manifest(manifest_path, manifest)
    chain = _build_chain(config, vectorstore, progress=progress)
    return _DocsChatRuntime(
        config=config,
        docs_root=docs_root,
        index_dir=index_dir,
        manifest=manifest,
        chain=chain,
        embeddings_provider=_normalise_embedding_provider(config),
        embeddings_model=_resolve_embedding_model(config),
        content_sources=tuple(content_sources),
    )


def _build_chain(
    config: DocsChatConfig,
    vectorstore: object,
    progress: ProgressCallback | None = None,
) -> object:
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
    _emit_progress(progress, "🤝 Connecting to the OpenAI chat model…")
    llm = ChatOpenAI(**llm_kwargs)
    _emit_progress(progress, "🔁 Preparing the retrieval chain…")
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
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    embeddings = _create_embeddings(config)
    return FAISS.load_local(
        str(index_dir),
        embeddings,
        allow_dangerous_deserialization=True,
    )


def _build_vectorstore(
    config: DocsChatConfig,
    documents: Sequence[object],
    progress: ProgressCallback | None = None,
) -> object:
    try:
        from langchain_community.vectorstores import FAISS
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)
    splits = splitter.split_documents(documents)

    embeddings = _create_embeddings(config)
    if not splits:
        raise DocsChatError(
            "No documentation content was loaded; confirm docs_chat paths point to Markdown or code."
        )

    total_batches = max(1, math.ceil(len(splits) / _EMBEDDING_BATCH_SIZE))
    _emit_progress(progress, f"🧠 Embedding {len(splits)} content chunks ({total_batches} batches)…")
    vectorstore = None
    for batch_index, start in enumerate(range(0, len(splits), _EMBEDDING_BATCH_SIZE), start=1):
        batch = splits[start : start + _EMBEDDING_BATCH_SIZE]
        if not batch:
            continue
        _emit_progress(progress, f"🧠 Embedding batch {batch_index}/{total_batches}…")
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


def _load_documents(
    content_sources: Sequence[_ContentSource],
    progress: ProgressCallback | None = None,
) -> Sequence[object]:
    try:
        from langchain_community.document_loaders import DirectoryLoader, TextLoader
    except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
        raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

    documents: list[object] = []

    for source in content_sources:
        if not source.root.exists():
            continue

        _emit_progress(progress, f"📥 Loading {source.kind} content from {source.root}")
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
        "embedding_provider": _normalise_embedding_provider(config),
        "embedding_model": _resolve_embedding_model(config),
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


def _resolve_index_dir(config: DocsChatConfig, base_dir: Path | None) -> Path:
    if config.index_path:
        return Path(config.index_path).expanduser()
    if base_dir is not None:
        return base_dir / "docs_chat" / "index"
    cache_root = Path.home() / ".dc43" / "docs_chat"
    return cache_root / "index"


def _missing_api_key_message(config: DocsChatConfig) -> str:
    env_name = config.api_key_env.strip() if config.api_key_env else ""
    if env_name:
        return (
            "Provide an API key via docs_chat.api_key or set the "
            f"{env_name} environment variable before retrying."
        )
    return "Provide an API key via docs_chat.api_key before retrying."


def _normalise_embedding_provider(config: DocsChatConfig) -> str:
    value = (config.embedding_provider or "huggingface").strip().lower()
    return value or "huggingface"


def _resolve_embedding_model(config: DocsChatConfig) -> str:
    provider = _normalise_embedding_provider(config)
    model = (config.embedding_model or "").strip()
    if provider != "openai" and (not model or model == "text-embedding-3-small"):
        return _DEFAULT_HUGGINGFACE_MODEL
    return model or "text-embedding-3-small"


def _resolve_api_key(config: DocsChatConfig) -> str | None:
    if config.api_key:
        return config.api_key.strip() or None
    if not config.api_key_env:
        return None
    value = os.getenv(config.api_key_env)
    if value:
        return value.strip() or None
    return None


def _create_embeddings(config: DocsChatConfig) -> object:
    provider = _normalise_embedding_provider(config)
    model_name = _resolve_embedding_model(config)

    if provider == "openai":
        try:
            from langchain_openai import OpenAIEmbeddings
        except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
            raise DocsChatError(_INSTALL_EXTRA_HINT) from exc

        api_key = _resolve_api_key(config)
        if not api_key:
            raise DocsChatError(_missing_api_key_message(config))

        return OpenAIEmbeddings(model=model_name, openai_api_key=api_key)

    if provider == "huggingface":
        try:
            from langchain_huggingface import HuggingFaceEmbeddings
        except ModuleNotFoundError as exc:  # pragma: no cover - safeguarded by ``status``
            raise DocsChatError(_INSTALL_HUGGINGFACE_HINT) from exc

        return HuggingFaceEmbeddings(model_name=model_name)

    raise DocsChatError(
        "Unsupported docs chat embedding provider: "
        f"{config.embedding_provider or provider}. Supported providers: "
        f"{', '.join(sorted(_SUPPORTED_EMBEDDING_PROVIDERS))}."
    )


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


def _check_core_dependencies(embedding_provider: str) -> tuple[bool, str | None]:
    try:
        import langchain  # noqa: F401
        import langchain_community  # noqa: F401
        import langchain_openai  # noqa: F401
        import langchain_text_splitters  # noqa: F401
    except ModuleNotFoundError:
        return (False, _INSTALL_EXTRA_HINT)
    if embedding_provider == "huggingface":
        try:
            import langchain_huggingface  # noqa: F401
            import sentence_transformers  # noqa: F401
        except ModuleNotFoundError:
            return (False, _INSTALL_HUGGINGFACE_HINT)
    return True, None


def _check_ui_dependencies() -> tuple[bool, str | None]:
    try:
        import gradio  # noqa: F401
    except ModuleNotFoundError:
        return (False, _INSTALL_GRADIO_HINT)
    return True, None


GRADIO_MOUNT_PATH = _GRADIO_MOUNT_PATH
