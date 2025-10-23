from queue import Queue
from types import SimpleNamespace

import threading

from dc43_contracts_app import docs_chat


def test_generate_reply_returns_guardrail_when_no_sources(monkeypatch):
    runtime = SimpleNamespace(
        chain=lambda payload: {"answer": "Here is something", "source_documents": []},
        content_sources=(),
    )
    monkeypatch.setattr(docs_chat, "_ensure_runtime", lambda progress=None: runtime)

    reply = docs_chat.generate_reply("What's the weather?", [])

    assert "dc43" in reply.answer.lower()
    assert reply.sources == []


def test_warm_up_blocking_invokes_runtime(monkeypatch):
    calls: list[object] = []

    class _Status:
        enabled = True
        ready = True
        message = None

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    monkeypatch.setattr(docs_chat, "_ensure_runtime", lambda progress=None: calls.append(progress))

    docs_chat.warm_up(block=True)

    assert len(calls) == 1


def test_warm_up_async_runs_once(monkeypatch):
    class _Status:
        enabled = True
        ready = True
        message = None

    calls: list[str] = []
    completed = threading.Event()

    def _record(progress=None):
        calls.append("run")
        completed.set()

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    monkeypatch.setattr(docs_chat, "_ensure_runtime", lambda progress=None: _record(progress))
    monkeypatch.setattr(docs_chat, "_WARMUP_THREAD", None, raising=False)
    monkeypatch.setattr(docs_chat, "_WARMUP_MESSAGES", None, raising=False)

    docs_chat.warm_up()
    assert completed.wait(timeout=1.0)

    completed.clear()
    docs_chat.warm_up()

    assert not completed.wait(timeout=0.2)
    assert calls == ["run"]


def test_ensure_runtime_reports_warmup_wait(monkeypatch):
    sentinel = object()

    class _Status:
        enabled = True
        ready = True
        message = None

    def _record(message):
        messages.append(message)

    messages: list[str] = []

    queue: Queue[object] = Queue()
    queue.put("🔧 warm-up progress message")
    queue.put(docs_chat._WARMUP_SENTINEL)

    class _WarmThread:
        def __init__(self):
            self._alive = True

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            self._alive = False

    warm_thread = _WarmThread()

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    monkeypatch.setattr(docs_chat, "_manifest_matches", lambda runtime: True)
    monkeypatch.setattr(docs_chat, "_RUNTIME", sentinel, raising=False)
    monkeypatch.setattr(docs_chat, "_WARMUP_THREAD", warm_thread, raising=False)
    monkeypatch.setattr(docs_chat, "_WARMUP_MESSAGES", queue, raising=False)

    runtime = docs_chat._ensure_runtime(progress=_record)

    assert runtime is sentinel
    assert any("warm-up" in message for message in messages)
