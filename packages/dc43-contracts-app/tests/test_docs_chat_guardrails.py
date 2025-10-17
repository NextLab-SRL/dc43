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

    starts: list[str] = []

    def _record(progress=None):
        starts.append("run")

    class _StubThread:
        def __init__(self, target, name=None, daemon=None):
            self._target = target
            self._started = False

        def start(self):
            self._started = True
            self._target()

        def is_alive(self):
            return self._started

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    monkeypatch.setattr(docs_chat, "_ensure_runtime", lambda progress=None: _record(progress))
    monkeypatch.setattr(docs_chat, "_WARMUP_THREAD", None)
    monkeypatch.setattr(docs_chat, "threading", threading)
    monkeypatch.setattr(threading, "Thread", _StubThread)

    docs_chat.warm_up()
    docs_chat.warm_up()

    assert starts == ["run"]
