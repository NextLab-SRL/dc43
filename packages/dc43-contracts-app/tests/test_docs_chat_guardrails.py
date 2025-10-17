from types import SimpleNamespace

from dc43_contracts_app import docs_chat


def test_generate_reply_returns_guardrail_when_no_sources(monkeypatch):
    runtime = SimpleNamespace(
        chain=lambda payload: {"answer": "Here is something", "source_documents": []},
        content_sources=(),
    )
    monkeypatch.setattr(docs_chat, "_ensure_runtime", lambda: runtime)

    reply = docs_chat.generate_reply("What's the weather?", [])

    assert "dc43" in reply.answer.lower()
    assert reply.sources == []
