from __future__ import annotations

from fastapi.testclient import TestClient

from dc43_contracts_app import docs_chat, server


def test_docs_chat_api_disabled():
    with TestClient(server.app) as client:
        response = client.post("/api/docs-chat/messages", json={"message": "hello"})
    assert response.status_code == 400
    assert "docs_chat" in response.json()["detail"].lower()


def test_docs_chat_api_not_ready(monkeypatch):
    class _Status:
        enabled = True
        ready = False
        message = "Missing API key"
        ui_available = False

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    with TestClient(server.app) as client:
        response = client.post("/api/docs-chat/messages", json={"message": "help"})
    assert response.status_code == 400
    assert "api key" in response.json()["detail"].lower()


def test_docs_chat_api_success(monkeypatch):
    class _Status:
        enabled = True
        ready = True
        message = None
        ui_available = True

    monkeypatch.setattr(docs_chat, "status", lambda: _Status())
    monkeypatch.setattr(
        docs_chat,
        "generate_reply",
        lambda message, history, progress=None: docs_chat.DocsChatReply(
            answer="Hello there", sources=["guide.md"], steps=["step one", "step two"]
        ),
    )

    with TestClient(server.app) as client:
        response = client.post(
            "/api/docs-chat/messages",
            json={"message": "hello", "history": [["human", "assistant"]]},
        )
    assert response.status_code == 200
    assert response.json() == {
        "message": "Hello there",
        "sources": ["guide.md"],
        "steps": ["step one", "step two"],
    }


def test_docs_chat_view_instructions():
    with TestClient(server.app) as client:
        response = client.get("/docs-chat")
    assert response.status_code == 200
    assert "documentation assistant is currently disabled" in response.text
