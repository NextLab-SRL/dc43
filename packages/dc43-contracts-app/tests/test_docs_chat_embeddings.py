from __future__ import annotations

import math
import sys
import types

from dc43_contracts_app import docs_chat
from dc43_contracts_app.config import DocsChatConfig


def test_build_vectorstore_batches_embedding_requests(monkeypatch):
    batches: list[list[str]] = []

    class _StubEmbeddings:
        def __init__(self, *args, **kwargs):
            self.calls = batches

        def embed_documents(self, texts):  # type: ignore[override]
            snapshot = [str(text) for text in texts]
            batches.append(snapshot)
            return [[0.0] for _ in texts]

    class _StubFAISS:
        def __init__(self):
            self._embeddings: _StubEmbeddings | None = None

        @classmethod
        def from_documents(cls, docs, embeddings):  # type: ignore[override]
            embeddings.embed_documents([doc.page_content for doc in docs])
            store = cls()
            store._embeddings = embeddings
            return store

        def add_documents(self, docs):  # type: ignore[override]
            assert self._embeddings is not None
            self._embeddings.embed_documents([doc.page_content for doc in docs])

    class _StubSplitter:
        def __init__(self, *args, **kwargs):
            pass

        def split_documents(self, docs):  # type: ignore[override]
            return docs

    fake_vectorstores = types.SimpleNamespace(FAISS=_StubFAISS)
    fake_embeddings = types.SimpleNamespace(OpenAIEmbeddings=_StubEmbeddings)
    fake_splitters = types.SimpleNamespace(RecursiveCharacterTextSplitter=_StubSplitter)

    monkeypatch.setitem(sys.modules, "langchain_community.vectorstores", fake_vectorstores)
    monkeypatch.setitem(sys.modules, "langchain_openai", fake_embeddings)
    monkeypatch.setitem(sys.modules, "langchain_text_splitters", fake_splitters)
    monkeypatch.setattr(docs_chat, "_resolve_api_key", lambda config: "token")

    document = types.SimpleNamespace
    total = docs_chat._EMBEDDING_BATCH_SIZE * 3 + 5
    documents = [document(page_content=f"doc {idx}", metadata={}) for idx in range(total)]

    store = docs_chat._build_vectorstore(DocsChatConfig(enabled=True), documents)  # type: ignore[attr-defined]
    assert isinstance(store, _StubFAISS)

    assert len(batches) == math.ceil(total / docs_chat._EMBEDDING_BATCH_SIZE)
    assert max(len(batch) for batch in batches) <= docs_chat._EMBEDDING_BATCH_SIZE
