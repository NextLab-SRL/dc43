# dc43-contracts-app

A FastAPI application that surfaces the dc43 governance experience. It relies on
shared service clients to interact with contract, governance, and data product
backends, and bundles HTML templates plus static assets for local demos and
packaged deployments.

## Features

- Browse and edit contracts, datasets, and data products backed by any dc43
  service implementation.
- Export integration helper bundles to bootstrap Spark or Delta pipelines.
- Embed a documentation-driven chat assistant powered by LangChain and Gradio so
  teams can query the Markdown guides that ship with dc43.

## Documentation chat assistant

The docs chat surface reuses off-the-shelf components—LangChain for retrieval
augmented generation and Gradio for the UI—so the repository does not have to
maintain bespoke chat widgets. To enable it:

1. Install the optional extra:
   ```bash
   pip install "dc43-contracts-app[docs-chat]"
   ```
2. Provide an API key via the configured environment variable (defaults to
   `OPENAI_API_KEY`).
3. Toggle the feature in `contracts-app.toml`:
   ```toml
   [docs_chat]
   enabled = true
   provider = "openai"
   model = "gpt-4o-mini"
   embedding_model = "text-embedding-3-small"
   api_key_env = "OPENAI_API_KEY"
   ```
4. Restart the dc43 app. The assistant indexes Markdown under `docs/` by
   default; override `docs_chat.docs_path` or `docs_chat.index_path` when the
   repository lives elsewhere.

The contracts setup wizard mirrors these settings via the **Documentation assistant** module. Pick
the Gradio option under the *User experience* group to populate `[docs_chat]` in the exported
`dc43-contracts-app.toml` and surface the assistant alongside other deployment assets.

Programmatic callers can POST to `/api/docs-chat/messages` with a JSON payload
(`{"message": "...", "history": [...]}`) and receive answers plus cited
sources. The embedded Gradio UI is mounted at `/docs-chat/assistant` and the
HTML entry point lives at `/docs-chat`.

## Environment variables

| Variable | Purpose |
| --- | --- |
| `DC43_CONTRACTS_APP_BACKEND_URL` | Remote backend URL when not running in embedded mode. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED` | Override the `docs_chat.enabled` flag (`1`, `true`, etc.). |
| `DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER` | Provider identifier (currently `openai`). |
| `DC43_CONTRACTS_APP_DOCS_CHAT_MODEL` | Chat completion model to request. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL` | Embedding model used to build the vector index. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV` | Name of the environment variable that stores the provider API key. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_PATH` | Override the directory that contains Markdown documentation. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_INDEX` | Directory where the LangChain/FAISS index is stored. |

Combine these overrides with existing workspace and backend settings to tailor
the dc43 app to your deployment environment.
