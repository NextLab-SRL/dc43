# dc43-contracts-app

A FastAPI application that surfaces the dc43 governance experience. It relies on
shared service clients to interact with contract, governance, and data product
backends, and bundles HTML templates plus static assets for local demos and
packaged deployments.

## Features

- Browse and edit contracts, datasets, and data products backed by any dc43
  service implementation.
- Inspect governance metrics alongside dataset and contract records with
  interactive trend charts so operators can review historical observations
  without leaving the UI.
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

   # Working from a source checkout? the root demo extra already pulls the
   # assistant stack – run `pip install --no-cache-dir -e ".[demo]"` once and
   # skip additional `dc43-contracts-app[docs-chat]` installs to avoid pip
   # dependency conflicts.
   ```
   Mixing both commands in the same environment (for example running the
   editable install and then invoking the PyPI extra) causes pip to report
   conflicting requirements because they reference the same local package.
2. Provide an API key via the configured environment variable (defaults to
   `OPENAI_API_KEY`) or set an inline secret with `docs_chat.api_key` in a
   private TOML file. When you rely on `embedding_provider = "huggingface"` the
   same OpenAI key is still used for chat completions, but embeddings no longer
   require an external service.
3. Toggle the feature in `contracts-app.toml`:
   ```toml
   [docs_chat]
   enabled = true
   provider = "openai"
   model = "gpt-4o-mini"
   embedding_provider = "huggingface" # Opt into "openai" to reuse hosted embeddings.
   embedding_model = "" # Defaults to sentence-transformers/all-MiniLM-L6-v2.
   api_key_env = "OPENAI_API_KEY"
   # api_key = "sk-your-api-key" # optional inline secret stored outside git
   # code_paths = ["~/project/sibling-module"] # optional extra source directories
   # reasoning_effort = "medium" # use with OpenAI `o4-*` models
   ```
   `api_key_env` records the *name* of the variable that stores your API key.
   Load the secret separately (for example via `direnv`, `dotenv`, a
   `.env` file passed to `dc43-demo --env-file`, or a shell
   `export OPENAI_API_KEY=...`).
4. Pass the config path to the demo launcher so the loader picks up your
   changes:
   ```bash
   dc43-demo --config /path/to/contracts-app.toml
   ```
   The legacy `export DC43_CONTRACTS_APP_CONFIG=/path/to/contracts-app.toml`
   workflow still works when you prefer a global environment variable.
5. Restart the dc43 app. The assistant indexes Markdown under `docs/` and the
   source trees in `src/`/`packages/` from your dc43 checkout by default and
   ignores parent directories (for example `~/src`). Override
   `docs_chat.docs_path`, `docs_chat.code_paths`, or `docs_chat.index_path` when
   the repository lives elsewhere.

   Embeddings are requested in small batches during the initial index build so
   the default Hugging Face workflow runs locally without tripping OpenAI's
   token limits. Point the assistant at large documentation or source
   directories without juggling manual chunk sizes. Prefer managed embeddings?
   set `embedding_provider = "openai"` and specify a compatible
   `embedding_model`. The docs-chat extra already installs
   `langchain-huggingface` and `sentence-transformers`, so leaving
   `embedding_model` empty keeps the `sentence-transformers/all-MiniLM-L6-v2`
   default.

   The FastAPI application now kicks off the documentation index warm-up as it
   loads the configuration so the one-off downloads and FAISS persistence happen
   in the background while the UI comes online. Cached manifests are reused
   across restarts until the docs or code change, and if a prompt arrives mid
   warm-up the chat surface explains that it is waiting for the cached index to
   finish building before continuing.

   While users wait for an answer the chat UI streams progress updates—loading
   documentation, embedding batches, and querying OpenAI—before presenting the
   final, cited response. Programmatic callers receive the same step list in the
   JSON payload under a new `steps` field.

   For deployments that want the index ready before the app starts, run
   `dc43-docs-chat-index --config /path/to/contracts-app.toml` as part of your
   build pipeline. The CLI shares the same configuration loader and writes the
   FAISS cache to `docs_chat.index_path` (or the workspace default) so runtime
   warm-ups immediately reuse the persisted manifest.

The contracts setup wizard mirrors these settings via the **Documentation assistant** module. Pick
the Gradio option under the *User experience* group to populate `[docs_chat]` in the exported
`dc43-contracts-app.toml` and surface the assistant alongside other deployment assets.

The assistant is purposefully constrained to dc43 setup and usage. When a prompt
strays outside that scope the response reminds the requester to come back with a
dc43-specific question so the chat surface stays focused on project guidance.

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
| `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER` | Embedding backend used to build the FAISS index (`openai` or `huggingface`). |
| `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL` | Embedding model used to build the vector index. Leave empty when relying on the default Hugging Face model. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV` | Name of the environment variable that stores the provider API key. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY` | Inline provider API key used when you prefer not to rely on environment variables. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_PATH` | Override the directory that contains Markdown documentation. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_INDEX` | Directory where the LangChain/FAISS index is stored. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS` | Comma/`:` separated list of extra source directories to index. |
| `DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT` | Reasoning hint (`low`/`medium`/`high`) for OpenAI `o4`/`o1` models. |

Combine these overrides with existing workspace and backend settings to tailor
the dc43 app to your deployment environment.
