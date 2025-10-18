# Getting started with dc43

Pick the guide that matches your role and the environment you have access to. Each walkthrough links back to component reference
material when you want to dive deeper.

## Scenario index

- [Operations: publish shared service backends](ops-service-backend.md)
- [Spark developers: run dc43 fully locally](spark-local.md)
- [Spark developers: consume shared dc43 services](spark-remote.md)
- [Spark developers: generate pipeline stubs with the contracts app](spark-contract-app-helper.md)
- [Contracts app: automate the setup wizard with Playwright scenarios](../tutorials/contracts-setup-automation.md)
- [Databricks teams: integrate dc43 with Unity Catalog](databricks.md)

The guides assume you have a working Python 3.11 environment. When you work from a source checkout run `pip install -e .` from the
repository root so editable installs pick up sibling packages.

## Documentation assistant

The dc43 app can expose a docs-first chat experience using LangChain and Gradio. Install the optional
dependencies and enable the feature in your configuration:

```bash
pip install --no-cache-dir -e ".[demo]"

# Already installed the demo extras? you do not need a second
# `dc43-contracts-app[docs-chat]` install – the meta package pulls the
# assistant dependencies automatically.

cat <<'TOML' > ~/.dc43/contracts-app.toml
[docs_chat]
enabled = true
provider = "openai"
model = "gpt-4o-mini"
embedding_provider = "huggingface"  # Opt into "openai" if you prefer remote embeddings.
embedding_model = ""  # Defaults to sentence-transformers/all-MiniLM-L6-v2 when blank.
api_key_env = "OPENAI_API_KEY" # rename when you prefer a different env var
# Keep secrets out of git-tracked files? add them to a private env file.
# Prefer storing the key alongside the config? set `api_key = "sk-..."` instead.
# Want to expand the knowledge base beyond the repository `src/` and `packages/`
# directories? Populate `code_paths` with extra folders (for example a mono-repo
# integration module).
code_paths = []
# Enabling OpenAI reasoning models such as `o4-mini`? set `reasoning_effort = "medium"` (or `"high"`).
reasoning_effort = ""
TOML

# Optionally capture secrets in a lightweight env file so you can avoid manual
# `export` commands.
cat <<'ENV' > ~/.dc43/docs-chat.env
OPENAI_API_KEY=sk-your-api-key
ENV

# Launch the demo. The runner copies your docs_chat overrides into its generated
# configuration so the workspace/backend defaults stay intact and loads the env
# file before spawning subprocesses.
dc43-demo --config ~/.dc43/contracts-app.toml --env-file ~/.dc43/docs-chat.env

# Optional: pre-build the documentation index (reuses the same configuration and
# stores the FAISS cache under the configured workspace).
dc43-docs-chat-index --config ~/.dc43/contracts-app.toml --workspace-root ~/dc43/workspace
# The CLI now prints a summary of the docs/code roots and embedding backend on
# success and exits with a clear error (for example "missing API key") when the
# assistant is not ready to warm up.
```

The launcher still honours `DC43_CONTRACTS_APP_CONFIG` if you prefer a global
environment variable. Restart the application and open `/docs-chat` to chat with
the Markdown guides bundled in `docs/`.

Expect detailed, citation-backed answers. The assistant now grounds its replies
in the retrieved Markdown and code snippets so prompts like “help me start a Spark
integration pipeline” return step-by-step guidance, highlighted file names from
`docs/` and `src/`, and links to the most relevant setup sections.

The chat surface is intentionally scoped to dc43. When a request is unrelated to
the platform (for example, a generic question about weather forecasts or an
external API), the assistant politely declines and reminds the user to focus on
dc43 tasks. This keeps the experience centred on the project documentation and
avoids suggesting unsupported workflows.

By default the assistant indexes the `docs/`, `src/`, and `packages/` trees from
your dc43 checkout. Paths outside the repository are ignored automatically so a
workspace rooted in `~/src` no longer triggers a scan of every sibling project.
Set `docs_chat.code_paths` when you want to add additional modules (for example
a sibling integration repo) or trim the scope to specific directories.
Teams experimenting with OpenAI's reasoning models can opt into higher-depth
answers by switching `docs_chat.model` to `o4-mini` and providing a
`docs_chat.reasoning_effort` value (`"medium"` or `"high"` depending on
latency/cost trade-offs).

Large documentation or source trees are embedded in small batches during the
initial index build so the default Hugging Face workflow can run locally
without exhausting OpenAI's 300k-token request limit. If you would rather reuse
OpenAI embeddings, set `embedding_provider` to `openai` and choose an
appropriate `embedding_model`. The docs-chat extra already installs
`langchain-huggingface` and `sentence-transformers`, and the assistant falls
back to `sentence-transformers/all-MiniLM-L6-v2` unless you provide a custom
`embedding_model`. Run `dc43-docs-chat-index` after tweaking your configuration
to persist the updated FAISS cache before redeploying the app.

The demo runner now kicks off the knowledge base warm-up as part of its startup
flow while the FastAPI apps boot. Downloading model metadata, scanning
Markdown, and persisting the FAISS index happens in the background so the
services come online quickly. If a chat request lands before the warm-up
completes, the UI streams the queued warm-up steps (including the initial
“waiting for warm-up” status) until the cache is ready and then continues with
retrieval, and the same progress shows up in the server logs for debugging.
Subsequent restarts reuse the cached index unless the docs or code change.

While a response is being generated the chat UI streams succinct progress
updates (for example “embedding documentation” or “querying OpenAI”) so users
know which stage of the retrieval-and-generation pipeline is running without a
wall of status text pushing the answer out of view. Once the response is ready,
the assistant posts a collapsible **Processing log** message with the recorded
steps and then replies in its own chat bubble so the final answer stays visible
at the bottom of the conversation.

> ℹ️ You can skip the env file when `docs_chat.api_key` stores the secret: run
> `dc43-demo --config ~/.dc43/contracts-app.toml` and the launcher will merge your
> overrides automatically.

> 💡 Paste a token into `docs_chat.api_key_env` by mistake? The loader now treats
> values that do not look like environment variable names (for example strings
> containing `-` characters such as `sk-...`) as inline secrets, so the assistant
> still starts. Update your config to use the dedicated `api_key` field for clarity.

> ⚠️ pip treats `pip install --no-cache-dir -e ".[demo]"` and a follow-up
> `pip install "dc43-contracts-app[docs-chat]"` as competing requirements when
> they point at the same checkout. Pick the single command that matches your
> environment to avoid the resolution error shown above.

Prefer a guided experience? The contracts setup wizard now includes a **Documentation assistant**
module under the *User experience* group. Selecting the Gradio assistant option captures the same
`[docs_chat]` settings (provider, models, key environment variable, and optional path overrides) so
the exported bundle and configuration downloads are ready to deploy without manual edits.
