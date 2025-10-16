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

export OPENAI_API_KEY="sk-your-api-key"

cat <<'TOML' > ~/dc43/contracts-app.toml
[docs_chat]
enabled = true
provider = "openai"
model = "gpt-4o-mini"
embedding_model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
TOML
```

Mount the config via `DC43_CONTRACTS_APP_CONFIG` or copy the snippet into your existing TOML file.
Restart the application and open `/docs-chat` to chat with the Markdown guides bundled in `docs/`.

> ⚠️ pip treats `pip install --no-cache-dir -e ".[demo]"` and a follow-up
> `pip install "dc43-contracts-app[docs-chat]"` as competing requirements when
> they point at the same checkout. Pick the single command that matches your
> environment to avoid the resolution error shown above.

Prefer a guided experience? The contracts setup wizard now includes a **Documentation assistant**
module under the *User experience* group. Selecting the Gradio assistant option captures the same
`[docs_chat]` settings (provider, models, key environment variable, and optional path overrides) so
the exported bundle and configuration downloads are ready to deploy without manual edits.
