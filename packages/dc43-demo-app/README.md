# dc43-demo-app

A runnable FastAPI + Spark demonstration that stitches together the dc43
contracts portal, service backends, and Spark integrations into a single
end-to-end experience. It exposes the ``dc43-demo`` console entry point and
bundles fixtures for pipelines, validation scenarios, and static assets.

The launcher generates temporary configuration files in the workspace used by
``dc43_demo_app.contracts_workspace``. When ``DC43_CONTRACTS_APP_CONFIG`` points
at a TOML file, the runner now merges that configuration (for example a
``[docs_chat]`` block enabling the documentation assistant) into the generated
defaults instead of overwriting it. Export the variable before starting the demo
or prefix the command directly:

```bash
export DC43_CONTRACTS_APP_CONFIG=$HOME/dc43/contracts-app.toml
dc43-demo

# or
DC43_CONTRACTS_APP_CONFIG=$HOME/dc43/contracts-app.toml dc43-demo
```

If the supplied file cannot be parsed the launcher falls back to the default
embedded configuration and logs a warning.
