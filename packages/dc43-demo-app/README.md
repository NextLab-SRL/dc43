# dc43-demo-app

A runnable FastAPI + Spark demonstration that stitches together the dc43
contracts portal, service backends, and Spark integrations into a single
end-to-end experience. It exposes the ``dc43-demo`` console entry point and
bundles fixtures for pipelines, validation scenarios, and static assets.

The launcher generates temporary configuration files in the workspace used by
``dc43_demo_app.contracts_workspace``. Pass a TOML path with ``--config`` to
merge personal overrides (for example a ``[docs_chat]`` block enabling the
documentation assistant) into the generated defaults. Secrets can live in a
lightweight ``.env`` file loaded via ``--env-file`` so the runner populates
``os.environ`` before spawning subprocesses:

```bash
dc43-demo --config $HOME/dc43/contracts-app.toml --env-file $HOME/dc43/docs-chat.env
```

The legacy ``DC43_CONTRACTS_APP_CONFIG=/path/to/file dc43-demo`` path still
works when you prefer exporting variables yourself. If the supplied file cannot
be parsed the launcher falls back to the default embedded configuration and
logs a warning.
