# Configuring `dc43-service-backends`

This guide explains how to configure the dc43 service backend HTTP application
using TOML files. The package ships with sensible defaults, but production
installations typically need to point the application at a contract store and
optionally secure the API with a bearer token.

## Configuration loading overview

`dc43_service_backends.config.load_config()` reads configuration from multiple
locations, applying the first one that exists:

1. A path that you explicitly pass to `load_config(path)`.
2. The location provided in the `DC43_SERVICE_BACKENDS_CONFIG` environment
   variable.
3. The packaged default located at
   `dc43_service_backends/config/default.toml`.

Each configuration file is parsed as TOML. Any invalid or unreadable file is
silently ignored and the defaults are used instead.

Environment variables can override values loaded from TOML:

* `DC43_CONTRACT_STORE` overrides the contract store directory.
* `DC43_BACKEND_TOKEN` overrides the bearer token.

## TOML schema

A configuration file is composed of two optional tables: `contract_store` and
`auth`.

```toml
[contract_store]
root = "./contracts"

[auth]
token = "change-me"
```

### Contract store

The `contract_store` table controls where the backend persists contracts on the
filesystem.

| Key  | Type | Description |
| ---- | ---- | ----------- |
| `root` | string | Absolute or relative path to the directory that stores contracts. When omitted or blank, the application defaults to a `contracts` directory under the current working directory. The path may include `~` to reference the current user's home directory. |

### Authentication

The `auth` table lets you require clients to supply a bearer token with each
request.

| Key  | Type | Description |
| ---- | ---- | ----------- |
| `token` | string | Optional token value compared against the `Authorization: Bearer <token>` header. Set to an empty string to disable token authentication, which is useful for local development. |

## Template

Copy `docs/templates/dc43-service-backends.toml` as a starting point. Update the
paths and secrets to match your environment, then point the application at the
new file by setting the `DC43_SERVICE_BACKENDS_CONFIG` environment variable or
by passing the path to `load_config()` directly.
