"""Table name derivation helpers shared across governance stores."""

from __future__ import annotations


def derive_related_table_name(table: str, suffix: str) -> str:
    """Create a deterministic companion table name sharing ``table``'s scope."""

    prefix: str | None = None
    name = table
    if "." in table:
        prefix, name = table.rsplit(".", 1)

    derived = derive_related_table_basename(name, suffix)
    if prefix:
        return f"{prefix}.{derived}"
    return derived


def derive_related_table_basename(name: str, suffix: str) -> str:
    """Return the derived table name without catalog/schema prefixes."""

    if suffix == "metrics":
        lowered = name.lower()
        suffix_mappings = {
            "_status": "_metrics",
        }
        for status_suffix, metrics_suffix in suffix_mappings.items():
            if lowered.endswith(status_suffix):
                return name[: -len(status_suffix)] + metrics_suffix
    return f"{name}_{suffix}"
