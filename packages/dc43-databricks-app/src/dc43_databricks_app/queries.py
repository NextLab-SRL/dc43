"""SQL against Databricks system tables.

Every function returns the raw SQL string so the app can also display it
(transparency is part of the demo value: this is the system-tables story).

Tables used:
  system.billing.usage + system.billing.list_prices  -> cost attribution
  system.access.table_lineage                        -> consumers & read activity
  system.information_schema.tables / columns / table_tags -> freshness, schema, tags
  system.lakeflow.job_run_timeline                   -> pipeline reliability
"""
from __future__ import annotations


def cost_by_workload(days: int = 30) -> str:
    return f"""
WITH usage AS (
  SELECT
    u.usage_date,
    u.sku_name,
    COALESCE(u.usage_metadata.job_id, u.usage_metadata.warehouse_id, 'other') AS workload_id,
    CASE
      WHEN u.usage_metadata.job_id IS NOT NULL THEN 'job'
      WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN 'warehouse'
      ELSE 'other'
    END AS workload_type,
    u.usage_quantity,
    u.usage_quantity * p.pricing.default AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  WHERE u.usage_date >= current_date() - INTERVAL {days} DAYS
)
SELECT workload_type, workload_id, sku_name,
       ROUND(SUM(cost_usd), 2) AS cost_usd_{days}d,
       ROUND(SUM(usage_quantity), 1) AS dbus
FROM usage
GROUP BY ALL
ORDER BY cost_usd_{days}d DESC
LIMIT 200
"""


def cost_daily(days: int = 30) -> str:
    return f"""
SELECT u.usage_date,
       ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= current_date() - INTERVAL {days} DAYS
GROUP BY u.usage_date
ORDER BY u.usage_date
"""


def table_activity(days: int = 30) -> str:
    """Reads + distinct consumers per table, from lineage events."""
    return f"""
SELECT
  CONCAT_WS('.', source_table_catalog, source_table_schema, source_table_name) AS table_fqn,
  COUNT(*) AS read_events_{days}d,
  COUNT(DISTINCT created_by) AS distinct_consumers,
  COUNT(DISTINCT COALESCE(entity_id, created_by)) AS distinct_entities,
  MAX(event_time) AS last_read_at
FROM system.access.table_lineage
WHERE event_time >= current_timestamp() - INTERVAL {days} DAYS
  AND source_table_name IS NOT NULL
GROUP BY ALL
ORDER BY read_events_{days}d DESC
LIMIT 500
"""


def downstream_consumers(days: int = 90) -> str:
    """How many distinct downstream targets each table feeds (blast radius)."""
    return f"""
SELECT
  CONCAT_WS('.', source_table_catalog, source_table_schema, source_table_name) AS table_fqn,
  COUNT(DISTINCT CONCAT_WS('.', target_table_catalog, target_table_schema, target_table_name))
    AS downstream_tables
FROM system.access.table_lineage
WHERE event_time >= current_timestamp() - INTERVAL {days} DAYS
  AND source_table_name IS NOT NULL
  AND target_table_name IS NOT NULL
GROUP BY ALL
"""


def table_freshness() -> str:
    return """
SELECT
  CONCAT_WS('.', table_catalog, table_schema, table_name) AS table_fqn,
  last_altered,
  TIMESTAMPDIFF(HOUR, last_altered, current_timestamp()) AS hours_since_update
FROM system.information_schema.tables
WHERE table_catalog NOT IN ('system', '__databricks_internal')
"""


def table_columns(catalog: str, schema: str, table: str) -> str:
    return f"""
SELECT column_name, data_type, is_nullable
FROM system.information_schema.columns
WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position
"""


def criticality_tags() -> str:
    return """
SELECT
  CONCAT_WS('.', catalog_name, schema_name, table_name) AS table_fqn,
  tag_value AS declared_criticality
FROM system.information_schema.table_tags
WHERE LOWER(tag_name) = 'criticality'
"""


def job_health(days: int = 30) -> str:
    return f"""
SELECT
  job_id,
  COUNT(*) AS runs,
  SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
  ROUND(100.0 * SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 1)
    AS success_rate_pct,
  MAX(period_end_time) AS last_run_at
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL {days} DAYS
  AND result_state IS NOT NULL
GROUP BY job_id
ORDER BY runs DESC
LIMIT 200
"""
