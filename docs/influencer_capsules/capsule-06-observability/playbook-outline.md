# Observability Playbook Outline

## 1. Instrumentation
- **Agents**: Enable metrics export with `--emit-metrics` flag.
- **Prometheus**: Scrape endpoint `http://<agent-host>:9105/metrics` every 15s.
- **Metric families**:
  - `dc43_contract_total`
  - `dc43_contract_freshness_seconds`
  - `dc43_policy_breach_total`
  - `dc43_runtime_lag_seconds`

## 2. Dashboards
- **Overview board** (Grafana)
  - Contracts by lifecycle stage
  - SLA compliance gauge
  - Policy breach heatmap
  - Runtime lag histogram
- **Drilldown board**
  - Contract detail with annotation timeline
  - Data product owner contact card
  - Recent schema change diff embed

## 3. Alerts & Automation
- **Thresholds**
  - Freshness > 900s → warning
  - Policy breach total > 0 in 5m window → critical
  - Runtime lag > 120s → warning
- **Channels**
  - Slack webhook (default)
  - Microsoft Teams connector (optional)
  - PagerDuty (bonus, via webhook template)
- **Playbook actions**
  - Auto-create ticket in Jira via `scripts/alerts/create_ticket.py`
  - Trigger runbook link to `docs/runbooks/streaming-lag.md`

## 4. Enablement
- Conduct 30-minute observability review with data product teams.
- Provide cheat sheet `docs/quickstart/observability-cheatsheet.pdf`.
- Encourage community submissions via forum thread #observability-gallery.
