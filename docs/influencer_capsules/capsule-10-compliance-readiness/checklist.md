# Compliance Readiness Checklist Overview

Use this checklist to prepare your organization for audits using DC43. The full
spreadsheet lives at `docs/compliance/compliance-readiness-checklist.xlsx`.

| Control | Framework Mapping | Owner | Automation Coverage | Evidence Source | Review Cadence |
|---------|------------------|-------|---------------------|-----------------|----------------|
| Contract approvals versioned | SOC 2 CC7.2, ISO A.12.1.2 | Governance Lead | 100% (DC43 workflow) | DC43 manifest bundle | Each contract merge |
| Policy change notifications | SOC 2 CC6.1 | Data Platform PM | 80% (DC43 webhooks) | Slack export + manifest diff | Weekly |
| Runtime monitoring | SOC 2 CC7.3, ISO A.12.4.1 | SRE Manager | 70% (DC43 metrics + Grafana) | Observability dashboard PDF | Daily |
| Quarterly attestations | SOC 2 CC2.1 | Compliance Manager | 60% (workflow template) | Attestation PDF + DC43 log | Quarterly |
| Data retention enforcement | GDPR Art.5, ISO A.18.1.3 | Legal Ops | 50% (scripts) | Retention script output + review notes | Monthly |

## How to Use
1. Import the spreadsheet into your GRC tool or share with stakeholders.
2. Update owner names and automation coverage percentages to reflect your org.
3. Attach evidence bundle output to each control for audit traceability.
4. Review the checklist during monthly compliance syncs.
