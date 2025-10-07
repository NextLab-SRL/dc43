# Retail Promotion Runbook

## 1. Contract Preparation
1. Clone base contract:
   ```bash
   python scripts/contracts/clone.py \
     --source docs/demo-contracts/retail-promo.yaml \
     --target artifacts/retail-promo-b2s.yaml \
     --override promo.name="Back-to-School Boost" \
     --override validity.end=2024-09-15
   ```
2. Review ownership metadata; update stakeholder emails.
3. Commit contract to Git repo for traceability.

## 2. Governance Workflow
1. Submit contract for approval:
   ```bash
   python scripts/workflows/submit.py \
     --file artifacts/retail-promo-b2s.yaml \
     --workflow retail-approvals
   ```
2. Monitor approvals in Slack `#retail-governance` (mock). Capture screenshots.
3. Once signed, download manifest artifact from workflow output folder.

## 3. Data Activation
1. Run Delta adapter to materialize feature table:
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/adapters/delta.py \
     --contract artifacts/retail-promo-b2s.yaml \
     --output-table retail.feature_promotions
   ```
2. Start streaming agent for real-time updates:
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/agents/streaming.py \
     --config docs/demo-contracts/realtime-retail-agent.yaml \
     --promo-config artifacts/retail-promo-b2s.yaml \
     --rate 400
   ```
3. Validate data quality using Great Expectations checkpoint `retail_promo_ge.yml`.

## 4. Reporting
1. Import dashboard JSON `docs/dashboards/retail-promo.json` into Grafana.
2. Update parameters for promo name + date range.
3. Export dashboard PNG for marketing review.

## 5. Tear Down
1. Stop streaming agent and Docker services.
2. Archive logs + manifests in `artifacts/retail-demo/`.
3. Reset datasets using `python scripts/bootstrap_demo_data.py --scenario retail-promo --reset`.

## 6. FAQ Prep
- Pricing questions → reference contract discount rules.
- Data quality concerns → share Great Expectations results.
- Timeline → highlight entire flow completes in under 60 minutes.
