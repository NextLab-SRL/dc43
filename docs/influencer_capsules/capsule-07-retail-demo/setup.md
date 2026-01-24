# Setup – Capsule 07 "Retail Demo"

## Objective
Run the end-to-end DC43 retail promotion demo: contracts, workflows, Delta
adapter, streaming agent, and dashboards.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Java 11+
   - Docker Desktop (for optional services)
   - Node.js 18+, `npm install --global reveal-md`
2. **Python dependencies**
   ```bash
   pip install dc43 pyspark==3.5.1 delta-spark==3.1.0 great-expectations fastapi uvicorn rich typer
   ```
3. **Optional front-end**
   - Install `npm install` within `packages/dc43-demo-app/ui` if you want web UI
     overlays.

## Preparation Steps
1. Clone repo and bootstrap retail demo assets:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   python scripts/bootstrap_demo_data.py --scenario retail-promo
   ```
2. Start supporting services (optional but recommended):
   ```bash
   docker compose -f deploy/compose/retail-demo.yml up -d
   ```
   Services: mock contract store, Kafka, Grafana, Postgres for campaign results.
3. Publish contract and trigger workflow:
   ```bash
   python scripts/contracts/clone.py \
     --source docs/demo-contracts/retail-promo.yaml \
     --target artifacts/retail-promo-b2s.yaml \
     --override validity.end=2024-09-15

   python scripts/workflows/submit.py \
     --file artifacts/retail-promo-b2s.yaml \
     --workflow retail-approvals
   ```
4. Launch adapters:
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/adapters/delta.py \
     --contract artifacts/retail-promo-b2s.yaml

   python packages/dc43-demo-app/src/dc43_demo_app/agents/streaming.py \
     --config docs/demo-contracts/realtime-retail-agent.yaml \
     --promo-config artifacts/retail-promo-b2s.yaml
   ```
5. Refresh marketing dashboard (Metabase or Grafana) using `docs/dashboards/retail-promo.json`.
6. Generate Reveal.js slides:
   ```bash
   cd docs/influencer_capsules/capsule-07-retail-demo
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Recording Checklist
- Capture contract editing in VS Code with large font.
- Record workflow approval notifications (use mock Slack channel).
- Screen record dashboard showing store segmentation map.
- Save CLI output logs to `artifacts/retail-demo-run.log`.

## Troubleshooting
- If streaming agent lags, reduce demo rate: add `--rate 200` flag.
- Use `--dry-run` on adapters for quick retakes without reloading data.
