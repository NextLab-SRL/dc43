# Setup – Capsule 06 "Observability Playbook"

## Objective
Reproduce the observability stack—Prometheus, Grafana, and alert webhooks—so the
video can showcase live dashboards and alerting behavior.

## Prerequisites
1. **Local tooling**
   - Docker Desktop or Docker Engine
   - Python 3.10+
   - Node.js 18+ (`npm install --global reveal-md`)
2. **Python dependencies**
   ```bash
   pip install dc43 rich typer slack-sdk
   ```
3. **Assets**
   - Grafana dashboard JSON `docs/dashboards/dc43-observability.json`
   - Slack webhook mock script `scripts/alerts/mock_slack.py`

## Environment Prep
1. Clone repo and start observability stack:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   docker compose -f deploy/compose/observability-demo.yml up -d
   ```
2. Seed metrics by running streaming agent in demo mode:
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/agents/streaming.py \
     --config docs/demo-contracts/realtime-retail-agent.yaml \
     --emit-metrics --use-mock-source
   ```
3. Load Grafana dashboard:
   - Browse to http://localhost:3000 (admin/admin default).
   - Import JSON `docs/dashboards/dc43-observability.json`.
4. Configure Slack mock receiver (optional real webhook):
   ```bash
   python scripts/alerts/mock_slack.py --port 5050
   export DC43_ALERT_WEBHOOK=http://localhost:5050/webhook
   ```
5. Generate Reveal.js slide assets:
   ```bash
   cd docs/influencer_capsules/capsule-06-observability
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Recording Checklist
- Screen record Grafana dashboard once metrics populate (approx. 45s).
- Trigger alert by pausing agent:
   ```bash
   pkill -f streaming.py # wait for alert, then restart
   ```
- Capture Slack mock console output for overlay.

## Troubleshooting
- If Grafana fails to connect, ensure Docker ports 3000, 9090 are free.
- Restart agent with `--log-level debug` if metrics don't appear.
