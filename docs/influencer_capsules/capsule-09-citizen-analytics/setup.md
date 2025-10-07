# Setup – Capsule 09 "Citizen Analytics"

## Objective
Show the citizen analytics starter kit: contract portal, access workflow,
notebook template, and dashboard publication.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Node.js 18+ (`npm install --global reveal-md`)
   - Docker Desktop (for optional portal mock)
2. **Python dependencies**
   ```bash
   pip install dc43 jupyterlab papermill rich typer streamlit
   ```
3. **Assets**
   - Portal mock app `packages/dc43-demo-app/src/dc43_demo_app/portal`
   - Notebook template `docs/notebooks/citizen-retail-starter.ipynb`
   - Dashboard JSON `docs/dashboards/citizen-retail.json`

## Preparation Steps
1. Clone repo and bootstrap citizen analytics demo:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   python scripts/bootstrap_demo_data.py --scenario citizen-analytics
   ```
2. Launch portal mock (optional UI overlay):
   ```bash
   streamlit run packages/dc43-demo-app/src/dc43_demo_app/portal/app.py
   ```
3. Submit access request workflow:
   ```bash
   python scripts/workflows/submit.py \
     --file docs/demo-contracts/retail-insights.yaml \
     --workflow citizen-access
   ```
4. Run notebook template with Papermill to generate visuals:
   ```bash
   papermill docs/notebooks/citizen-retail-starter.ipynb \
     artifacts/citizen-retail-output.ipynb
   ```
5. Import dashboard JSON into Grafana or Metabase for recording.
6. Generate Reveal.js slides:
   ```bash
   cd docs/influencer_capsules/capsule-09-citizen-analytics
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Recording Checklist
- Screen record portal access request and approval.
- Capture notebook execution (fast-forward long cells in post).
- Record final dashboard showing compliance badge overlay.

## Troubleshooting
- If Streamlit port 8501 is occupied, set `--server.port 8502`.
- Use `--dry-run` on workflows when you only need manifest artifacts.
