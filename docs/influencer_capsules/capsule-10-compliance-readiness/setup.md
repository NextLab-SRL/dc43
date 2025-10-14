# Setup – Capsule 10 "Compliance Readiness"

## Objective
Produce the compliance evidence bundle demo and showcase the readiness
checklist. Ensure messaging emphasizes audit transparency.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Node.js 18+ (`npm install --global reveal-md`)
   - Docker Desktop (for optional GRC mock service)
2. **Python dependencies**
   ```bash
   pip install dc43 rich typer click tabulate
   ```
3. **Assets**
   - Checklist `docs/compliance/compliance-readiness-checklist.xlsx`
   - Evidence sample `docs/compliance/sample-evidence-bundle.zip`
   - GRC mock uploader `scripts/compliance/mock_grc_upload.py`

## Preparation Steps
1. Clone repo and generate fresh evidence bundle:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   dc43 compliance bundle retail-promos --quarter Q3-2024 --output artifacts/compliance-bundle.zip
   ```
2. Inspect bundle contents for screenshots:
   ```bash
   unzip -l artifacts/compliance-bundle.zip
   ```
3. Generate checklist PDF for on-screen reference:
   ```bash
   python scripts/compliance/export_checklist.py \
     --source docs/compliance/compliance-readiness-checklist.xlsx \
     --output artifacts/compliance-readiness.pdf
   ```
4. (Optional) Start mock GRC uploader for demo:
   ```bash
   python scripts/compliance/mock_grc_upload.py --port 5055
   ```
5. Generate Reveal.js slide deck:
   ```bash
   cd docs/influencer_capsules/capsule-10-compliance-readiness
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Recording Checklist
- Capture CLI command generating bundle (terminal at 18pt font).
- Record walkthrough of checklist PDF (use screen annotations to highlight rows).
- Show upload step to mock GRC system with success confirmation.

## Troubleshooting
- If CLI command missing, install latest project build: `pip install -e .`.
- Use `--include-logs` flag to append monitoring evidence if bundle seems empty.
