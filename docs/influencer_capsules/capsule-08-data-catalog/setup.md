# Setup – Capsule 08 "Data Catalog Harmony"

## Objective
Demonstrate syncing DC43 contracts into a data catalog (OpenMetadata example)
while highlighting reusable patterns for other catalog platforms.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Node.js 18+ (`npm install --global reveal-md`)
   - Docker Desktop (for local OpenMetadata stack)
2. **Python dependencies**
   ```bash
   pip install dc43 httpx rich typer pydantic yaml
   ```
3. **Assets**
   - Integration script `integrations/catalog_sync.py`
   - Sample manifest `docs/demo-contracts/catalog-sync-manifest.json`
   - Catalog mapping config `integrations/config/catalog-openmetadata.yaml`

## Preparation Steps
1. Clone repo and start OpenMetadata (optional for UI shots):
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   docker compose -f deploy/compose/openmetadata-demo.yml up -d
   ```
2. Publish contract to generate manifest:
   ```bash
   python scripts/contracts/publish.py \
     --file docs/demo-contracts/delta-retail.yaml \
     --endpoint http://localhost:9000/contracts \
     --emit-manifest artifacts/catalog-manifest.json
   ```
3. Run catalog sync script:
   ```bash
   python integrations/catalog_sync.py \
     --manifest artifacts/catalog-manifest.json \
     --config integrations/config/catalog-openmetadata.yaml \
     --dry-run > artifacts/catalog-sync.log
   ```
   - For real API calls, remove `--dry-run` and ensure environment variables for
     API tokens are set (`OPENMETADATA_TOKEN`).
4. Generate Reveal.js slides:
   ```bash
   cd docs/influencer_capsules/capsule-08-data-catalog
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```
5. Prepare glossary screenshot by visiting
   http://localhost:8585/catalog and selecting the synced asset.

## Recording Checklist
- Capture terminal output showing field mappings.
- Screen record catalog UI before and after sync.
- Export glossary table as PNG for overlay.

## Troubleshooting
- If OpenMetadata containers fail, run `docker compose logs openmetadata`.
- Use `--platform catalog-mock` to output Markdown summary instead of hitting an
  API—useful when recording without Docker.
