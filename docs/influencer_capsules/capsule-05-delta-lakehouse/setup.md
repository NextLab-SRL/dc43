# Setup – Capsule 05 "Delta Lakehouse"

## Objective
Demonstrate how DC43 contracts drive a Delta Lake build-out. Capture notebook
runs, contract updates, and observability dashboards.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Java 11+
   - Spark 3.5.1 with Delta Lake 3.1.0 (use `pip install delta-spark==3.1.0`)
   - Node.js 18+ and `npm install --global reveal-md`
2. **Python dependencies**
   ```bash
   pip install dc43 pyspark==3.5.1 delta-spark==3.1.0 great-expectations typer rich
   ```
3. **Optional notebooks**
   - Databricks Community Edition or open-source alternative like
     `pyspark-notebook` Docker image.

## Preparation Steps
1. Clone repo and bootstrap lakehouse demo assets:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   python scripts/bootstrap_demo_data.py --scenario lakehouse-retail
   ```
2. Start local Delta Lake environment (if using Docker):
   ```bash
   docker compose -f deploy/compose/delta-demo.yml up -d
   ```
3. Open notebook `docs/demos/delta_lakehouse_demo.ipynb` and run all cells to
   generate baseline visuals.
4. Export contract YAML for overlay shots:
   ```bash
   cat docs/demo-contracts/delta-retail.yaml > artifacts/delta-retail.yaml
   ```
5. Generate Reveal.js slide assets:
   ```bash
   cd docs/influencer_capsules/capsule-05-delta-lakehouse
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Recording Checklist
- Screen record notebook execution in 1080p, 60fps; trim waiting periods in
  post-production.
- Capture Spark SQL command `DESCRIBE HISTORY retail_sales` to show contract-driven
  updates.
- Record observability dashboard (Grafana) after adapter run.

## Troubleshooting
- If Spark fails due to memory, set `SPARK_DRIVER_MEMORY=4g` and rerun.
- Use `--dry-run` on adapter script if you want to show generated SQL without
  executing changes.
