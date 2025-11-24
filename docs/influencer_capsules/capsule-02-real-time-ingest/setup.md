# Setup – Capsule 02 "Real-Time Ingestion"

## Objective
Recreate the streaming demo with synthetic retail clickstream data so viewers
see contract-driven governance in action.

## Prerequisites
1. **Local tooling**
   - Python 3.10+
   - Java 11+ (for Spark)
   - Docker Desktop (for optional Kafka stub)
   - Node.js 18+ and `npm install --global reveal-md`
2. **Python dependencies**
   ```bash
   pip install dc43 pyspark==3.5.1 delta-spark==3.1.0 kafka-python rich typer
   ```
3. **Optional container stack**
   - `docker compose` available for spinning up Kafka + Schema Registry test
     services using `deploy/compose/streaming-demo.yml`.

## Environment Preparation
1. **Clone repository & bootstrap demo assets**
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   python scripts/bootstrap_demo_data.py --scenario realtime-retail
   ```
2. **Start infrastructure (optional but recommended)**
   ```bash
   docker compose -f deploy/compose/streaming-demo.yml up -d
   ```
   This provisions Kafka, a mock contract store API, and Grafana for metrics.
3. **Seed contracts**
   ```bash
   python scripts/contracts/publish.py \
     --file docs/demo-contracts/realtime-retail.yaml \
     --endpoint http://localhost:9000/contracts
   ```
4. **Launch streaming agent**
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/agents/streaming.py \
     --config docs/demo-contracts/realtime-retail-agent.yaml
   ```
5. **Run Reveal.js slides locally**
   ```bash
   cd docs/influencer_capsules/capsule-02-real-time-ingest
   reveal-md slides.md --watch --css ../shared/dc43-theme.css
   ```

## Capture Checklist
- Record terminal session using asciinema or screen recorder at 60fps.
- Capture Grafana dashboard once metrics populate (approx. 30s after agent
  start).
- Export YAML diff when adding `promo_code` field for on-screen overlay.

## Troubleshooting
- If Spark fails to start, ensure `JAVA_HOME` points to Java 11 and increase
  driver memory: `export SPARK_DRIVER_MEMORY=4g`.
- If Kafka is unavailable, use the in-memory event source by adding
  `--use-mock-source` to the agent command; visuals remain the same.
