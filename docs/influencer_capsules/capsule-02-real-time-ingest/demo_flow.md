# Demo Flow – Capsule 02

## Overview
This flow demonstrates publishing a DC43 contract and enforcing it with the
streaming agent against synthetic retail clickstream data. The sequence mirrors
the narration in Segment 4 of the script.

## Steps
1. **Bootstrap sample data**
   ```bash
   dc43 demo ingest realtime-retail --rate 500 --duration 180
   ```
   - Generates JSON payloads pushed to Kafka topic `retail_clicks` (or in-memory
     fallback).
2. **Inspect contract**
   ```bash
   cat docs/demo-contracts/realtime-retail.yaml
   ```
   - Highlight masking rules for `customer_id` and freshness SLA.
3. **Publish contract to store**
   ```bash
   python scripts/contracts/publish.py \
     --file docs/demo-contracts/realtime-retail.yaml \
     --endpoint http://localhost:9000/contracts
   ```
4. **Start streaming agent**
   ```bash
   python packages/dc43-demo-app/src/dc43_demo_app/agents/streaming.py \
     --config docs/demo-contracts/realtime-retail-agent.yaml \
     --log-level info
   ```
   - Wait for log line `Contract realtime-retail@1.4.0 applied`.
5. **Show Spark Structured Streaming job**
   ```bash
   spark-submit docs/demos/streaming_retail_job.py
   ```
   - Optional if you want to illustrate direct Spark integration.
6. **Introduce schema change**
   ```bash
   python scripts/contracts/bump_field.py \
     --file docs/demo-contracts/realtime-retail.yaml \
     --field promo_code --pii tokenize
   ```
   - Republish contract and show agent reloading automatically.
7. **Display observability dashboard**
   - Open Grafana at http://localhost:3000 and show throughput, lag, and policy
     compliance panels.

## Recording Tips
- Run terminal in dark mode with 18pt font.
- Clear previous outputs before recording each take.
- Use OBS scenes to switch between camera, terminal, and dashboard smoothly.
