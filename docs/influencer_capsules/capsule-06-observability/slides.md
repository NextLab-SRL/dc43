---
title: "DC43 Observability Playbook"
author: "DC43 Influencer Series"
revealOptions:
  transition: "slide"
  backgroundTransition: "fade"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#030D1F" -->
# See Your Contracts in Action
<p style="color:#47D1C1;">DC43 Observability Stack</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Metrics • Dashboards • Alerts</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1517433456452-f9633a875f6f" data-background-size="cover" data-background-opacity="0.35" -->
## Blind Spots Today

- <span class="fragment fade-in">No freshness telemetry</span>
- <span class="fragment fade-in">Manual policy checks</span>
- <span class="fragment fade-in">Late breach notifications</span>
- <span class="fragment fade-in">Fragmented reporting</span>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## Playbook Layers

<div class="columns">
<div>
<ol>
<li class="fragment fade-right">Instrument agents (Prometheus)</li>
<li class="fragment fade-right">Visualize (Grafana dashboards)</li>
<li class="fragment fade-right">Automate alerts (Slack/Teams)</li>
</ol>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1553877522-43269d4ea984" alt="Observability monitors" style="border-radius:18px; box-shadow:0 22px 44px rgba(3,13,31,0.35);"/>
</div>
</div>

---

<!-- .slide: data-background-gradient="radial-gradient(circle at top,#47D1C1,#030D1F)" -->
### Key Metrics

- Contracts deployed
- Policy breaches (rolling 24h)
- Freshness SLA compliance
- Data volume & drift signals

<p class="fragment fade-in" style="color:#F5F7FA;">Powered by Prometheus exporter in DC43 agents</p>

---

<!-- .slide: data-background-color="#0B1533" -->
## Demo Runbook

1. `docker compose -f deploy/compose/observability-demo.yml up`
2. Start streaming agent with `--emit-metrics`
3. Import Grafana dashboard JSON
4. Pause agent to trigger alert

<p class="fragment fade-in" style="color:#47D1C1;">Synthetic data only · repeatable demo</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1460925895917-afdab827c52f" data-background-size="cover" data-background-opacity="0.35" -->
# Share Your Dashboards

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Download playbook PDF</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Submit panels in community forum</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Join observability office hours</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 18px 42px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">CTA</p>
<p style="color:#0B1533;">Scan to grab the playbook + dashboard bundle.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
