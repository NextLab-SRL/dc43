---
title: "Contract-Driven Real-Time Ingestion"
author: "DC43 Influencer Series"
revealOptions:
  transition: "slide"
  backgroundTransition: "convex"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#040B1A" -->
# Streaming without firefighting
<p style="color:#47D1C1;">DC43 Ingest Recipe</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Publish once. Enforce everywhere.</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1517430816045-df4b7de11d1d" data-background-size="cover" data-background-opacity="0.35" -->
## Legacy pain

- <span class="fragment fade-in">Topic-specific pipelines</span>
- <span class="fragment fade-in">Schema drift = red alerts</span>
- <span class="fragment fade-in">Manual policy enforcement</span>
- <span class="fragment fade-in">Unclear audit trail</span>

<aside class="notes">
Use quick punch-in on each bullet to match narration pace.
</aside>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## DC43 Ingest Blueprint

<div class="columns">
<div>
<ul>
<li class="fragment fade-left"><strong>Contract Store</strong><br/><span style="font-size:0.8em;">Versioned YAML spec</span></li>
<li class="fragment fade-left"><strong>Streaming Agent</strong><br/><span style="font-size:0.8em;">Pulls and enforces contract</span></li>
<li class="fragment fade-left"><strong>Runtime</strong><br/><span style="font-size:0.8em;">Spark Structured Streaming</span></li>
</ul>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1582719478250-c89cae4dc85b" alt="Neon data stream" style="border-radius:18px; box-shadow:0 24px 48px rgba(4,11,26,0.35);"/>
</div>
</div>

---

<!-- .slide: data-background-gradient="radial-gradient(circle at center,#47D1C1,#040B1A)" -->
### YAML Snapshot

```yaml
contract: realtime-retail
version: 1.4.0
fields:
  - name: customer_id
    type: string
    pii: tokenize
  - name: promo_code
    type: string
    pii: tokenize
quality:
  freshness: 30s
  completeness: 99.5%
```

<p class="fragment fade-in" style="color:#F5F7FA;">Contracts drive runtime settings & security.</p>

---

<!-- .slide: data-background-color="#101E3A" -->
## Demo Flow

1. `dc43 demo ingest realtime-retail`
2. Publish contract to store
3. Start streaming agent
4. Watch Grafana metrics update in seconds

<p class="fragment fade-in" style="color:#47D1C1;">All synthetic data · repeatable runbook</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1531297484001-80022131f5a1" data-background-size="cover" data-background-opacity="0.3" -->
## Ready to build?

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Clone repo &amp; follow setup guide</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Share your remix with #DC43Builds</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Contribute adapters &amp; recipes</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 16px 32px rgba(0,0,0,0.4);">
<p style="color:#040B1A; font-weight:700; margin-bottom:12px;">Call To Action</p>
<p style="color:#040B1A;">Scan for GitHub issues board &amp; quickstart.</p>
<div style="width:180px; height:180px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
