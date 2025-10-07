---
title: "DC43 × Snowflake (Roadmap Preview)"
author: "DC43 Influencer Series"
revealOptions:
  transition: "convex"
  backgroundTransition: "fade"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#011627" -->
# Roadmap Preview
<p style="color:#47D1C1;">DC43 ↔ Snowflake Adapter</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Prototype • Contributors Wanted</p>

<aside class="notes">
State clearly that no GA connector exists yet.
</aside>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1498050108023-c5249f4df085" data-background-size="cover" data-background-opacity="0.35" -->
## Why Snowflake?

- <span class="fragment fade-in">Governed data sharing at scale</span>
- <span class="fragment fade-in">Native masking + row access policies</span>
- <span class="fragment fade-in">Task orchestration &amp; notifications</span>
- <span class="fragment fade-in" style="color:#FF6F61;">Opportunity: Sync DC43 contracts automatically</span>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## Adapter Blueprint

```
DC43 Contract → Signed Manifest → Adapter Service → Snowflake SQL
```

<div class="columns">
<div>
<ul>
<li class="fragment fade-left">Manifest endpoint (REST + signatures)</li>
<li class="fragment fade-left">Python adapter (Typer + Jinja templates)</li>
<li class="fragment fade-left">Snowflake Tasks for rollout</li>
</ul>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1545239351-1141bd82e8a6" alt="Cloud data center" style="border-radius:18px; box-shadow:0 20px 48px rgba(1,22,39,0.35);"/>
</div>
</div>

---

<!-- .slide: data-background-gradient="linear-gradient(135deg,#47D1C1,#011627)" -->
### Prototype Output

```sql
create masking policy pii_mask as (val string) returns string ->
  case when current_role() in ('DATA_GOVERNANCE') then val else '***' end;

alter table staging.orders modify column customer_email set masking policy pii_mask;
```

<p class="fragment fade-in" style="color:#F5F7FA;">Generated from DC43 contract metadata · Run in dev only</p>

---

<!-- .slide: data-background-color="#0B1533" -->
## How to Contribute

1. Review RFC `2024-07-snowflake-adapter`
2. Test prototype script locally
3. Share feedback on GitHub discussion #142
4. Join bi-weekly working group (Thursdays)

<p class="fragment fade-in" style="color:#47D1C1;">Scan QR for sign-up + roadmap</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1520607162513-77705c0f0d4a" data-background-size="cover" data-background-opacity="0.3" -->
# Call to Action

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Comment on the RFC</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Bring Snowflake expertise</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Prototype tests welcome</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 18px 42px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">CTA</p>
<p style="color:#0B1533;">Scan for working group calendar + feedback form.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
