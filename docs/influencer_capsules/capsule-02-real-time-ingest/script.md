# Capsule 02 – Real-Time Ingestion without the Pain

**Runtime target:** 4 minutes (YouTube primary, cutdowns for Shorts/Reels)

## Segment 1 – Hook (0:00–0:20)
- **Narration**
  > "Streaming data doesn't have to mean spinning up yet another bespoke pipeline. In the next four minutes I'll show you how DC43's real-time ingest recipe lets you publish contracts once and have Spark Structured Streaming enforce them everywhere."
- **Stage directions**
  - {Action: Cold open with split screen: left = jittery streaming dashboard, right = smooth DC43 pipeline.}
  - {Action: Pop in animated title card "Contract-driven Streaming".}

## Segment 2 – The Status Quo (0:20–0:55)
- **Narration**
  > "Most teams wire stream processors directly to topics and pray schemas never change. When marketing adds a new field, production fails, governance panics, and the incident bridge fills up." 
  > "We need a control plane that understands contracts and can push updates to multiple runtimes without redeploying everything."
- **Stage directions**
  - {Action: Display montage of incident Slack messages, red flashing alerts.}
  - {Action: Overlay text "Schema drift = downtime".}

## Segment 3 – Meet the DC43 Ingest Recipe (0:55–2:10)
- **Narration**
  > "DC43 ships a reference ingest recipe that pairs the contract store with a streaming agent. You author the contract in YAML, sign it, and the agent pulls it on a schedule. The agent then configures Spark Structured Streaming checkpoints, quality rules, and dead-letter queues automatically." 
  > "Because contracts are immutable once approved, every consumer—from BI to ML—has the same expectations. If governance updates the policy, the agent picks up the new version and rolls it out without downtime."
- **Stage directions**
  - {Action: Walk viewers through the Reveal.js flow diagram.}
  - {Action: Zoom into key YAML snippet as you read it.}
  - {Action: Trigger overlay showing agent polling timeline.}

## Segment 4 – Demo Walkthrough (2:10–3:15)
- **Narration**
  > "Let me show you the exact flow we'll publish in the resources. We start by running `dc43 demo ingest realtime-retail` to seed sample clickstream data. Then we push the contract to the store and start the streaming agent. Within 20 seconds, the Spark job spins up, checkpoints to Delta Lake, and surfaces observability metrics in the DC43 dashboard." 
  > "When I add a new field—`promo_code`—the contract requires masking, so the agent tokenizes it before writing downstream. No redeploys, no guesswork."
- **Stage directions**
  - {Action: Switch to screen capture showing terminal commands.}
  - {Action: Highlight `promo_code` addition with callout.}
  - {Action: Cut to dashboard view showing metrics.}

## Segment 5 – CTA & Next Steps (3:15–4:00)
- **Narration**
  > "You can recreate this in under 30 minutes using the setup guide below. Try it, remix it, and share your results with the hashtag #DC43Builds." 
  > "Want to take it further? Pair DC43 with your favorite quality toolkit or build a new runtime adapter. Check the issue board for open streaming enhancements—we'd love your pull request."
- **Stage directions**
  - {Action: Display CTA slide with build checklist and QR to GitHub issues.}
  - {Action: End with upbeat nod, fade out over dashboard loop.}

## Safety Notes
- Emphasize that the streaming recipe uses open-source connectors; avoid implying vendor lock-in.
- Clarify that data shown is synthetic retail telemetry.
