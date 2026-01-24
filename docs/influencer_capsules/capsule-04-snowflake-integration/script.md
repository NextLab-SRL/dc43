# Capsule 04 – Preparing DC43 for Snowflake Connectivity

**Runtime target:** 3 minutes 45 seconds (YouTube + LinkedIn)

> **Important:** DC43 does not ship an official Snowflake adapter yet. This
> capsule spotlights the roadmap, community prototypes, and how viewers can help
> test or contribute the integration.

## Segment 1 – Hook (0:00–0:25)
- **Narration**
  > "Snowflake fans, we've heard you. DC43 is architected to plug into any compute layer, and today I'm sharing how we're prepping the community-driven Snowflake adapter—and how you can help us finish it."
- **Stage directions**
  - {Action: Open on energetic host with Snowflake skyline graphic behind.}
  - {Action: Flash text "Roadmap Preview • Contributors Wanted".}

## Segment 2 – Why Snowflake Matters (0:25–1:05)
- **Narration**
  > "Snowflake runs mission-critical workloads for thousands of teams. If we can sync DC43 contracts directly into Snowflake tasks and views, governance becomes part of your ELT workflow." 
  > "That's why we're designing an adapter that translates DC43 policies into Snowflake tags, masking rules, and task orchestration." 
  > "Again: this is in development. No production connector yet—just prototypes you can explore."
- **Stage directions**
  - {Action: Display slide listing governance capabilities Snowflake offers.}
  - {Action: Highlight disclaimer badge "Prototype – Not GA".}

## Segment 3 – Architecture Preview (1:05–2:15)
- **Narration**
  > "Here's the plan. Step one: expose DC43 contracts via signed manifests. Step two: run a lightweight adapter service—written in Python—that maps contract metadata to Snowflake SQL DDL. Step three: deploy via Snowflake Tasks so updates propagate automatically." 
  > "Community members have already built proof-of-concept scripts syncing tags and row access policies. We're publishing the design spec this week so you can kick the tires." 
  > "If you rely on masking policies or data sharing, this is your chance to influence the final API."
- **Stage directions**
  - {Action: Walk through architecture slide with arrows from DC43 to Snowflake.}
  - {Action: Zoom in on pseudo-code snippet showing manifest translation.}
  - {Action: Show GitHub issue board with "Snowflake Adapter" label.}

## Segment 4 – Prototype Demo (2:15–3:15)
- **Narration**
  > "Let me show you the sandbox. We'll run a local mock of Snowflake using the open-source `snowflake-connector-python` against a test warehouse. The script reads a DC43 contract and generates the SQL needed to create masking policies. Then it outputs the statements you'd run in Snowflake." 
  > "We'll drop the script and sample contract in the resources so you can experiment safely."
- **Stage directions**
  - {Action: Screen capture terminal running `python prototypes/snowflake_adapter.py`.}
  - {Action: Highlight generated SQL, overlay note "Run in dev warehouse only".}
  - {Action: Show checklist of next steps for contributors.}

## Segment 5 – Call to Action (3:15–3:45)
- **Narration**
  > "Ready to help us make this real? Join the adapter working group—link in the description—and comment on the design RFC. We need Snowflake architects, governance leads, and QA testers." 
  > "DC43 stays vendor-neutral because contributors like you bring their expertise. Let's build this together."
- **Stage directions**
  - {Action: CTA slide with meeting cadence, RFC link, and QR to feedback form.}
  - {Action: End with direct-to-camera appeal and thumbs-up.}

## Safety Notes
- Repeat the "prototype" status verbally and on-screen.
- Encourage users to test only in development or sandbox accounts.
- Avoid showing proprietary Snowflake UIs—use mockups or open documentation.
