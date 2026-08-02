---
title: This Week at Auzietek: Human-Led AIOps in the Lab
slug: this-week-at-auzietek-human-led-aiops-in-the-lab
summary: A weekly Auzietek field report showing how the lab used BlackKnightController, Codex, Astra, Ollama, screenshots, and pipelines to turn infrastructure work into reviewable evidence.
tags: [services, aiops, leadership, blackknightcontroller, company-mind, evidence, principles]
theme_variant: auzietek
status: published
seo_title: This Week at Auzietek human-led AIOps lab report
seo_description: A weekly Auzietek update showing human-led AIOps, BlackKnightController evidence, pipeline discipline, screenshots, and practical infrastructure leadership from the lab.
hero_image_url: /content-files/assets/brand/auzietek-company-mind-concept.png
---

This week at Auzietek, the lab crossed a useful line.

The story was not only “we built another thing.” The story was that the work
started to document itself: BlackKnightController views became evidence,
pipelines became explainable, lab content became reviewable, and the public
site started carrying the stronger product story instead of only polished copy.

## What changed this week

The lab moved several threads forward at once:

- BlackKnightController's resource workbench became the main `/resources` view.
- Pipeline and latest-run pages gained local explainer paths.
- Draw.io exports became useful enough for review and planning.
- The local Ollama worker started assisting with pipeline explanations and
  first-pass graph layouts.
- The lab edge page gained direct review links.
- Micro-blog content can now be refreshed through the filesystem import loop.
- Public Auzietek and BlackKnight content started using product screenshots as
  proof instead of decorative filler.

That matters because infrastructure work often fails in the same quiet way.

The people involved are capable. The tools are powerful. The documentation
exists somewhere. The dashboard has a lot of numbers. But the actual operating
path is still unclear when something needs to change.

Auzietek is building toward a simpler idea:

```text
humans set intent
systems expose state
assistants help reason
pipelines do repeatable work
evidence proves what happened
```

That is human-led AIOps. Not magic. Not replacement. A cleaner partnership
between people, tools, and operational memory.

## The review loop is becoming a product feature

The current lab loop looks like this:

```text
pipeline / UI / content change
  -> deploy to the OpenStack or ESXi lab target
  -> capture BKC views and route proof
  -> review with Auzie, Codex, and Astra
  -> update the pipeline, article, or UI fragment
  -> sync/import/deploy again
  -> promote only after the evidence reads cleanly
```

The important part is that micro-blog can accept the same content update more
than once. A revision is not a crisis. The lab can refresh, re-import, and show
the new state without hand-editing the database.

The stronger proof, though, is not a screenshot of the public website. It is
the BKC UI showing the actual operating surface: resources, inventory,
pipelines, integrations, evidence, and the relationships between them.

![BlackKnightController OpenStack pipeline workbench](/content-files/assets/bkc/openstack-bkc/bkc-pipelines.png)

The local Ollama worker is part of that story. Auzietek is not trying to make a
model secretly operate the lab. The useful version is more disciplined:

```text
BKC gathers state and evidence
Ollama explains or proposes a layout
Codex turns approved changes into files and pipelines
Auzie reviews the result
BKC executes the repeatable action path
```

That keeps the human in authority while still letting the system become more
understandable.

## The business problem is usually not one command

Most fragile infrastructure work is not fragile because one command is hard.
It is fragile because the context is scattered:

- which server is the real target;
- which network path is valid from here;
- which credential or key is current;
- what changed last time;
- what can safely be retried;
- what should never be done without approval.

When that context only lives in a senior engineer's head, every change becomes
more expensive than it should be.

BlackKnightController is Auzietek's practical answer to that problem. It turns
repeatable work into pipelines, keeps evidence near the action, and gives a
human-readable shape to the systems being operated.

![BlackKnightController OpenStack-hosted resource workbench](/content-files/assets/bkc/openstack-bkc/bkc-beta-resources.png)

## AI is strongest when grounded

A general model can reason surprisingly well when the tool boundaries are clean.
It does not need to “be the infrastructure.” It needs grounded context:

- stable API contracts;
- descriptive object names;
- predictable folder conventions;
- compact JSON state fragments;
- links between evidence, decisions, and resources;
- strict action permissions.

That is the Auzietek preference. Let AI help with the reasoning, writing,
comparison, summarization, and layout work. Let the controller own the actual
action path and preserve the result.

## Evidence creates trust

Trust is not built by saying “the automation ran.”

Trust comes from proof:

- the route returned the expected status;
- the service accepted the expected login;
- the pipeline captured the stage result;
- the screenshot shows the current UI;
- the graph shows the resource relationship;
- the reusable fragment explains why this pattern should not regress.

![BlackKnightController pipeline detail and edit view](/content-files/assets/bkc/openstack-bkc/bkc-pipeline-detail-edit.png)

That evidence is useful for operators, clients, junior engineers, and future
assistants. It turns a one-time fix into an organizational memory.

![BlackKnightController inventory console](/content-files/assets/bkc/openstack-bkc/bkc-inventory.png)

Even early diagram output becomes useful when it is honest about its maturity.
This first-pass IONOS/lab-edge diagram is not final design collateral. It is
editable operational evidence: a generated sketch that can be reviewed,
corrected, and turned into documentation.

![First-pass editable IONOS and lab edge diagram](/content-files/assets/bkc/diagrams/ionos-lab-edge-story-preview.png)

That is the more interesting product pattern. A screenshot proves what the UI
looked like. A diagram explains how the pieces relate. A fragment explains why
the decision was made. A pipeline repeats the work.

## Website proof should be concise

For public site review, a single front-door screenshot per lane is usually
enough: Auzietek, BlackKnightController, Linux Users, and Retro Users. The
deeper screenshots should belong to the product story: the BKC views, pipeline
highlights, run evidence, and diagrams that show how the lab is actually being
operated.

## Cleaner operations are a leadership choice

Auzietek is still an emerging lab and product effort, not a giant consulting
firm pretending to have a thousand people behind a landing page.

That is part of the point.

Small teams can do serious work when they build the right loops:

1. start with a real problem;
2. automate the smallest useful path;
3. validate it against live systems;
4. preserve the evidence;
5. publish the lesson when it is safe;
6. improve the next run.

This is lean engineering applied to operations. Less ceremony. More proof.

## What Auzietek wants to help teams do

The practical service direction is straightforward:

- turn tribal runbooks into repeatable pipelines;
- map infrastructure relationships so teams know what they own;
- build safer lab and small-office automation paths;
- create useful documentation from real evidence;
- use AI assistance without hiding risk or judgment;
- teach newer engineers through examples that actually ran.

That model can fit a homelab, a small business, an MSP, a school, a nonprofit,
or a larger company trying to cut through tool sprawl.

The tone may be experimental. The goal is serious: make technology more
natural, predictable, humane, and clean.
