---
title: Auzietek, BlackKnightController, and Codex: Turning Operations Into a Living Fabric
slug: auzietek-bkc-codex-operating-fabric
summary: Auzietek is using BlackKnightController and Codex together to control lab systems, hosted services, content publishing, DNS, and documentation as one evidence-driven operating fabric.
tags: [services, think-tank, aiops, blackknightcontroller, codex, lab, hosted-services]
theme_variant: auzietek
status: published
seo_title: Auzietek AI-assisted operations with BlackKnightController and Codex
seo_description: Auzietek uses BlackKnightController and Codex to automate infrastructure, document evidence, manage lab services, and guide hosted service operations through repeatable workflows.
---

Something unusual is happening in the Auzietek lab.

We are not just using AI to write notes. We are using Codex and
BlackKnightController together to operate real systems: lab hypervisors, Docker
Swarms, OpenStack, ESXi, DNS records, edge services, documentation captures, and
public content.

That matters because the hard part of operations is rarely one command. The hard
part is context:

- what exists;
- how it is related;
- which system owns the current truth;
- what changed last time;
- what can be safely automated now;
- what needs a human decision before the next step.

BlackKnightController gives those actions a home. Codex helps reason across the
evidence, write the next safe step, and turn successful work back into reusable
pipelines, docs, and content.

![BlackKnightController Company Mind resource workbench](/content-files/assets/bkc/bkc-company-mind-resources.png)

## From assistant chat to operational control

The pattern is simple to say and powerful in practice:

```text
human intent -> Codex reasoning -> BKC action path -> validation -> evidence
```

In the lab, that means a conversation can become a real operational workflow:

- capture current BKC resource graph views as PNG documentation artifacts;
- sync those images into the publishing system;
- update public articles with live proof;
- refresh content projections;
- validate the edge URLs;
- commit the exact code/content changes back to Git.

That is a very different workflow from a traditional admin panel. The human is
still in control, but the system can carry more context and perform more of the
repeatable work.

## Hosted services are part of the same story

Auzietek also runs public services: site content, beta publishing lanes,
Kanboard-style planning, Grafana/observability, DNS, and future certificate
rotation work. Those are not separate from the lab story.

They are exactly where the pattern gets interesting.

If BKC can understand a lab host, it can also understand a hosted VPS. If it can
capture a graph view for documentation, it can help publish the evidence. If it
can update lab DNS safely, it can grow into a DNS and certificate rotation
pipeline for public systems.

That is the Auzietek direction: make operations feel more natural, predictable,
and clean by connecting the human, the assistant, the tools, and the evidence.

![BlackKnightController integrations screen](/content-files/assets/bkc/bkc-integrations.png)

## Why this is more than a demo

Most automation demos stop at “it ran.”

Auzietek is aiming for something stronger:

- it ran;
- it was validated;
- the evidence was captured;
- the documentation was updated;
- the reusable fragment was preserved;
- the public story became clearer.

That loop is the product idea hiding in plain sight. AI is useful, but AI plus
clean tool boundaries, operational memory, resource graphs, pipelines, and human
judgment is where the work starts to feel genuinely different.

This is the practical edge of the ThinkTank: systems that help people operate
more infrastructure with less mystery.
