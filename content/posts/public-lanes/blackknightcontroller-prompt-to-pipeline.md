---
title: Prompt to Pipeline: The BlackKnightController Operating Contract
slug: blackknightcontroller-prompt-to-pipeline
summary: BlackKnightController is strongest when discovery work is converted back into executable pipeline actions, guards, parameters, and validation evidence.
tags: [blackknightcontroller, lab, pipelines, ipmi, pxe, docker-swarm]
theme_variant: midnight
status: published
seo_title: BlackKnightController prompt to pipeline operating model
seo_description: Learn the BlackKnightController operating contract: use AI and diagnostics to discover the fix, then capture the result as executable pipeline logic with validation evidence.
---

The most important BlackKnightController rule is simple:

```text
prompt -> Codex -> BKC pipeline
```

The prompt starts the work. Codex helps investigate, reason, template, and
repair. But the finished result must land back in the BlackKnightController
pipeline.

If a command only lived in an SSH session, it was diagnostic evidence. It was
not the finished automation.

## Why this rule exists

Infrastructure work has a trap: a human and an assistant can fix a system once
and still fail to improve the environment.

That happens when the useful discovery disappears into shell history:

- the PXE exception that had to be MAC-scoped;
- the BIOS setting that changed the boot path;
- the installer argument that made Debian behave;
- the post-install validation that proved Horizon or Proxmox was actually
  usable;
- the edge proxy rule that made the service reachable from the operator side.

BlackKnightController treats those discoveries as pipeline material.

The pipeline should capture:

- the action;
- the target;
- the guardrail;
- the known-good fragment;
- the validation check;
- the evidence link or receipt.

![BlackKnightController pipeline workbench](/content-files/assets/bkc/bkc-pipeline-workbench.png)

The pipeline workbench is where that contract becomes visible. A pipeline is not
just a name in a list; it carries source layers, resolved dictionary values,
latest run status, stage maps, actions, and run history. That gives the operator
and the assistant the same object to reason about.

![BlackKnightController pipeline detail editor](/content-files/assets/bkc/bkc-pipeline-detail-edit.png)

The edit/detail view is where the pipeline becomes maintainable instead of
mysterious. JSON fields, stage definitions, notes, links, and dashboards need to
be readable enough that an engineer can review them quickly and an assistant can
reuse them without drifting into guesswork.

That is how a messy lab session becomes a better next run.

## What the recent runs proved

The lab sequence exercised the full control path:

- iDRAC/IPMI power control;
- exact-MAC PXE boot lanes;
- Debian Trixie bare-metal provisioning;
- OpenStack and Proxmox rebuilds;
- ESXi installer handoff and post-install control;
- Docker Swarm seeding on OpenStack and ESXi guests;
- edge URLs, Grafana, Portainer, and resource graph visibility.

The important part is not that every step was perfect the first time. The
important part is that the failures produced better fragments.

Once a Debian PXE lane was known good, it needed to be protected. Once the ESXi
VM clone pattern worked, it became a reusable workload seed. Once a broad PXE
listener was risky, PXE became deny-by-default with exact target exceptions.

![BlackKnightController inventory console](/content-files/assets/bkc/bkc-inventory-console.png)

The same pattern applies to inventory. Hosts, groups, VMs, clusters, APIs,
containers, pipelines, actions, and credentials should not live in separate
mental drawers. BKC keeps them close enough that a pipeline can explain what it
touches and why.

## The product direction

BlackKnightController is not trying to replace every infrastructure tool. It is
trying to connect the useful control surfaces that already exist:

- power control;
- boot intent;
- installer payloads;
- SSH and API actions;
- generated scripts and templates;
- network and DNS updates;
- validation and evidence capture.

That combination lets a small team treat physical infrastructure more like a
repeatable deployment target.

The magic is not that an AI assistant can type commands quickly. The magic is
that the useful commands become part of an operational memory the next run can
trust.
