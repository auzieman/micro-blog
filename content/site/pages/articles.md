---
page: articles
title: Articles, Proof, and Tutorial Lanes
body: Auzietek articles connect public explanations to working lab evidence, screenshots, and reusable examples.
tag: null
eyebrow: Publication map
---

# Articles, Proof, and Tutorial Lanes

Auzietek articles are intended to work like a modern technical magazine backed by a living lab.

The article explains the idea. The screenshots prove the work existed. The repository carries the runnable material when the topic deserves a companion kit. BlackKnightController keeps the operational evidence close to the pipelines and resources that produced it.

## The four public lanes

### Auzietek

The Auzietek lane explains the company direction: services, principles, human-led AIOps, business case, and ThinkTank.

It should answer practical questions:

- What can Auzietek help with?
- Why does this automation model matter?
- How does BlackKnightController reduce repeated work?
- Where do the long-range ideas fit without becoming vaporware?

### BlackKnightController

The BlackKnight lane is product proof. It should show real infrastructure automation: IPMI, PXE, pipelines, resource graphs, edge routing, hypervisors, Docker Swarm, OpenStack, ESXi, validation, and receipts.

![BlackKnightController pipeline detail and edit view](/content-files/assets/bkc/bkc-pipeline-detail-edit.png)

This lane is where public readers should see the system becoming real.

### Linux Users

The Linux Users lane preserves practical engineering lessons: commands, caveats, diagrams, old notes brought forward, and modernized examples.

The tone should be clear enough for a newer engineer while preserving enough detail for someone experienced to trust it.

Useful Linux articles should include:

- exact commands where appropriate;
- portability notes;
- assumptions and prerequisites;
- terminal captures or diagrams;
- links to companion repositories when the example becomes large.

### Retro Users

The Retro Users lane keeps classic-computing lessons alive: AmigaOS experiments, AuziX ideas, emulation, preservation, old constraints, and the discipline that comes from understanding smaller systems deeply.

Retro is not nostalgia as decoration. It is a reminder that engineering can be fun, understandable, and surprisingly durable.

## Current proof spotlight

Recent lab work produced screenshots and stories that belong directly in the public site.

![BlackKnightController inventory console](/content-files/assets/bkc/bkc-inventory-console.png)

Those screenshots support articles about:

- rebuilding lab hosts from destructive PXE pipelines;
- recovering from damaged workstation state;
- moving services between Proxmox, ESXi, OpenStack, and Docker Swarm;
- using DNS and edge routing to expose review sites;
- turning pipeline history into public tutorials;
- keeping AI-assisted operations bounded by evidence and approval.

## The magazine-and-repo pattern

For small lessons, the article can stand alone.

For larger lessons, the article should become the readable front page for a companion repository:

```text
article
  -> explanation
  -> diagrams and screenshots
  -> companion repo
  -> runnable examples
  -> follow-up article after validation
```

That pattern matters for older Puppet/OpenVox notes, ESXi backup scripts, Docker Swarm examples, PXE guides, monitoring stacks, and BlackKnightController pipeline translations.

## Editorial rules

Auzietek should avoid empty claims. If an article says a pattern works, it should say where it was tested, what assumptions were used, and what still needs polish.

Good public material should:

- preserve useful source detail;
- avoid over-compressing technical articles into summaries;
- include media when it proves or clarifies the work;
- link to GitHub when the example becomes code-heavy;
- keep opinion grounded in evidence;
- make the next engineer more capable.

## Where to start

- Read the BlackKnightController lane for product proof.
- Read Linux Users for practical systems tutorials.
- Read Retro Users for classic-computing continuity.
- Read ThinkTank for the longer arc: RACS, AuziX, Company Mind, and human-first automation.

