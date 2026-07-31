---
title: AuziX: Retro Roots and a Future Workstation OS Experiment
slug: auzix-retro-roots-future-workstation-os-experiment
summary: AuziX carries Amiga, GoboLinux, Slackware, Nix, and modern container ideas into a speculative but concrete workstation operating-system experiment.
tags: [retro, auzix, amiga, linux, nix, gobo-linux, workstation, teaching]
theme_variant: retro
status: published
seo_title: AuziX retro-inspired workstation OS experiment
seo_description: AuziX is an Auzietek operating-system experiment that blends Amiga-style structure, readable paths, reproducible builds, containers, and modern Linux foundations.
hero_image_url: /content-files/assets/retro/auzix/auzix-midori.jpg
---

AuziX is where the retro lane tilts toward the future.

It is not simply “make Linux look like an Amiga.” That would be cute, but too
small. The deeper question is more interesting:

> What if the things older systems got right — readable structure, fast feedback,
> clear ownership, understandable startup, and user-visible system logic — were
> rebuilt with modern Linux, reproducible builds, containers, and local compute
> fabric ideas?

That is the AuziX thread.

![AuziX Midori browser checkpoint](/content-files/assets/retro/auzix/auzix-midori.jpg)

## The design DNA

AuziX pulls inspiration from several places:

- AmigaOS for logical structure, startup clarity, and desktop personality.
- GoboLinux for human-readable application hierarchy ideas.
- Slackware for simple, inspectable system habits.
- Nix and reproducible-build thinking for deterministic system state.
- Modern container patterns for isolating services without hiding them.

The public repo describes a root contract around directories such as:

```text
/System
/Programs
/Services
/Stacks
/Work
/Users
/Volumes
/Network
```

That is not just cosmetic. It is an attempt to make ownership visible.

## Why this belongs beside Amiga projects

Retro systems make constraints visible. AuziX asks whether a modern workstation
can recover some of that clarity without giving up modern hardware, browsers,
networking, package transport, and service isolation.

![AuziX NetSurf checkpoint](/content-files/assets/retro/auzix/auzix-netsurf.jpg)

## Current state: experimental, but concrete

The work is speculative, but not imaginary. Local notes show boot images,
installer paths, package receipts, browser checkpoints, strict-root audits, and
KVM-first test loops.

![AuziX lab deployment proof from the BlackKnightController run history](/content-files/assets/retro/auzix/proof/auzix-lab-demo.png)

That distinction matters. Auzietek can talk about future computing ideas
because the ideas are being pushed through real build artifacts and real
validation evidence.

![AuziX installer repository checkpoint](/content-files/assets/retro/auzix/auzix-installer-repository.jpg)

The useful lesson for readers is not “copy this OS today.” The useful lesson is
how to reason about operating-system shape:

- make path ownership visible;
- keep compatibility as a bridge, not the whole identity;
- prefer small, inspectable services;
- preserve receipts that describe intent;
- test in a fast VM loop before chasing harder hardware.

![AuziX working-session proof trail](/content-files/assets/retro/auzix/proof/auzix-working-session.png)

Repo: [AuziX on GitHub](https://github.com/auzieman/AuziX).
