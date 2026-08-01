---
title: From Recovery Weekend to Repeatable Hypervisor Lab
slug: blackknightcontroller-recovery-weekend-repeatable-lab
summary: A real lab recovery story showing why BlackKnightController keeps evidence, hardware control, PXE intent, and post-install validation close together.
tags: [blackknightcontroller, lab, proxmox, openstack, esxi, docker-swarm]
theme_variant: midnight
status: published
seo_title: BlackKnightController lab recovery and repeatable hypervisor deployment
seo_description: A practical BlackKnightController story about recovering lost workstation state, rebuilding hypervisors, and turning discovery into repeatable deployment pipelines.
---

Good automation earns its keep when the day gets messy.

During one lab weekend, a workstation failure damaged local project state while
the active BlackKnightController deployment was still running in Docker Swarm.
That forced a useful question: if the engineer loses the convenient workstation,
how much of the infrastructure story can be recovered from the running control
plane, shared storage, git history, and the machines themselves?

That is exactly the kind of pressure BlackKnightController is meant to absorb.

## The useful pattern

The lab was not treated as sacred. The servers were cattle, not pets. OpenStack,
Proxmox, and ESXi targets could be wiped and rebuilt because the interesting
state lived in pipelines, fragments, shared content, and validation evidence.

The recovery pattern became:

- recover the running BKC state from the swarm;
- re-check NFS and shared project folders;
- validate IPMI and iDRAC control;
- repair PXE boot paths;
- preserve known-good installer fragments;
- rebuild the hypervisor targets;
- run post-install service checks;
- feed the discoveries back into the pipelines.

The lesson was not that every step should be fully automated on the first try.
The lesson was that discovery should not disappear after the first success.

## Why this matters

Small infrastructure teams often build useful automation in fragments. One
engineer knows the PXE detail. Another remembers the BIOS setting. A third knows
which post-install command actually made the web UI behave.

BlackKnightController turns those fragments into a visible operational path.

The goal is not to hide the work. The goal is to make the work repeatable,
explainable, and safe to run again when the lab grows from two servers to a
room full of machines.

## Current state

The current lab pattern can provision bare metal, bring hypervisors online,
seed workloads, expose services through an edge layer, and show the living
environment in a resource graph.

That is the real product story: not a perfect demo, but a system that can learn
from the messy demo and become more reliable on the next run.

## The timeline that made the pattern real

The weekend did not start as a polished demo. It started with a corrupted
workstation, a running BlackKnightController stack in Docker Swarm, an NFS share
on ns1, and enough recovered project state to ask a better question: could the
control plane help reconstruct the work instead of depending on one developer
machine?

The answer was yes, but not magically. The useful sequence looked more like
field engineering than a brochure:

```text
recover reachable BKC
  -> verify swarm, ns1, and NFS state
  -> reconnect SSH keys and Docker contexts
  -> compare local repo state to running services
  -> rebuild bare-metal pipelines
  -> validate hypervisor installs
  -> publish the findings back into articles, fragments, and screenshots
```

That loop is the product. BlackKnightController is not valuable because it
claims to automate everything. It is valuable because the system gives the
operator somewhere durable to put the truth once the truth is discovered.

## What changed after the first painful run

The recovery weekend also exposed a failure mode that traditional scripts do
not solve by themselves: the automation can drift away from the known-good
manual fix. A server can PXE boot correctly, install packages, and still fail
to boot from disk because a firmware mode or boot order assumption changed.

The corrected BlackKnight pattern became stricter:

- every destructive installer gets a declared target;
- BIOS or UEFI mode is treated as input evidence, not background trivia;
- PXE one-shot state is disarmed before local-disk boot validation;
- first boot is not assumed until SSH identity and deployment markers match;
- known-good fragments are named, preserved, and reused instead of casually
  rewritten during a troubleshooting spiral.

That sounds mundane, but it is the difference between a fun lab trick and a
repeatable operations platform.

## Why this belongs on the public site

Most infrastructure products show the happy path. This story matters because
it shows the scar tissue: damaged workstation state, rebuilt SSH paths, edge
networking, hypervisor installs, Grafana views, Portainer checks, Horizon
sessions, ESXi compromises, and the eventual move from “we did it once” to “we
can do it again.”

That is a better trust signal than a perfect animation. The lab proved that BKC
can help an operator recover context, rebuild targets, preserve findings, and
turn the whole weekend into reusable material for the next engineer.
