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
