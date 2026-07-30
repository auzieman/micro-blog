---
title: Lab Pipelines as Product Proof
slug: lab-pipelines-as-product-proof
summary: The recent Auzietek lab sequence used BlackKnightController to rebuild bare metal, deploy hypervisors, seed workloads, expose services, and capture evidence as repeatable pipeline material.
tags: [services, blackknightcontroller, pipelines, lab, aiops, openstack, esxi, docker-swarm]
theme_variant: auzietek
status: published
seo_title: Auzietek BlackKnightController lab pipelines as product proof
seo_description: How Auzietek uses BlackKnightController lab pipelines, screenshots, validation, and public articles to prove repeatable infrastructure automation instead of merely describing it.
hero_image_url: /content-files/assets/bkc/bkc-pipeline-paths-graph.png
---

Auzietek is not trying to sell automation as a slogan.

The useful proof is the working loop: take ordinary infrastructure tasks,
capture the steps, validate the result, preserve the fragments, and make the
next run easier to explain.

That is what the recent BlackKnightController lab sequence demonstrated.

![BlackKnightController pipeline paths graph](/content-files/assets/bkc/bkc-pipeline-paths-graph.png)

## The sequence we proved

The lab work was deliberately grounded in real hardware and real services:

- power control through iDRAC/IPMI;
- PXE boot paths for destructive bare-metal rebuilds;
- Debian Trixie base provisioning;
- OpenStack lab bring-up on one server;
- Proxmox and ESXi lanes on another server;
- Docker Swarm workload seeding;
- Portainer, Grafana, Horizon, OpenWebUI, and edge routes;
- screenshot capture and public documentation updates.

That is a lot of surface area, but each step is normal IT work. The product
idea is not that BlackKnightController invents a new universe. The product idea
is that it keeps these actions, relationships, and proofs close enough together
that the next operator does not have to rediscover the whole story.

## Why screenshots matter

Screenshots are not decoration here. They are field evidence.

![BlackKnightController Company Mind resource workbench](/content-files/assets/bkc/bkc-company-mind-resources.png)

The Company Mind view shows resources and relationships as an operating graph:
hypervisors, swarms, networks, services, pipelines, and edge paths. That makes
the lab readable at a glance, and it gives a human/operator pair a shared map to
reason from.

![BlackKnightController pipeline workbench](/content-files/assets/bkc/bkc-pipeline-workbench.png)

The pipeline workbench shows the other half: repeatable action. Discovery work
does not count as finished until the useful command, template, API call, or
validation check lands back in a pipeline.

## The business meaning

A small team usually does not fail because nobody can run a command. It fails
because the context is scattered:

- the BIOS setting is in one person’s memory;
- the DHCP exception is in an old shell session;
- the proxy rule was tested but never documented;
- the VM seed step worked once but was not promoted;
- the dashboard exists but is not tied to deployment evidence.

BlackKnightController is aimed at that gap.

The conservative business case is straightforward: reduce rediscovery, reduce
rework, shorten rebuilds, preserve operational knowledge, and make client
environments easier to teach, migrate, and support.

## What comes next

The next public pass should keep expanding the magazine-and-repo pattern:

```text
validated lab pipeline
  -> public article
  -> screenshot evidence
  -> GitHub guide or companion kit
  -> repeatable service offer
```

That is the Auzietek way: not “you could do this,” but “we did this, here is the
proof, here is the lesson, and here is the path for doing it again.”

Related:
[BlackKnightController pipeline articles](https://blackknight.auzietek.com/blog),
[Auzietek Linux Lab Guides](https://github.com/auzieman/auzietek-linux-lab-guides).
