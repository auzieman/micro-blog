---
title: "Swarm And Edge Wrap For Publish"
slug: "blackknightcontroller-swarm-and-edge-wrap-for-publish"
summary: "Repository Markdown staged for public article/story cleanup."
status: draft
source_type: repo_markdown
source_repo: "BlackKnightController"
source_path: "BlackKnightController/docs/video-swarm-edge-wrap.md"
source_id: "blackknightcontroller-docs-video-swarm-edge-wrap-md"
captured_at: "2026-07-29"
candidate_lane: "blackknightcontroller"
tags: [aiops, blackknight, docker, esxi, linux, needs-review, openstack, pipelines, repo-md]
assets:
  []
---

# Swarm And Edge Wrap For Publish

This is the wrap-up contract for the final two demo videos.

## Pipelines

- `40 VIDEO — OpenStack Docker Swarm Seed`
  - Creates the OpenStack-hosted Docker Swarm guests.
  - Bootstraps Docker Swarm inside OpenStack.
  - Uses OpenStack API plus BKC SSH.
  - Needs the OpenStack tenant SSH route checked before daily-use Docker context
    access.

- `50B CANDIDATE — ESXi Docker Swarm Seed`
  - Starts from known-good `bkc-trixie-base`.
  - Clones five ESXi guests.
  - Pins generated MACs in ns1 DHCP.
  - Installs Docker and validates a five-node Swarm.

## Edge Access

The workstation normally cannot route directly to `10.20.0.0/24`, so published
access must go through the lab edge:

- Web UIs go through `deploy/lab-edge`.
- Docker CLI contexts use SSH aliases with `ProxyJump`.
- Portainer uses Agent endpoints, not raw Docker TCP.

Known ESXi proof:

```text
ESXi swarm manager: 10.20.0.121
Docker context route: ssh://bkc-esxi-swarm-mgr-01 via ProxyJump bkc-edge
Portainer agent route: swarm1.lab.auzietek.com:19091 -> 10.20.0.121:9001
```

OpenStack proof is control-plane healthy, but tenant SSH routing still needs the
final management route/floating IP decision before it should be presented as a
workstation-native Docker context.
