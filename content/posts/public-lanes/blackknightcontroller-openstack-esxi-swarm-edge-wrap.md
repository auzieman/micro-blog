---
title: OpenStack, ESXi, Swarms, and the Edge: One Lab, Several Control Planes
slug: openstack-esxi-swarms-and-the-edge
summary: A BlackKnightController lab wrap-up showing how OpenStack, ESXi, Docker Swarm, Portainer, Grafana, and edge routing become one operator-facing fabric.
tags: [blackknightcontroller, openstack, esxi, docker-swarm, edge, grafana, portainer]
theme_variant: midnight
status: published
seo_title: BlackKnightController OpenStack ESXi Docker Swarm edge lab
seo_description: How BlackKnightController connected OpenStack, ESXi, Docker Swarm, Portainer, Grafana, and edge routing into one repeatable lab operating fabric.
hero_image_url: /content-files/assets/bkc/bkc-pipeline-paths-graph.png
---

The recent lab grew past a single hypervisor story.

Server1 became the OpenStack side. Server2 became the ESXi side. Proxmox stayed
near the edge. Docker Swarm workloads moved into the new platforms. Portainer,
Grafana, Horizon, edge routing, DNS, and SSH jump paths all had to keep making
sense from the operator workstation.

That is where BlackKnightController starts to feel different from a pile of
scripts.

![BlackKnightController pipeline paths graph](/content-files/assets/bkc/bkc-pipeline-paths-graph.png)

## The split platform pattern

The final demo shape looked like this:

```text
OpenStack lane
  -> build tenant VMs
  -> bootstrap Docker Swarm
  -> validate SSH, Docker, and OpenStack server state

ESXi lane
  -> clone known-good Debian base guests
  -> pin DHCP/MAC identity
  -> bootstrap Docker Swarm
  -> expose Portainer agent path through the lab edge

Edge lane
  -> keep workstation-friendly URLs
  -> route web UIs
  -> preserve Docker context / ProxyJump notes
  -> keep Grafana and Portainer useful as operator views
```

The point is not that every platform should run the same way. The point is that
the operator should not have to mentally reload the entire environment every
time the target changes.

## Known proof from the ESXi side

The ESXi side reached a very concrete state:

```text
ESXi swarm manager: 10.20.0.121
Docker context route: ssh://bkc-esxi-swarm-mgr-01 via ProxyJump bkc-edge
Portainer agent route: swarm1.lab.auzietek.com:19091 -> 10.20.0.121:9001
```

That is the kind of detail worth preserving. It tells the next operator where
the service actually lives, how the workstation reaches it, and which edge port
is translating the lab-only network into something usable.

## OpenStack needed a different boundary

The OpenStack control plane was healthy, but tenant SSH routing still needed the
final management route or floating-IP decision before it should be presented as
a workstation-native Docker context.

That is a good example of BKC honesty. Do not call the entire story done because
one layer is green. Mark the exact boundary:

```text
control plane healthy
tenant workload created
service-side route pending
workstation-native context not yet promoted
```

That lets the next run start from evidence instead of vibes.

## Why the edge matters

The workstation normally cannot route directly to every `10.20.0.0/24` service.
That is intentional. The lab edge exists to present useful entry points without
pretending the private network is flat.

Web UIs go through edge routes. Docker CLI contexts use SSH aliases and
ProxyJump. Portainer uses agent endpoints, not raw Docker TCP. Grafana becomes
the shared “what is alive?” view.

That is the practical fabric BKC is building:

```text
different platforms -> consistent operator paths -> validated evidence
```

The lab is not replacing every tool. It is connecting them into a shape an
engineer can understand, repeat, and teach.
