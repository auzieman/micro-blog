---
title: RX-Demo Part 1: Building Observability Into a Cloud-Native App
slug: rx-demo-part-1-cloud-native-observability
summary: RX-Demo is a practical test app for showing how logs, metrics, traces, containers, and deployment pipelines fit together.
tags: [linux, containers, observability, docker, monitoring, teaching]
theme_variant: linux-pro
status: published
seo_title: RX-Demo observability walkthrough for Linux and containers
seo_description: Learn how a small cloud-native demo app can teach logs, metrics, traces, Docker deployment, and practical observability.
canonical_url: https://auzietek.com/node/44
---

Good observability starts before the outage.

RX-Demo exists as a practical teaching app: small enough to understand, but
real enough to show the relationship between application behavior, containers,
logs, metrics, traces, and dashboards.

The point is not to bolt monitoring onto a finished system as decoration. The
point is to build a service that can explain itself while it runs.

## What a useful demo app should show

A good observability demo should make the path visible:

- a request enters the app;
- the app performs work;
- logs describe meaningful events;
- metrics count rates, latency, and errors;
- traces connect steps across services;
- dashboards answer operator questions.

If a demo cannot help a newer engineer understand that path, it is probably too
abstract.

## Containers make the lesson portable

Docker Compose and Docker Swarm are useful teaching targets because they expose
the core ideas without requiring a full Kubernetes conversation first:

```bash
docker compose up -d
docker compose logs -f
docker stack services demo
docker service ps demo_web
```

Those commands are approachable. They also map cleanly to larger systems later.

## Why it belongs in the Linux lane

RX-Demo is not just an application story. It is a Linux operations story:
processes, ports, container images, environment variables, logs, health checks,
storage, and dashboards all meet in one place.

That makes it a good bridge between “I can run Linux commands” and “I can
operate a service.”

Legacy source: [Auzietek RX-Demo article](https://auzietek.com/node/44).
