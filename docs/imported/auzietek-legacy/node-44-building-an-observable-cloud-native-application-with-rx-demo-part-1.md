---
title: "Building an Observable Cloud-Native Application with RX-Demo Part 1"
slug: "building-an-observable-cloud-native-application-with-rx-demo-part-1"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/44"
source_id: "node-44"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "https://auzietek.com/sites/default/files/inline-images/drupal-article-image-01-62cab407be.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-44/image-01-62cab407be1b.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/drupal-article-image-02-030e55ee6c.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-44/image-02-030e55ee6c2a.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/drupal-article-image-03-cc25b0284d.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-44/image-03-cc25b0284d9e.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/drupal-article-image-04-2cbb5e59ed.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-44/image-04-2cbb5e59ed70.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/drupal-article-image-05-65aec46119.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-44/image-05-65aec4611977.png"
---

## Project Walkthrough

One of the recurring themes across AuzieTek projects is that applications should be designed for operations from the very beginning. Logging, metrics, tracing, health reporting, messaging, and dashboards should not be afterthoughts added after deployment. They should be woven into the application itself.

RX-Demo is a reference implementation demonstrating these concepts using ASP.NET, RabbitMQ, SQL Server, Redis, OpenTelemetry, Grafana, Loki, Tempo, Prometheus, Docker Compose, and Kubernetes.

![UI1](../images/legacy-auzietek/node-44/image-01-62cab407be1b.png)

This project is intentionally larger than a simple CRUD sample. It models a prescription workflow using event-driven microservices while exposing nearly every layer of the application through structured telemetry.

## Architecture Overview

RX-Demo demonstrates a complete event-driven workflow rather than a traditional monolithic application.

- ASP.NET API Gateway
- RabbitMQ command and event bus
- Background worker services
- SQL Server persistence
- Redis projection cache
- OpenTelemetry Collector
- Prometheus metrics
- Loki structured logging
- Tempo distributed tracing
- Grafana dashboards

![architecture](../images/legacy-auzietek/node-44/image-02-030e55ee6c2a.png)

Each component generates telemetry as work flows through the system, allowing developers to follow a single prescription request from the browser all the way through API calls, queued messages, database updates, event publication, cache projection, and response.

## Structured JSON Logging

Traditional application logging often produces long text files that require manual searching. RX-Demo instead emits structured JSON logs where each field can be indexed, filtered, queried, and correlated.

```
{
  "PrescriptionId": 12345,
  "Patient": "*****",
  "Status": "Approved",
  "ElapsedMs": 34,
  "CorrelationId": "..."
}
```

Structured logging allows Grafana, Loki, and Elastic-style platforms to search large volumes of events quickly while preserving important business and operational context.

## Metrics Designed for Operations

Every service exports Prometheus metrics including request counts, processing latency, queue depth, worker activity, and application health. These metrics become the foundation for dashboards, alerting, and deployment validation.

Rather than asking, "Is the server running?", operators can answer better questions:

- Which service is slowing down?
- How many prescriptions are queued?
- What percentage of approvals are failing?
- How long does an average refill take?
- Are the dashboards and telemetry endpoints actually usable?

## Distributed Tracing

Modern applications rarely execute inside a single process. A single user request may traverse APIs, queues, background workers, databases, caches, and external telemetry systems.

OpenTelemetry traces allow developers to visualize every hop in a single timeline, dramatically reducing troubleshooting time during production incidents.

## Cloud Events and Event-Driven Design

Rather than tightly coupling services together, RX-Demo demonstrates asynchronous communication through RabbitMQ. Commands initiate work, while events describe completed business actions. This separation allows additional services to subscribe without modifying the original application.

That same event stream could later feed analytics, auditing, machine learning, compliance review, or external integrations without rewriting the core business logic.

## Executive Health Dashboards

One goal of RX-Demo was to bridge the gap between engineering telemetry and business visibility. Instead of presenting thousands of raw metrics, the project calculates weighted component health that can be displayed as simple executive dashboards.

![metrics1](../images/legacy-auzietek/node-44/image-03-cc25b0284d9e.png)

This approach makes platform health understandable for developers, operations teams, and management alike.

![metrics2](../images/legacy-auzietek/node-44/image-04-2cbb5e59ed70.png)

## Designed for Containers

RX-Demo was built with container-first deployment in mind. The project supports local Docker Compose environments while also providing Kubernetes manifests suitable for cloud, lab, or on-prem clusters.

- Docker Compose
- Kubernetes
- OpenTelemetry Collector
- Prometheus
- Grafana
- Loki
- Tempo
- RabbitMQ
- SQL Server
- Redis

Developers can begin locally and later migrate the same architecture into Kubernetes with minimal conceptual changes.

## Why Build This?

As both an SRE architect and software developer, I have spent years watching organizations struggle to add observability after applications have already reached production.

RX-Demo explores the opposite philosophy: design applications so telemetry becomes a first-class feature. If every request produces meaningful logs, metrics, and traces, operations teams gain immediate visibility while developers gain dramatically better troubleshooting tools.

The result is software that is easier to maintain, easier to monitor, and far easier to evolve over time.

## Explore the Project

The complete source code, deployment manifests, and documentation are available on GitHub.

[View RX-Demo on GitHub →](https://github.com/auzieman/rx-demo)

## Where This Leads: BlackKnight Controller

RX-Demo is more than a demonstration application. It has become one of the reference workloads used during development of BlackKnight Controller, AuzieTek's infrastructure automation and orchestration platform.

Traditional deployment pipelines typically stop after a successful build, unit tests, and deployment. BlackKnight extends that workflow by validating that the deployed application is actually healthy, observable, reachable, and ready for developers to use.

After deployment, the controller verifies services using the same telemetry that operators depend on every day:

- Container rollout status through the Kubernetes API
- Application readiness and health endpoints
- OpenTelemetry metric availability
- Prometheus scrape validation
- Loki structured log verification
- Grafana dashboard provisioning
- Dashboard plugin validation
- RabbitMQ messaging and workflow validation
- Published access links for developers and testers

Rather than simply reporting that a deployment completed successfully, BlackKnight asks a more useful question:

> Can an engineer actually use this application right now?

Only after the platform verifies telemetry, dashboards, metrics, application endpoints, and supporting infrastructure does the deployment receive a successful operational status.

### Example Pipeline Events

```
✓ Updating source from Git

✓ Applying Kubernetes manifests

✓ Waiting for Grafana, Prometheus,
  Loki, and Tempo

✓ Telemetry endpoints responded

✓ Grafana plugin validation passed

✓ Loki query prepared

✓ Demo access links published

✓ Pipeline completed
```

These events are generated by the deployment pipeline itself. Rather than waiting for a human to verify the deployment, the controller confirms that monitoring components, dashboards, metrics, logs, and application endpoints are functioning before declaring success.

![bkc-pipeline-steps](../images/legacy-auzietek/node-44/image-05-65aec4611977.png)

In the RX-Demo pipeline, BlackKnight did more than apply manifests. It checked Kubernetes rollout events, verified telemetry endpoints, confirmed Grafana routing, validated the expected dashboard plugin, prepared a Loki Explore query for CloudEvent audit logs, and published ready-to-use access links for the application and observability tools.

## Beyond Deployment

Many automation platforms focus on getting software into production. BlackKnight is being designed to focus on keeping it healthy once it arrives.

That means combining infrastructure automation with runtime validation, observability, telemetry analysis, container status, deployment events, and eventually AI-assisted operational reasoning.

The goal is not simply continuous deployment. It is continuous operational confidence.

This shifts the pipeline from deployment automation to operational automation. The pipeline is not merely delivering software. It is assembling an environment, validating that the observability ecosystem is intact, and handing developers a workspace that is immediately ready for investigation and use.

That is one of the most distinctive aspects of BlackKnight Controller: it treats logs, metrics, traces, dashboards, APIs, and runtime state as part of the delivery contract.

Blog tags

[codex](/taxonomy/term/57)

[ASPNet](/taxonomy/term/60)

[Opentelemetry](/taxonomy/term/20)

[Python](/taxonomy/term/21)

[AI](/taxonomy/term/22)

Submitted by auzieman
 on Mon, 07/06/2026 - 18:30

