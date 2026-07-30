---
title: Kubernetes Monitoring with Prometheus, Grafana, and MariaDB
slug: kubernetes-monitoring-prometheus-grafana-mariadb
summary: A preserved Kubernetes monitoring lab showing namespaces, persistent claims, Prometheus, Grafana, MariaDB, and practical validation.
tags: [linux, kubernetes, prometheus, grafana, mariadb, monitoring, teaching]
theme_variant: linux-pro
status: published
seo_title: Kubernetes monitoring with Prometheus Grafana MariaDB
seo_description: A practical Kubernetes monitoring lab using Prometheus, Grafana, MariaDB, persistent volumes, namespaces, and validation checks.
canonical_url: https://auzietek.com/node/40
---

Monitoring is the part of Kubernetes that turns “it deployed” into “we can
operate it.”

The legacy article used Prometheus, Grafana, and MariaDB as a teaching stack.
The tone was playful, but the architecture was serious enough to matter:

- Prometheus collects metrics.
- Grafana visualizes them.
- MariaDB gives an example stateful service.
- Kubernetes manifests describe the desired state.
- Persistent volume claims keep data from vanishing on pod restart.

## Start with a namespace and storage

Keep monitoring resources together:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: prometheus-pv-claim
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mariadb-pv-claim
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

Even in a lab, persistent claims are worth teaching early. They force the
operator to ask where data lives.

## Deploy Prometheus

Prometheus is the cluster’s metric collector. In a real deployment you would
usually use a chart or operator, but reading the YAML once is useful because it
shows what the abstraction eventually creates.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus
          ports:
            - containerPort: 9090
```

## Add Grafana

Grafana is the visual layer. It should be treated as a real service: persistent
storage, explicit admin credentials, and a clear ingress or service exposure
plan.

```bash
kubectl -n monitoring get pods
kubectl -n monitoring get svc
kubectl -n monitoring logs deploy/prometheus
```

## What to validate

Do not stop when pods are merely `Running`.

Validate:

- Prometheus target page has scrape targets.
- Grafana can reach Prometheus.
- dashboards load without datasource errors.
- storage survives pod restart.
- service URLs are reachable from the expected network.
- logs tell a useful story when something fails.

## Why this is still useful

Modern Kubernetes stacks often use Helm, kube-prometheus-stack, Loki, Tempo,
OpenTelemetry, and managed services. Those are excellent tools, but they can
hide the fundamentals.

This lab keeps the fundamentals visible:

```text
namespace
deployment
service
persistent volume claim
logs
metrics
dashboard
validation
```

That vocabulary transfers directly into larger environments.
