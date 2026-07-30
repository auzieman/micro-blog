---
title: Ansible, Docker, and a Small Monitoring Sandbox
slug: ansible-docker-grafana-monitoring-sandbox
summary: A practical lab pattern for using Ansible to prepare Docker, Grafana, OpenTSDB-style services, persistent paths, and repeatable monitoring experiments.
tags: [linux, ansible, docker, grafana, monitoring, opentsdb, teaching]
theme_variant: linux-pro
status: published
seo_title: Ansible Docker Grafana monitoring sandbox
seo_description: Build a small monitoring sandbox with Ansible, Docker, Grafana-style persistent paths, and repeatable service setup.
canonical_url: https://auzietek.com/node/17
---

Sometimes the fastest way to learn a stack is to build a sandbox that includes
just enough moving pieces to feel real.

The legacy Auzietek article combined Ansible, Docker, OpenTSDB, Grafana, and
host preparation in one sitting. It was not trying to be a perfect production
runbook. It was a working sketch for repeatable experimentation.

That distinction matters. A good sandbox should be:

- fast to rebuild;
- explicit about paths;
- easy to inspect;
- safe to break;
- close enough to production that the lessons transfer.

## Prepare persistent paths

The first useful Ansible task is usually not glamorous. It creates the folders
that containers will later mount.

```yaml
---
- hosts: all
  become: true
  vars:
    auto_create_metrics: true
    cors_domains: "*"
  tasks:
    - name: Creates directories
      file:
        path: "{{ item }}"
        state: directory
      with_items:
        - /srv/grafana/grafana_data
        - /srv/grafana/grafana_provisioning
        - /srv/mysqlgrafana
```

That small block says a lot: persistent state belongs somewhere obvious, and
automation should create it before the container tries to use it.

## Seed containers and copy defaults

The older workflow pulled images, started temporary containers, and copied
default configuration out to `/srv`.

```yaml
- name: Pull Grafana
  ansible.builtin.shell:
    cmd: docker pull grafana/grafana-oss

- name: Run a temporary Grafana container
  ansible.builtin.shell:
    cmd: docker run -td --name grafana grafana/grafana-oss

- name: Populate local Grafana data
  ansible.builtin.shell:
    cmd: docker cp grafana:/var/lib/grafana /srv/grafana/grafana_data

- name: Populate Grafana provisioning files
  ansible.builtin.shell:
    cmd: docker cp grafana:/etc/grafana/provisioning /srv/grafana/grafana_provisioning
```

Modern Compose images often document their mount points better, and many shops
would use bind mounts or named volumes from the start. The lesson still holds:
make container state inspectable and recoverable.

## Why Ansible still helps

Even when Docker handles service runtime, Ansible remains useful for:

- installing Docker and dependencies;
- creating users and groups;
- preparing directories;
- dropping environment files;
- writing firewall rules;
- copying compose files;
- validating that ports respond.

That is exactly where BKC pipelines often meet traditional Linux work. The
pipeline owns the operator story, while shell, Python, Compose, or Ansible do
the local task they are good at.

## A modernized shape

The same lab today might look like:

```text
inventory.yml
prepare-host.yml
docker-compose.yml
.env
dashboards/
provisioning/
validate.sh
```

And the validation should be part of the lesson:

```bash
docker compose ps
curl -fsS http://localhost:3000/api/health
curl -fsS http://localhost:4242/api/version
```

The value is not merely that Grafana starts. The value is that the setup can be
repeated by another engineer without decoding your shell history.
