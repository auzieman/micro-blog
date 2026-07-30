# Legacy Auzietek content migration audit

This file keeps the public-lane migration grounded so we do not accidentally
compress useful history into short cards again.

## Source inventory sampled from auzietek.com

The legacy Drupal site exposes more useful material than the first public-lane
pass carried forward. Strong migration candidates:

- `/node/13` — ESXi backup notes using SSH and `vim-cmd`
- `/node/14` — practical open source Puppet setup
- `/node/15` — Linux `find` regex article
- `/node/17` — Ansible, Docker, OpenTSDB/Grafana sandbox
- `/node/18` — Dynatrace OneAgent Ansible install notes
- `/node/19` — OpenTelemetry, Ansible, and Dynatrace demo
- `/node/20` through `/node/27` — GPT-assisted programming/sysadmin articles
- `/node/28` — Dynatrace Managed SELinux/firewall requirements
- `/node/29` — Dynatrace OneAgent log-retention on AIX
- `/node/30` — Container Tamer
- `/node/31` — Docker/server naming patterns and Dynatrace autotagging
- `/node/32` — metadata for monitoring and automation
- `/node/33` — BlackKnightController introduction
- `/node/35` — AuziX / future computing essay
- `/node/36` and `/node/37` — RACS concepts
- `/node/38` — Docker Swarm walkthrough
- `/node/39` — K3s sandbox walkthrough
- `/node/40` — Kubernetes monitoring with Prometheus/Grafana/MariaDB
- `/node/41` through `/node/43` — AmiWriterMUI, AmiWriterReact, MuIRC
- `/node/44` — RX-Demo observability article

## Migration rules

- Listing cards should be concise; article bodies may be long.
- Preserve code blocks and terminal evidence where it teaches something.
- Localize owned screenshots and diagrams when practical.
- Avoid dragging giant base64 or ad/noise HTML into the repository.
- Prefer improved archival articles over short paraphrases.
- Keep source URLs in `canonical_url` when an article is derived from Drupal.

## Current first-wave repair

Expanded or added:

- Linux `find` regex
- RX-Demo observability
- Dynatrace Managed requirements
- containers outside big cloud
- Puppet setup
- ESXi backup notes
- Ansible/Docker/Grafana sandbox
- Docker Swarm walkthrough
- K3s sandbox walkthrough
- Kubernetes monitoring stack

Remaining high-value follow-up:

- Dynatrace OneAgent Ansible article
- OpenTelemetry/Dynatrace article
- GPT-assisted sysadmin/programming series
- Container Tamer
- Docker naming/autotagging
- metadata-driven monitoring/automation
- richer Amiga/Codex project pages
- RACS/AuziX long-form concept pages
