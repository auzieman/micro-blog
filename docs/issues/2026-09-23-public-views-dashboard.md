# Public site views dashboard from existing Prom

- date: 2026-09-23
- requested_by: Auzie
- risk: public-telemetry
- kanboard: sync pending

## Scope

Grafana board on `mon.auzietek.com` from live `blog_page_views_total`
(already scraped at `microblog-otel:9464`). Analytics-shaped: domain,
page, region, view counts.

Hits, not distinct viewers. No raw IP. Do not use `nginx_public_*`
(exporter target is down and leaking on 165). Uniques stay on the later
card. Country/region will read `unknown` until `GEOIP_LOOKUP_URL` is set.

## Plan

1. Add `collector/microblog-public-analytics-dashboard.json`.
2. File-provision it into swarm Grafana on 165 (empty provisioning dir).
3. Leave blog-ui / Drupal / VM145 / HDD untouched.

## Acceptance

`mon.auzietek.com` shows a Micro Blog folder dashboard with views by
host and route. No IP column.
