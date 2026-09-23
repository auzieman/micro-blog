# Nginx GeoIP into page-view counters

- date: 2026-09-23
- requested_by: Auzie
- risk: public-telemetry
- kanboard: sync pending

## Evidence

165 already has the free country DB (`/usr/share/GeoIP/GeoIP.dat`,
`geoip_country` in `nginx.conf`). `geoiplookup 8.8.8.8` → US.
Access logs already record `$geoip_country_code`.

`add_header X-Country-*` is a **response** header. Flask never sees it.
Blog-ui locations do not `proxy_set_header` the country.

## Plan

1. Flask reads `X-Country-Code` / `X-Country-Name` first, then
   `GEOIP_LOOKUP_URL`, then unknown. Origin IP is what nginx GeoIP
   already keys on (`$remote_addr`). No IP label on Prom.
2. Add `proxy_set_header X-Country-Code $geoip_country_code` and
   `X-Country-Name $geoip_country_name` on the three 18081 vhosts.
3. Deploy UI via edge BKC; nginx -t && reload via BKC. Backup confs.

## Rollback

Restore nginx backups; revert Flask to `geoip_for_ip` only.

## Acceptance

A public hit shows a real `country` on `blog_page_views_total`, not
only `unknown`. City not required (country DB only).
