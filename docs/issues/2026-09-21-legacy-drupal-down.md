# Legacy Drupal public takedown

- status: planned
- requested_by: Auzie
- date: 2026-09-21
- kanboard: sync pending
- risk: service-routing
- target: `legacy.auzietek.com` on IONOS primary `74.208.45.165`

## Evidence

- `https://auzietek.com/` and `https://www.auzietek.com/` already serve the
  micro-blog.
- `https://legacy.auzietek.com/` still serves Drupal 9 (`auzietek_drupal`).
- Drupal `ads.txt` is `google.com, pub-1591072092053145, DIRECT, f08c47fec0942fa0`.
- Drupal HTML still fires Universal Analytics `UA-221476185-1`, not `GTM-TRK7JDZ`.

## Authorized change

Stop serving the old Drupal application publicly. Keep the files.

- nginx: `legacy.auzietek.com` returns 301/308 to `https://auzietek.com$request_uri`.
- Stop/scale down `auzietek_drupal` only.
- Keep `/srv/drupal`, `/svc/drupal_auzietek`, and MySQL volumes.
- Keep the DNS A record so the redirect vhost still answers.
- Do not delete the Drupal image, database, or archive backup.
- Do not touch Kanboard, Grafana, Prometheus, or micro-blog.
- Do not add GTM/AdSense to Drupal as part of this.

## Method

BKC only. No raw SSH mutate. Preferred sequence:

1. `auzietek-vps-backup-prepare` or an IONOS backup receipt covering nginx,
   Drupal files, and the Drupal database.
2. Bounded retire run: rewrite the legacy vhost, nginx test+reload, scale
   Drupal to 0.
3. Smoke: `legacy.auzietek.com` redirects; `auzietek.com`,
   `linux-users.auzietek.com`, and `www.blackknightcontroller.com` stay 200.

## Rollback

Restore the previous nginx vhost from the backup receipt and scale
`auzietek_drupal` back up. Data remains on disk.

## Acceptance

- `https://legacy.auzietek.com/` is not a live Drupal page.
- Canonical Auzietek/Linux/BKC hosts still 200.
- `/srv/drupal` still exists.
- BKC run id and receipt recorded on this ticket.
