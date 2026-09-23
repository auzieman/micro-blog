# Auzietek GA4 stream G-CDGKSP6YER

- date: 2026-09-23
- requested_by: Auzie
- risk: public-runtime
- kanboard: sync pending
- source: follow-on to `d7b759c` / BlackKnight `G-SLDFDNQC98`

## Scope

Ship page gtag `G-CDGKSP6YER` on Auzietek public hosts (`auzietek.com`,
`www`, `beta`, Linux Users, Retro Users) next to `GTM-TRK7JDZ`. Do not
put this ID on BlackKnight. Lab and `legacy.auzietek.com` stay untagged.
No AdSense change.

## Plan

1. Default `GA_MEASUREMENT_ID_AUZIETEK=G-CDGKSP6YER`. Keep BlackKnight
   on `G-SLDFDNQC98`.
2. Commit and push `review/retro-users-experience-refresh-001`.
3. No-lab deploy: HTTPS update of `/var/tmp/bkc-micro-blog-src` on IONOS,
   local rsync into `/svc/micro-blog`, then edge BKC
   `auzietek-beta-preview-deploy` with `sync_source=false`.

## Rollback

Rebuild `blog-ui` from `d7b759c` / run `b9781a83-d670-460c-964f-b59726b5b6c4`.

## Acceptance

Live `auzietek.com` / Linux Users contain `gtag/js?id=G-CDGKSP6YER`.
BlackKnight still has only `G-SLDFDNQC98`. Drupal, VM145, HDD untouched.

## Execution log

(filled after the run)
