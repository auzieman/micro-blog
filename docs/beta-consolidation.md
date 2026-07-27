# Beta consolidation runbook

Status: candidate path for `beta.auzietek.com`

## Environment lanes

```text
alpha
  -> microblog.lab.auzietek.com
  -> ESXi/lab swarm dogfood and feature validation

beta
  -> beta.auzietek.com
  -> public preview, curated content, corporate Auzietek theme

production
  -> auzietek.com / www.auzietek.com
  -> later cutover after redirects, SSL, backups, and rollback are proven
```

## Public domain split

The beta lane should prove a bounded public map before production cutover:

```text
www.auzietek.com
  main business site

beta.auzietek.com
  preview and promotion lane

blackknight.auzietek.com
  BlackKnightController product and field evidence

linux-users.auzietek.com
  practical teaching lane for Linux and infrastructure engineers

retro-users.auzietek.com
  Amiga, retro systems, preservation, and classic-computing experiments
```

Keep `www.auzietek.com` business-forward. Use the microsites to hold deeper
product journals, tutorials, and lab-heavy material so the main site does not
feel like a catch-all archive.

## Source material

Beta should be populated from a captured copy of the current Auzietek site:

```text
Drupal SQL dump
Drupal public/private files
nginx/vhost notes
legacy URL inventory
current public screenshots when useful
```

Keep raw SQL/files outside Git. Use Git for import scripts, manifests,
classification notes, and sanitized article fixtures.

## Import flow

1. Capture SQL/files from the existing Auzietek host.
2. Mount or copy the capture into the beta import workspace.
3. Preview Drupal JSON:API or public-crawl imports in `/admin`.
4. Import as drafts first.
5. Review, edit, classify, and publish selected articles.
6. Keep old URLs in `source_url` and canonical/redirect notes.
7. Promote only accepted Kanboard cards to public beta.

## Theme direction

Default public theme is `auzietek`.

It uses:

```text
left navigation rail
Auzietek brand at the top
breadcrumb trail over articles
margined content layout
cleaner corporate palette
dark code/evidence islands where useful
```

The older `midnight` theme remains available for lab/dogfood demos.

See `docs/public-site-strategy.md` for the writing voice, SEO posture, and
promotion rules.

## Admin bootstrap

Keep the deployment-local admin access feature:

```text
ADMIN_ACCESS_CODE=<deployment-local secret>
```

Google OAuth may be preferred for production, but local-code admin access is
still valuable for:

```text
isolated lab deployments
first-boot repair
controlled migration windows
single-admin emergency access
```

Never leave the default local code on an internet-reachable deployment.
