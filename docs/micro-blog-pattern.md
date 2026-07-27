# Micro-blog deployment pattern

Status: working pattern for lab alpha, public beta, and later production cutover.

The pattern is intentionally small:

```text
one codebase
  -> one mounted content tree
  -> one deployment-local .env
  -> one public lane posture
  -> one edge/DNS name
  -> import or sync content
  -> validate public pages
```

This keeps `micro-blog` useful as both a real public publishing tool and a
repeatable BlackKnightController workload.

## Lanes

Use lanes to separate audience and deployment maturity.

```text
alpha
  lab dogfood
  microblog.lab.auzietek.com
  safe to rebuild

beta
  public preview
  beta.auzietek.com
  curated content before production cutover

production
  final public front door
  auzietek.com / www.auzietek.com
  promoted only after DNS, SSL, redirects, backups, and rollback are proven
```

Use microsite lanes to separate audience without forking the app:

```text
auzietek
  business front door
  services, proof, client-fit positioning

blackknight
  product journal
  BKC demos, pipeline evidence, hardware-as-code stories

linux
  teaching lane
  practical Linux, PXE, containers, monitoring, troubleshooting guides

retro
  retro lane
  classic systems, preservation, emulation, and engineering lessons
```

In the lab, these can be previewed with query parameters:

```text
/blog?lane=auzietek
/blog?lane=blackknight
/blog?lane=linux
/blog?lane=retro
```

Later, edge/DNS can pin domains to the same code path.

## Content tree

Do not require a full image rebuild for ordinary article changes.

Mount or sync a content directory into the deployment:

```text
/srv/micro-blog/content
  posts/
    public-lanes/
    linux/
  assets/
```

The current public-lane seed content lives in:

```text
content/posts/public-lanes/
```

Use filesystem bootstrap sync after content is mounted:

```http
POST /admin/bootstrap/filesystem-sync
```

The useful operating rule:

```text
code changes rebuild images
content changes sync/import content
```

## Environment contract

Each deployment gets its own `.env`; do not commit real `.env` files.

Important public posture variables:

```text
SITE_URL
SITE_NAME
SITE_BRAND
SITE_SECTION
SITE_HEADLINE
SITE_DESCRIPTION
SITE_POSITIONING
SITE_AUDIENCE
DEFAULT_THEME_VARIANT
SITE_NAV_LINKS_JSON
MICROSITES_JSON
```

Important admin/security variables:

```text
ADMIN_EMAIL
ADMIN_ACCESS_CODE
FLASK_SECRET_KEY
SESSION_COOKIE_SECURE
ENABLE_HSTS
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
GOOGLE_OAUTH_REDIRECT_URI
```

Keep `ADMIN_ACCESS_CODE` available as a deployment-local bootstrap route, but
set a real non-default value anywhere the UI is reachable beyond a private lab.

## DNS and edge

The DNS pattern mirrors the BKC lab naming work:

```text
hostname.lab.auzietek.com      canonical host identity
service.lab.auzietek.com       friendly edge/service alias
```

For alpha:

```text
microblog.lab.auzietek.com -> lab edge / ESXi swarm canary
```

For beta:

```text
beta.auzietek.com -> public preview deployment
```

For production:

```text
auzietek.com
www.auzietek.com
```

DNS updates should be applied through the provider API when possible and
validated as part of the deployment receipt.

## Deployment receipt

A deployment is not considered complete until these checks pass:

```text
container/service health
API /healthz
UI /healthz
public /blog
selected lane pages
featured post page
sitemap.xml
rss.xml
edge URL
DNS name when present
```

For the current lab canary, the important URLs are:

```text
http://swarm1.lab.auzietek.com:8091/blog?lane=auzietek
http://swarm1.lab.auzietek.com:8091/blog?lane=blackknight
http://swarm1.lab.auzietek.com:8091/blog?lane=linux
http://swarm1.lab.auzietek.com:8091/blog?lane=retro
```

## Housekeeping boundary

Keep deployment pattern work separate from cleanup work.

Pattern work:

```text
document the lane model
document content sync/import
document DNS/edge expectations
validate live behavior
```

Housekeeping work:

```text
remove stale configs
prune old images/artifacts
normalize old docs
clean unused nginx routes
archive migration captures
review backup/restore
```

Do not mix these unless a cleanup item blocks deployment validation.
