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

The lab hostnames should pin to the same lane behavior without query strings:

```text
microblog.lab.auzietek.com     -> auzietek lane
auzietek.lab.auzietek.com      -> auzietek lane
blackknight.lab.auzietek.com   -> blackknight lane
linux-users.lab.auzietek.com   -> linux lane
retro-users.lab.auzietek.com   -> retro lane
```

Later, production/beta edge DNS can pin public domains to the same code path.

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

## Legacy content migration

Treat the old Drupal site as source material, not the final public voice.

Current lab import posture:

```text
legacy auzietek.com article
  -> public crawl import
  -> draft article
  -> lane assignment
  -> image localization where possible
  -> editorial rewrite
  -> public beta
  -> production promotion
```

The first lab migration targets are intentionally narrow:

```text
linux-users.lab.auzietek.com
  Linux, Docker, Kubernetes, monitoring, Ansible, PXE, shell, and operations
  articles. Keep the clean light theme; it reads like a teaching bench.

retro-users.lab.auzietek.com
  AmigaOS, retro tools, older repos, and classic-computing videos. Keep the
  retro theme playful but restrained: warm paper, dark panels, and small
  boing-ball-inspired accents rather than a toy UI.
```

Imported drafts should stay `draft` until reviewed. Before publishing:

- rewrite titles so they name the engineering lesson;
- remove casual filler or old valley-talk from the opening;
- keep the original human spark in the body when it helps the teaching;
- add a short summary suitable for search results;
- keep the source URL for traceability;
- verify images were localized under `/content-files/imports/assets/`;
- avoid inline `data:image/...` assets as hero images;
- add GitHub and YouTube links as useful evidence cards, not random link piles.

Good article shape:

```text
problem
why it matters
what we observed
repeatable pattern
safe next step
related repo/video
```

Do not bulk-publish migrated content. The migration pipeline should make review
easy; editorial promotion should stay deliberate.

Current lab mirror state:

```text
linux-pro
  5 imported legacy drafts
  2 curated published seed posts

retro
  3 imported legacy drafts
  1 curated published seed post
  retro workbench header staged under /content-files/assets/retro/
  retro YouTube playlist linked from the lane landing card
```

## Joint media push approval gate

Use the lab lanes as the review room before any public push.

Approval checklist:

- public lane page loads by hostname;
- imported posts are still drafts until explicitly accepted;
- selected posts have rewritten titles, summaries, and opening paragraphs;
- article body keeps the useful technical lesson and removes distracting old
  source-site phrasing;
- images load from local `/content-files/...` paths or an intentional external
  source;
- video links open in a new tab and support the article/lane story;
- GitHub links point to active repos or clearly marked archives;
- canonical/SEO fields are filled before production promotion;
- Kanboard or a promotion note records what was accepted.

For a public campaign, promote in this order:

```text
lab lane
  -> accepted draft
  -> beta.auzietek.com / microsite beta
  -> social/video post
  -> production domain
```

Do not point `auzietek.com` or `www.auzietek.com` at the new app until SSL,
redirects, backups, rollback, and content review are complete.

## Content mount note

The ESXi swarm lab canary currently uses node-local `/srv/micro-blog/content`
bind mounts. That works, but any generated or imported asset must either:

- be copied to every app worker that can run `blog-ui`; or
- move to shared storage, such as the ns1 NFS-backed content mount.

Prefer shared storage before production-style promotion. Node-local fan-out is
acceptable for lab canary work, but it should be captured in the deployment
receipt so future runs do not create 404s after Swarm reschedules a container.

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
blackknight.lab.auzietek.com -> lab edge / BlackKnight lane
linux-users.lab.auzietek.com -> lab edge / Linux teaching lane
retro-users.lab.auzietek.com -> lab edge / Retro lane
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
