# Auzietek public site strategy

Status: working direction for `beta.auzietek.com`, then `www.auzietek.com`

## Goal

`www.auzietek.com` should become the business-oriented front door:

- clear infrastructure automation services
- right-fit client positioning
- polished BlackKnightController product direction
- practical proof from real lab work
- tutorials that teach young engineers without sounding casual or scattered

The site should feel like Auzietek put on a suit without losing the field notes,
evidence, and practical engineering spirit that make the work credible.

## Domain lanes

Keep the public map intentionally small.

```text
www.auzietek.com / auzietek.com
  Main business site, services, product positioning, selected articles

beta.auzietek.com
  Public preview and promotion lane before production cutover

blackknight.auzietek.com
  BlackKnightController product journal, demos, pipeline evidence, operator patterns

linux-users.auzietek.com
  Teaching lane for newer Linux and infrastructure engineers
```

Future domains should prove a distinct audience before they are added. Avoid
creating a domain for every tag.

## Content posture

Write like an experienced engineer teaching someone smart who is still building
their operational instincts.

Prefer:

- problem first
- why it matters
- the evidence observed
- the repeatable pattern
- the safe next step

Avoid:

- inside jokes in primary business copy
- vague hype
- unexplained acronyms
- raw lab chatter without a takeaway
- posts that are only “what I did today” unless they teach a reusable pattern

Personality is welcome in articles, but the landing pages should make the value
plain within a few seconds.

## SEO structure

Each public article should have:

- one clear title that names the problem or outcome
- a concise summary that can stand alone in search results
- tags that match the domain lane and topic
- source URL or legacy URL notes when migrated
- updated canonical URL after promotion

Recommended tag families:

```text
services
blackknightcontroller
linux
lab
openstack
proxmox
vmware
docker-swarm
monitoring
security
managed-operations
```

## Promotion model

```text
lab evidence
  -> imported draft
  -> edited beta article
  -> accepted Kanboard card
  -> public beta
  -> production promotion
```

Drafts can be rough. Public beta should be readable. Production should be
client-safe, search-safe, and linked from the correct domain lane.

## Site configuration

The app supports one codebase with per-deployment public posture:

```text
SITE_NAME
SITE_BRAND
SITE_SECTION
SITE_HEADLINE
SITE_DESCRIPTION
SITE_POSITIONING
SITE_AUDIENCE
SITE_NAV_LINKS_JSON
MICROSITES_JSON
```

Use these instead of forking templates for each public face. Fork only when a
domain has a genuinely different product experience.

