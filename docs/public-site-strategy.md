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

retro-users.auzietek.com
  Amiga, retro systems, preservation, and classic-computing experiments
```

Future domains should prove a distinct audience before they are added. Avoid
creating a domain for every tag.

## Content posture

Write like an experienced engineer teaching someone smart who is still building
their operational instincts.

The desired voice is:

```text
serious professor
  + practical field engineer
  + just enough mad-science curiosity to stay memorable
```

Older funny Linux notes, valley-talk, late-night lab banter, and rough forum-like
phrasing should be treated as source material, not final public copy. Keep the
useful lesson and the human spark. Remove wording that would distract a young
engineer, a potential MSP client, or a future product/funding conversation.

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
- slang-heavy troubleshooting language in promoted public articles

Personality is welcome in articles, but the landing pages should make the value
plain within a few seconds.

## Article format: technical magazine, not raw notebook

The blog-like lanes should feel closer to good classic computing magazines than
to an unedited lab diary.

The article should carry the story:

- what problem we were solving;
- why the reader should care;
- what the important moving parts are;
- what worked;
- what failed or surprised us;
- what a careful engineer would do next.

The repo should carry the runnable artifacts:

- complete scripts;
- full playbooks;
- pipeline JSON;
- templates;
- example inventories;
- screenshots and diagrams;
- validation commands;
- issue notes.

Long command blocks and giant configuration listings should usually become a
highlight plus a GitHub link instead of dominating the article. The article can
show the important excerpt, then point to the full runnable version.

Good public article shape:

```text
headline
short standfirst / summary
why this matters
architecture or mental model
key excerpt
what happened in the lab
full example on GitHub
lessons / caveats
next step
```

Use callouts for:

- “Known good fragment”
- “Watch out”
- “Full repo example”
- “Manual version”
- “BKC pipeline version”
- “Ansible/Puppet companion”

That keeps the pages readable on phones while still preserving depth for
engineers who want the whole kit.

Before promoting content, use `docs/prompts/public-content-proof.md` as the
autoprompt reminder. The key rule is simple: if real screenshots, diagrams,
terminal captures, validation output, or GitHub artifacts exist, include or link
them. Do not let polished prose replace proof.

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
retro
amiga
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
  -> companion repo/tutorial kit when useful
  -> accepted Kanboard card
  -> public beta
  -> production promotion
```

Drafts can be rough. Public beta should be readable. Production should be
client-safe, search-safe, and linked from the correct domain lane.

## Legacy Auzietek transition

Keep the existing Drupal-era `auzietek.com` content available during the beta
transition instead of treating cutover as an instant replacement.

Recommended posture:

```text
current auzietek.com / www.auzietek.com
  legacy production until beta content is reviewed and promoted

legacy.auzietek.com or /legacy
  read-only archive after cutover, with redirects from important old URLs

beta.auzietek.com
  new business site and content model under review
```

Useful older articles should be migrated into the correct lane, polished, and
linked back to their source when that helps continuity. Less useful historical
content can remain in the legacy archive rather than being forced into the new
site structure.

Older technical examples should also be refreshed when the ecosystem moved on.
For example, Puppet articles should preserve the original operational lesson
while pointing readers toward current open-source Puppet/OpenVox-style tooling,
modern Debian/Fedora assumptions, and a companion repo with current examples.

During the transition, static business pages should behave like pages, not blog
indexes. Blog/article browsing belongs under `/blog`, lane domains, or specific
article routes.

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
