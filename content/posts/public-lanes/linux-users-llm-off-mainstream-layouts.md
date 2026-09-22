---
title: When the map is not FHS — LLMs, common patterns, and the bush problem
slug: when-the-map-is-not-fhs-llms-custom-layouts
summary: LLMs are not broken — they are dense on common patterns and thin on sparse ones. AlpinE and most of BKC ride the dense road; custom layouts, Amiga-shaped assigns, and house Jinja still look like bushes.
tags: [linux, ai, operations, packaging, teaching, alpine, bkc]
theme_variant: linux-pro
status: published
seo_title: LLMs, common patterns, and custom Linux layouts
seo_description: Why AI-assisted work succeeds on Alpine-shaped and pipeline-shaped systems, falters on sparse custom layouts and template discipline, and still produces syntactically correct one-offs that skip key features.
hero_image_url: /content-files/assets/linux/auzix-programs-nginx-volume-map.png
---

It is not that large language models do not work.

They are fantastic where the training distribution is dense: readable
`find` regexes, systemd skeletons, Grafana panels, Puppet or Ansible
shapes you already understand, Docker Compose, APK/abuild culture,
“trigger a pipeline and keep a receipt.” Ask for those and you often get
something you can review in minutes.

They fail — or more precisely, they *almost succeed while skipping the
point* — where the map is sparse. That failure looks a lot like a
self-driving car and a bush. The road-shaped world is in the prior. The
bush is not. The car does not “almost” understand the bush. It invents a
confident wrong action near the bush, then invents another one after the
first wrong action.

Both sides matter. Success on common patterns is real. Failure on sparse
ones is also real. The useful article is about that density, not a score
against the model.

A companion ThinkTank note,
[Imitated Intelligence](/post/imitated-intelligence-llms-as-smart-macros?lane=auzietek),
covers what an LLM actually is. This piece stays on the operator side.

## Success analogue: AlpinE on a mainstream spine

AlpinE is the bet that you can keep readable intent while sitting on a
living, mainstream package culture. Alpine recipes, abuild, apk, and the
enormous public corpus of “how do I build this on Alpine” pages are
exactly the kind of dense prior models already have.

That does not make AlpinE trivial. It does mean an assistant is not
fighting every DistroWiki page at once. The spine is familiar: fetch,
patch, build, package, install, prove. Readable shelves and product taste
can ride on top without requiring the model to invent a fifth `lib`
before breakfast.

When we say “AlpinE until the models catch up,” we are not saying AI is
useless. We are saying: put the bot on the road while the bushes are
still bushes.

## Mostly common: BlackKnightController

Somewhat strangely, BlackKnightController is mostly a common pattern too.

Pipelines, stages, receipts, git sync, lab hosts, compose/swarm, “do not
mutate production by raw SSH” — that is familiar automation fabric. Ask
a model to sketch a pipeline JSON, a health gate, or a content canary
that syncs Markdown and hits a bootstrap URL, and it will often land
something syntactically correct that actually runs.

![BlackKnightController pipeline workbench](/content-files/assets/bkc/openstack-bkc/bkc-pipelines.png)

That success is important. It is why AI-assisted lab work is worth doing
at all. BKC is not a private language invented to confuse bots. It is
boring CI/CD taste with a paper trail.

## Where BKC still looks like a bush

The uncommon bits are where the same assistant falters without making
the pipeline “broken.”

BKC wants structured transforms: environment, Jinja (or equivalent)
templates for scripts and configs, `bkc-cli` for trigger / send /
receipt, make the file executable through the known path. That is the
feature set.

What models are wired to do instead is the dense prior for “get it
done”:

- hardwire a host, path, or flag that should have been `env` or a
  template variable;
- drop a one-off shell fragment beside the pipeline instead of extending
  the template;
- run the mutating command directly over SSH even though `bkc-cli` and a
  paper trail already exist;
- craft a bespoke send/execute path, fail three or more times, then
  eventually get there.

The resulting pipeline is often still syntactically correct. Stages
complete. URLs respond. Receipts exist. What got skipped is the *key
feature*: parameterization, reusability, the boring structured transform
you already paid for.

That is the bush in a common-pattern product. Not a crash. A miss of the
point that still looks like a green run.

It is the same muscle as “I have `bkc-cli` and I still typed the docker
exec by hand.” Wired in. Eventually works. Leaves hardwired crumbs.

## Sparse bush: custom layouts and Amiga-shaped assigns

Custom Unix layouts that refuse Debian Policy as physics are a thicker
bush.

Readable volumes such as:

- `/Programs/<Name>/<version>/Commands` instead of a pile of `bin`/`sbin`
- `/Libraries` instead of another `lib`/`lib64` apology
- `/System/Settings` and `/System/State` instead of treating `etc`/`var`
  as the design

…are culturally old (Unix notes, Amiga, Atari, Mac) and statistically
rare in public text. Short names were a compiler and filesystem limit,
not the design. Putting a library on `/Libraries` is putting it back —
but almost every Stack Overflow answer will pull toward `/usr`.

![Readable guest volumes: nginx lives under /Programs, not another /usr apology](/content-files/assets/linux/auzix-programs-nginx-volume-map.png)

If a human checks out the tree, pulls a package, extracts the source,
runs a small set of path replacements, and packs the result, they can
nail a large fraction of a catalog with roughly eighty lines of honest
bash and a few weeks of boring checks. One map. Hard stop on the first
miss.

![Package control next to the Compatibility map — the contract a model keeps dropping](/content-files/assets/linux/auzix-package-control-compatibility.png)

Ask a model to own that intake across many packages and you get a
different failure class than BKC’s hardwired crumbs:

1. **Rewrite the builder host by accident.** Compatibility shebangs in
   `configure` so the Alpine builder cannot execute it.
2. **Report success on an empty shelf.** Continue-after-failure wrappers
   that look complete with zero APKs.
3. **Overlay guest libc onto the compiler.** Host `readlink` then loses
   `libcrypto`.
4. **Drop a known-good config for bare `defconfig`.** BusyBox TC comes
   back; `TCA_CBQ_MAX` looks like a kernel mystery and is not.
5. **Force target linker flags onto host link lines.** apk-tools cannot
   find `-lssl` even with `openssl-dev` installed for the builder.

None of that requires genius to avoid. It requires holding one boring
contract without inventing a fifth `lib` — the same discipline as
“use the template, use `bkc-cli`,” applied to a thinner prior.

Amiga-shaped work sits here too: `PROGDIR:`, `LIBS:`, `SYS:` are real
culture with a thin corpus. Models invent modern desktop metaphors
instead.

## Density, not morality

Mainstream Linux text is overwhelmingly FHS-shaped. Distro policy,
aports, Debian helpers, and “don’t invent an OS” advice all pull toward
`/usr` and `/lib`. Alpine build culture is dense. Generic pipeline YAML
is dense. Your house Jinja-for-scripts convention and your custom volume
map are sparse.

The model’s prior is not evil. It is statistical. When uncertain, it
falls back to the dense prior: hardwired paths, Compatibility as a
rewrite target, one-off SSH, another package-specific exception.

Mortals leave the mainstream too far without a ledger and get the same
disease: orphaned logic, lost logic, hardwired one-offs. Chat memory is
not a ledger. A green unit test is not a packed APK. A live patch is not
a fresh image proof. A green pipeline stage is not proof you used the
structured transform.

## What LLMs are good for here

They are good at:

- drafting the eighty-line sketch once a human has named the map;
- riding AlpinE / Alpine-shaped build loops;
- sketching BKC-shaped pipelines that sync, prove, and leave receipts;
- writing tests that assert the map or the template contract;
- summarizing a failed run from a receipt;
- proposing a *bounded* next check;
- turning a finished lab note into a Linux Users teaching article.

They are not yet good at owning sparse contracts — custom volume maps,
Amiga assigns, or “always Jinja + env + `bkc-cli`” — across a long
session without adult supervision. They will still produce working
one-offs that skip the feature you cared about.

## Operator rules that match the density

- Prefer a mainstream spine (AlpinE / Alpine culture) when the bot must
  move volume.
- Write sparse contracts *outside* the chat: volumes, template vars,
  fail-closed pack, `bkc-cli` paper trail.
- Treat green syntax as necessary, not sufficient. Ask whether the key
  feature ran (template, env, receipt) or only a one-off.
- First miss stops the list. Empty repositories are not complete.
- Prefer reusing a known config or finished binary over reinventing
  configure flags from vibes.
- When the next failure is another reinvented corner, stop. That is
  evidence about density, not a dare to grind `rN` forever.

## Honesty about the maybe

Custom path intake with a strict empty-root proof can remain a
**maybe**. Pieces can prove without an agent-complete catalog.

AlpinE can remain the working bet. BKC can remain mostly road, with
honest notes where Jinja and `bkc-cli` still lose to wired-in habits.

That is not an anti-AI sermon. It is a portability note for people who
build real systems with these tools.

Self-driving cars will get better at bushes. Models will get better at
sparse layouts and house template discipline. Until then, the prize is
not breaking the LLM. The prize is matching the job to the density —
eighty lines of bash and a map you can still read when the chat is gone,
or a mainstream spine the prior already knows how to drive.

## Assumptions and scope

- Evidence is from Auzietek lab work on custom source intake, AlpinE
  posture, and BKC pipeline assistance through September 2026.
- This article does not claim factory completion or that BKC is
  defective.
- Companion: [Imitated Intelligence — LLMs as a giant smart macro](/post/imitated-intelligence-llms-as-smart-macros?lane=auzietek).

## Related lanes

- **Linux Users:** AI-assisted ops with receipts, not vibes.
- **Retro Users:** why Amiga assigns and readable shelves still matter.
- **BlackKnight / ThinkTank:** human-first automation, paper trails, and
  knowing when the green stage skipped the feature.
