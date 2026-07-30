---
title: BlackKnightController Pipelines as Tutorial Kits
slug: blackknightcontroller-pipelines-as-tutorial-kits
summary: BKC pipelines are not only automation. They can become public articles, runnable GitHub kits, manual walkthroughs, and Ansible or Puppet/OpenVox companions.
tags: [blackknightcontroller, pipelines, tutorials, ansible, puppet, aiops, github]
theme_variant: midnight
status: published
seo_title: BlackKnightController pipelines as tutorial kits
seo_description: How validated BlackKnightController pipelines can become public tutorials, GitHub companion repos, manual guides, Ansible playbooks, and Puppet/OpenVox examples.
hero_image_url: /content-files/assets/bkc/bkc-pipeline-workbench.png
---

The surprising thing about a good pipeline is that it is already halfway to a
good tutorial.

A mature BlackKnightController pipeline contains the pieces an engineer needs to
learn from the work:

- goal and target;
- required inputs;
- prerequisites;
- ordered stages;
- transport choices;
- risk boundaries;
- validation checks;
- known-good fragments;
- evidence from real runs.

That is why BKC pipelines should not stay hidden inside the tool. The validated
ones can become public teaching material.

![BKC pipeline workbench](/content-files/assets/bkc/bkc-pipeline-workbench.png)

## The publishing pattern

The pipeline remains the native operating artifact. The public article tells the
story. The GitHub repo carries the complete runnable example.

```text
BKC pipeline
  -> public article
  -> GitHub tutorial kit
  -> manual shell walkthrough
  -> Ansible companion
  -> Puppet/OpenVox companion
  -> diagrams and screenshots
```

That lets different readers enter at different levels.

Someone learning Linux can start with the manual shell version. A traditional
operations team can start with Ansible. A configuration-management shop can look
at the Puppet/OpenVox companion. BKC remains the full fabric version that ties
hardware, services, evidence, and validation together.

## Example: bare-metal provisioning

A PXE pipeline naturally becomes a tutorial:

```text
1. The problem
2. The old manual way
3. The BKC pipeline way
4. DHCP/TFTP/HTTP pieces
5. Installer answer file
6. Disk/bootloader caveats
7. Validation evidence
8. Full repo example
```

The article should show the important excerpt, not a giant wall of preseed or
shell. The repo should carry the full files.

## Companion repo shape

```text
README.md
docs/
  overview.md
  troubleshooting.md
  screenshots/
manual/
  00-prereqs.md
  10-install.md
  20-validate.md
ansible/
  inventory.example.yml
  playbook.yml
  roles/
puppet/
  manifests/
  modules/
bkc/
  pipeline.json
  defaults.json
  fragments.json
examples/
  .env.example
  dhcpd.example.conf
  nginx.example.conf
```

The goal is not to bury people in files. The goal is to show that the public
guide is backed by something real.

## Why this matters

Many automation articles describe what a person could do. Auzietek should focus
on what was actually done, validated, and converted into repeatable form.

That posture matters:

- no vaporware;
- no stock screenshots pretending to be infrastructure;
- no magic step hidden in a chat transcript;
- every tutorial can point back to a runnable artifact or known-good fragment.

This is how Auzietek expands from a product site into a mentoring ecosystem:

- BKC for full-stack operational control;
- GitHub kits for hands-on learners;
- articles for context and judgment;
- issues for confirmed fixes and improvements;
- fragments for the truth we do not want to rediscover.

The machine becomes useful. The lesson becomes shareable.
