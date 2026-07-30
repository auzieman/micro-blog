# Pipeline to tutorial kit

BKC pipelines are not only automation assets. They are also unusually good
tutorial source material.

A mature pipeline already contains:

- goal and target;
- inputs;
- prerequisites;
- ordered stages;
- transport choices;
- risks;
- validations;
- known-good fragments;
- operational memory.

That is most of what a strong tutorial needs.

## Concept

Turn a proven BKC pipeline into several reader-friendly forms:

```text
BKC pipeline
  -> public article
  -> GitHub tutorial kit
  -> manual shell walkthrough
  -> Ansible companion
  -> Puppet/OpenVox companion
  -> diagrams/screenshots
```

The public article explains the engineering story. The companion repo carries
complete files.

## Example targets

- PXE Debian/Trixie whole-disk install
- IPMI/BMC discovery and power control
- OpenStack lab host prepare
- Proxmox lab host prepare
- ESXi swarm seed
- Docker Swarm bring-up
- OpenStack VM seeding
- lab-edge nginx/IPFire routing
- monitoring and Grafana linkage

## Article pattern

```text
Title: Repeatable PXE installs without the drama

1. The problem
2. The old manual way
3. The BKC pipeline way
4. The important files
5. Validation evidence
6. Manual version
7. Ansible companion
8. Puppet/OpenVox companion
9. Caveats and next steps
```

Do not paste a 300-line preseed or playbook into the article. Show the important
section and link to the full repo path.

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

## Translation rules

When translating BKC to Ansible:

- preserve stage order;
- preserve validation commands;
- keep destructive actions explicit;
- keep inventory and secrets out of the repo;
- use handlers for service restarts;
- use templates for generated files.

When translating BKC to Puppet/OpenVox:

- focus on steady-state configuration;
- keep provisioning and destructive install steps separate;
- express packages, files, services, and users declaratively;
- document where imperative bootstrapping is still required.

When translating to manual shell:

- keep commands copyable;
- include expected output;
- include rollback or cleanup notes;
- explain why each command exists.

## Prompt steering

Each tutorial kit should include prompt files that tell Codex/Cursor/Copilot how
to adapt the kit.

```text
docs/prompts/transform-for-my-lab.md
docs/prompts/write-ansible-companion.md
docs/prompts/write-puppet-companion.md
docs/prompts/generate-troubleshooting-guide.md
```

Example prompt intent:

> Transform this tutorial kit for a small lab with one DHCP/TFTP server and two
> Dell hosts. Preserve destructive warnings, update IP ranges, and emit a
> validation checklist.

## Why this matters

This expands Auzietek from “here is BlackKnightController” into a mentoring
ecosystem:

- BKC for people who want the full operating fabric;
- manual guides for learners;
- Ansible/Puppet companions for existing shops;
- GitHub repos for repeatable proof;
- articles that teach the operational thinking behind the tools.

That is a stronger public posture than publishing one-off blog posts.
