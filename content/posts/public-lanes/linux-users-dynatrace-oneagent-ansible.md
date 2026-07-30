---
title: Dynatrace OneAgent with Ansible: The Pieces That Matter
slug: dynatrace-oneagent-ansible-paas-token-host-properties
summary: A refreshed walkthrough for installing Dynatrace OneAgent with Ansible, including the PaaS token nuance, inventory shape, host properties, and validation.
tags: [linux, ansible, dynatrace, monitoring, observability, automation]
theme_variant: linux-pro
status: published
seo_title: Dynatrace OneAgent Ansible install walkthrough
seo_description: Practical Dynatrace OneAgent Ansible notes covering PaaS tokens, tenant URL, host groups, network zones, host properties, and validation output.
canonical_url: https://auzietek.com/node/18
hero_image_url: /content-files/assets/linux/dynatrace-oneagent/download.png
---

Some monitoring installs fail because the operator does not understand the
secret shape yet.

The legacy Auzietek notes around Dynatrace OneAgent and Ansible are useful for
that reason. The mechanics are not hard, but there are a few screens and tokens
that can be confusing the first time through. Once those are understood, the
install becomes a repeatable host-prep step.

![Dynatrace deployment download screen](/content-files/assets/linux/dynatrace-oneagent/download.png)

## The practical sequence

The flow is:

```text
Dynatrace tenant
  -> deployment screen
  -> PaaS token
  -> Ansible collection
  -> inventory
  -> oneagent install args
  -> validation in Dynatrace host list
```

The important caveat from the original article: the download/link experience can
make it feel like one step is enough, but the Ansible role still needs the
tenant URL and token values wired correctly.

![Dynatrace Ansible docs screen](/content-files/assets/linux/dynatrace-oneagent/ansible-docs.png)

## Minimal inventory

For a local test host, the inventory can be intentionally small:

```yaml
all:
  hosts:
    127.0.0.1:
      ansible_connection: local
```

For real infrastructure, keep this in a repo as an example and keep the real
inventory outside public commits.

## OneAgent install playbook

The key values are:

- tenant environment URL;
- PaaS token;
- host group;
- network zone;
- host properties;
- restart behavior.

```yaml
---
- name: Install Dynatrace OneAgent
  become: true
  hosts: all
  collections:
    - dynatrace.oneagent
  vars:
    oneagent_download_dir: /tmp/
    oneagent_version: latest
    oneagent_environment_url: "https://YOUR-TENANT.live.dynatrace.com"
    oneagent_paas_token: "YOUR_PAAS_TOKEN_HERE"
    oneagent_install_args:
      - --set-host-group=App-4242_Docker_Prod
      - --set-network-zone=ionos
      - --set-host-property=AppName=labenv-oteld
      - --set-host-property=Function=OpenTelemetry_example
      - --set-host-property=Environment=Prod
      - --restart-service
  tasks:
    - import_role:
        name: oneagent
```

![PaaS token screen](/content-files/assets/linux/dynatrace-oneagent/paas-token.png)

## Validation output matters

Ansible is chatty, and that is useful. Early task output tells you whether the
role reached validation, download, and install steps:

```text
TASK [dynatrace.oneagent.oneagent : Validate download directory name doesn't contain spaces]
ok: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate OneAgent installer version parameter]
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate installation arguments]
skipping: [127.0.0.1]
```

The public article should not bury the reader in every task line. The companion
repo should keep full playbooks and full sample output.

## Re-applying host metadata

OneAgent host properties are operationally valuable because they let monitoring
and automation speak the same language.

If the install succeeds but the metadata does not look right, a follow-up
playbook can re-run `oneagentctl`-style configuration through the role:

```yaml
---
- name: Apply host-level OneAgent configuration
  become: true
  hosts: all
  collections:
    - dynatrace.oneagent
  vars:
    oneagent_install_args:
      - --set-host-group=APP-002_Prod
      - --set-network-zone=ionos
      - --set-host-property=AppName=labenv-oteld
      - --set-host-property=Function=OpenTelemetry_example
      - --set-host-property=Environment=Prod
      - --restart-service
  tasks:
    - import_role:
        name: oneagent
```

![Dynatrace host outcome](/content-files/assets/linux/dynatrace-oneagent/outcome.png)

## The modern Auzietek/BKC lesson

This is a good example of where blog, repo, and pipeline should separate:

- the article explains the token and metadata shape;
- the repo holds the full playbooks;
- BKC records the host inventory, secrets boundary, and validation result;
- GitHub Issues can track corrections when product screens change.

That keeps the tutorial readable while preserving the complete working example.
