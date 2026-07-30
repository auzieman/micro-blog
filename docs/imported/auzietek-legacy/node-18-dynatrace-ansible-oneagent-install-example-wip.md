---
title: "Dynatrace  Ansible Oneagent install example:WIP"
slug: "dynatrace-ansible-oneagent-install-example-wip"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/18"
source_id: "node-18"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "https://auzietek.com/sites/default/files/inline-images/ansible-oneagent-download.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-18/image-01-625baa20e353.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/ansible-oneagent.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-18/image-02-5cc05b8142bd.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/ansible-oneagent-token.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-18/image-03-b28eceb7b77f.png"
  - source: "https://auzietek.com/sites/default/files/inline-images/ansible-oneagent-outcome.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-18/image-04-c75fe649f02c.png"
---

WIP:

For those checking out Dynatrace's monitoring solution

After getting your SaaS tenant going you may want to spin up a few agents,

Detailed instructions here, [Dynatrace Docs](https://www.dynatrace.com/support/help/setup-and-configuration/dynatrace-oneagent/deployment-orchestration/ansible)

One step I felt wasn't too clear at this point is here, inside your account under Deploy Dynatrace is the link to get the tar.gz file

![screenshot1](../images/legacy-auzietek/node-18/image-01-625baa20e353.png)

Whats a little confusing on this page is that download step,

![Docs..](../images/legacy-auzietek/node-18/image-02-5cc05b8142bd.png)

Yes you can kinda do that step but you still need the previous download in order to get the url working, off hand I'd say do my first step then skip to importing the collection.

Once its all downloaded, keys or rpc setups all taken care of comes the fun bit.  Here are a couple examples of using it.

In the screen where you might setup a hand installation you can generate or copy in your PaaS token

![token](../images/legacy-auzietek/node-18/image-03-b28eceb7b77f.png)

With your SaaS tenant host name and token in hand,,

An initial installation, I'll call this file oneagent_setup.yml Remember you need your SaaS tenant information,

```
---
- name: Download OneAgent installer in specific version to a custom
    directory with additional OneAgent install parameters. Both linux_other
    and linux_arm have different user specified by platform args parameter.
  become: true
  hosts: all
  collections:
    - dynatrace.oneagent
  vars:
    oneagent_download_dir: /tmp/
    oneagent_version: latest
    oneagent_install_args:
      - --set-host-group=App-4242_Docker_Prod
      - --set-network-zone=ionos
      - --set-host-property=AppName=labenv-oteld
      - --set-host-property=Function=OpenTelemetry_example
      - --set-host-property=Environment=Prod
      - --restart-service
    oneagent_environment_url: 'https://[YOUR Tennant base URL].live.dynatrace.com'
    oneagent_paas_token: 'YOUR PAAS TOKEN HERE'
  tasks:
  - import_role:
        name: oneagent
```

Next you need an Inventory, here is a simplistic localhost example,

```
all:
  hosts:
    127.0.0.1:
      ansible_connection: local
```

Once this is ran and assuming there aren't typos you should end up with a nicely configured oneagent,

```
]$ ansible-playbook -i docker_servers.yml oneagent_setup.yml
TASK [dynatrace.oneagent.oneagent : Validate installation directory name doesn't contains spaces] ***
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate download directory name doesn't contain spaces] ***
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate if download directory is available] ***
ok: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate OneAgent installer version parameter] ***
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Validate installation arguments] ***********
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Provide OneAgent installer] ****************
skipping: [127.0.0.1]

TASK [dynatrace.oneagent.oneagent : Gather installers info] ********************
skipping: [127.0.0.1]
```

The process is pretty chatty and if you don't see your host properties etc when done you may want to run a second playbook to try and clean that up,

This example would re-run the oneagentctl applying arguments and restarting it,

```
---
- name: Apply host level configuration with oneagentctl
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
      - --set-host-property=IT-SME=auzie-morgan
      - --set-host-property=Client-Owner=auzie-morgan
      - --set-host-property=Environment=Prod
      - --set-host-property=Notify=auzieman@auzietek.com
      - --restart-service
  tasks:
    - import_role:
          name: oneagent
```

Note no need to download again, so also no need to supply the url, token etc. as long as everything works out you should find an entry like this one in your hosts list shortly after.

![outcome](../images/legacy-auzietek/node-18/image-04-c75fe649f02c.png)

Blog tags

[Dynatrace](/taxonomy/term/17)

[linux](/taxonomy/term/7)

[ansible](/taxonomy/term/12)

[automation](/taxonomy/term/18)

[walk-through](/taxonomy/term/19)

Submitted by auzieman
 on Fri, 10/14/2022 - 15:46

