---
title: ESXi Backups in the Good Old Days
slug: esxi-backups-vimcmd-good-old-days
summary: An archival ESXi SSH backup pattern using `vim-cmd`, snapshots, suspend windows, and datastore copies.
tags: [linux, esxi, vmware, backup, shell, operations]
theme_variant: linux-pro
status: published
seo_title: ESXi backup notes with vim-cmd and SSH
seo_description: A practical archival ESXi backup walkthrough using SSH, vim-cmd, guest state checks, snapshots, and datastore copies.
canonical_url: https://auzietek.com/node/13
---

This one is a time capsule, but a useful one.

Before every platform had a polished backup appliance, many small shops used
SSH, datastore copies, snapshots, and `vim-cmd` to get “good enough” VM backups
from ESXi. It was not elegant, but it was understandable: ask the host what VMs
exist, check power state, quiesce or suspend briefly, copy the files, and bring
the guest back.

The caveat from the original note still matters: this kind of backup can pause
guests. On a quiet lab that may be fine. In production, storage snapshots,
backup APIs, and application-aware quiescing are better choices.

## The old operator loop

The legacy script walked host files and guest files:

```bash
cd /etc/snapshots/

for vmhost in $(cat vmhosts); do
  echo "Working with VMHost:$vmhost"

  for vmguest in $(cat "$vmhost.vmguests"); do
    echo "Working with VMGuest:$vmguest, getting vmgid"
    vmgid=$(ssh "$vmhost" "vim-cmd /vmsvc/getallvms | grep $vmguest | cut -f1 -d' '")

    echo "Working with VMGuest:$vmguest, got vmgid:$vmgid, getting vmgstate"
    vmgstate=$(ssh "$vmhost" "vim-cmd /vmsvc/power.getstate $vmgid | grep -v Retrieved")
    vmgstate=$(echo "$vmgstate" | sed "s/ /_/g")

    echo "VMGuest:$vmguest was VMGuest ID:$vmgid and is in the state:$vmgstate"
  done
done
```

That is not a productized backup system. It is a readable operational pattern.
It also shows why SSH access on ESXi remained useful for years: normal operators
could script normal work.

## What the script was really doing

The important pieces were:

- keep an inventory of ESXi hosts;
- keep a per-host list of guest names;
- resolve guest names to VM IDs with `vim-cmd`;
- check current power state;
- briefly suspend or snapshot when needed;
- copy VM files to a backup target;
- restore the previous power state;
- log what happened.

Those steps are still the right questions even if the implementation changes.

## Why this belongs in the modern lane

BlackKnightController now treats hypervisors through APIs, SSH, IPMI, and
pipelines. This old ESXi pattern is a predecessor to the same idea:

```text
inventory -> state check -> controlled action -> validation -> evidence
```

The tooling changed. The operator contract did not.

For a current lab, use supported backup tooling when available. For learning,
this article is still valuable because it shows the moving parts without hiding
behind a vendor wizard.
