---
title: ESXi Backups with vim-cmd, SSH, and Storage Snapshots
slug: esxi-backups-vimcmd-good-old-days
summary: A practical ESXi SSH backup pattern using `vim-cmd`, brief suspend or snapshot windows, datastore copies, and a storage target such as TrueNAS, ZFS, or LVM.
tags: [linux, esxi, vmware, backup, shell, operations]
theme_variant: linux-pro
status: published
seo_title: ESXi backup notes with vim-cmd, SSH, and snapshots
seo_description: A practical ESXi backup walkthrough using SSH, vim-cmd, guest state checks, brief suspend or snapshot windows, and storage-side copies.
canonical_url: https://auzietek.com/node/13
---

This note started as an older ESXi backup script, but the pattern is still
useful for labs and small environments.

If you have a supported backup product, use it. If you are running a lab, a
small office host, or an ESXi 8 environment without fancy shared storage, it is
still valuable to understand the moving parts. SSH, `vim-cmd`, guest state
checks, brief suspend or snapshot windows, and a storage target such as TrueNAS,
ZFS, LVM, or a NAS export can produce understandable backups.

The caveat from the original note still matters: this kind of workflow can
pause guests or capture a crash-consistent image if you do not quiesce the
application correctly. That may be acceptable for a quiet lab VM. For databases
and business systems, add application-aware steps, test restores, and prefer
storage/API-supported snapshots when they are available.

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

That is not a productized backup system. It is a readable operational pattern:
discover the VM, identify its power state, move it into a safe-enough copy
window, perform the storage copy or snapshot, then restore the previous state.
That is why SSH access on ESXi remains useful in labs: normal operators can
script normal work and see exactly what happened.

## What the script was really doing

The important pieces were:

- keep an inventory of ESXi hosts;
- keep a per-host list of guest names;
- resolve guest names to VM IDs with `vim-cmd`;
- check current power state;
- briefly suspend, snapshot, or quiesce when needed;
- copy VM files or snapshot-backed volumes to a backup target;
- restore the previous power state;
- log what happened.

Those steps are still the right questions even if the implementation changes.

## Storage target options

A modern lab version usually has one of these backing stores:

- a TrueNAS VM or physical TrueNAS box exporting NFS or iSCSI;
- a Linux storage host using ZFS snapshots;
- an LVM-backed volume where snapshot copies are acceptable;
- a NAS share used as a landing zone for datastore copies;
- a vendor array such as NetApp where snapshots and clones are first-class.

The old companion script used LVM snapshots:

```bash
#!/bin/bash
vol=$1

if [ ! -f /etc/snapshots/$vol.last ]; then
  echo "0" > /etc/snapshots/$vol.last
fi

last=$(cat /etc/snapshots/$vol.last)
if [ "$last" -gt 0 ]; then
  last=0
fi

mkdir -p /mnt/snapshots/$vol.$last
umount /mnt/snapshots/$vol.$last || true

lvremove -f /dev/nas0/$vol.$last
sizeraw=$(du -sk /mnt/$vol | cut -f1)
size=$(echo "$sizeraw * 1.15" | bc)
lvcreate -L"${size}k" -s -n "$vol.$last" /dev/nas0/$vol
mount /dev/nas0/$vol.$last /mnt/snapshots/$vol.$last

last=$((last + 1))
echo "$last" > /etc/snapshots/$vol.last
```

That exact script should be reviewed before reuse, but the idea is still sound:
make a point-in-time copy at the storage layer, then copy from the stable view
instead of chasing a moving guest disk.

## How I would update it for an ESXi 8 lab

For a current ESXi 8 lab, I would keep the same operator loop but tighten the
edges:

- use SSH keys with a dedicated automation account where possible;
- resolve VM IDs with exact matching instead of loose `grep`;
- record VMX paths and datastore names before acting;
- prefer snapshots or guest-aware quiesce over suspend when the guest supports
  it;
- copy to a storage target with enough retention and checksums;
- run a restore test into an isolated network;
- keep logs beside the backup manifest.

The commands may shift between ESXi generations, but the shape of the work is
still useful.

## A small modernized skeleton

For a teaching repo, I would split the work into boring pieces instead of one
giant clever script:

```text
inventory/
  hosts.txt
  guests/
    esx01.txt

scripts/
  list-vms.sh
  snapshot-guest.sh
  copy-guest.sh
  restore-test.sh

manifests/
  2026-07-31/
    esx01-web01.json
```

The first script should only prove that host access and VM discovery are sane:

```bash
#!/usr/bin/env bash
set -euo pipefail

host="${1:?usage: list-vms.sh esx-host}"

ssh "$host" 'vim-cmd vmsvc/getallvms' |
  awk 'NR == 1 { next } { print $1, $2, $3 }'
```

Then add exact-name selection. Then add state capture. Then add the snapshot or
copy behavior. This is slower to write than one heroic shell file, but much
easier to teach, test, and hand to another operator.

## Snapshot thinking, not vendor worship

The storage idea is the part worth preserving. A ZFS, LVM, TrueNAS, or NetApp
style snapshot gives you a stable view of changing data without immediately
duplicating every block. Clones can appear quickly because only changed blocks
need new storage.

That is why this pattern keeps coming back in labs:

```text
quiet or quiesce guest
  -> snapshot or suspend boundary
  -> copy from stable storage view
  -> resume normal service
  -> test restore somewhere isolated
```

That flow is still relevant for people running ESXi 8, especially when their
budget does not include the backup stack they would prefer to buy.

## Why this belongs in the modern lane

BlackKnightController now treats hypervisors through APIs, SSH, IPMI, and
pipelines. This ESXi pattern is a predecessor to the same idea:

```text
inventory -> state check -> controlled action -> validation -> evidence
```

The tooling changes. The operator contract does not.

Learners do reach advanced work if we coach the intermediate steps clearly.
Understanding a script like this makes the later storage snapshot, backup API,
or BKC pipeline feel less like magic.

Companion example:
[ESXi vim-cmd backup pattern](https://github.com/auzieman/auzietek-linux-lab-guides/tree/main/examples/esxi-vimcmd-backups).
