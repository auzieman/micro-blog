---
title: Repeatable PXE Installs Without the Drama
slug: repeatable-pxe-installs-without-the-drama
summary: A practical Linux guide for making PXE installs repeatable: scope DHCP, keep installer arguments boring, validate the handoff, and protect known-good fragments.
tags: [linux, teaching, debian, pxe, operations]
theme_variant: linux-pro
status: published
seo_title: How to make Debian PXE installs repeatable
seo_description: Learn a practical PXE install pattern for Debian and Linux labs: exact target scoping, simple installer arguments, preseed validation, disk boot checks, and post-install SSH proof.
---

PXE booting Linux is old technology, but it still rewards discipline.

Most PXE failures are not mysterious. They usually come from a small mismatch:
the wrong boot mode, a DHCP rule that catches too much, an installer argument
copied from the wrong example, a stale boot order, or a disk layout that was
not validated after install.

The fix is to keep the pattern boring.

## Start with exact target scope

Do not let every PXE request on the network fall into an installer.

For a lab, a safer pattern is:

1. identify the target machine;
2. record the boot NIC MAC address;
3. add a temporary DHCP/PXE exception for that exact MAC;
4. boot the installer;
5. remove or empty the exception after the handoff.

That gives you a one-shot install lane instead of an accidental network-wide
trap.

## Keep installer arguments simple

For Debian-style installs, start with the documented path before adding clever
options.

A plain unattended install usually needs:

- automatic install mode;
- the preseed URL;
- a predictable disk target;
- confirmation flags for partitioning;
- an SSH server package;
- a late command for minimal admin access;
- a normal reboot.

Avoid changing bootloader or partitioning behavior unless you have evidence for
the target hardware. If a simple working installer suddenly becomes fragile,
compare against the last known-good installer file before adding new logic.

## Validate the handoff

A PXE install is not done when the installer starts. It is done when the machine
boots from local disk and proves it is the installed operating system.

Useful checks:

```bash
hostnamectl
ip addr
lsblk
findmnt /
efibootmgr
systemctl status ssh
```

You are confirming:

- the hostname is expected;
- the IP address came back on the right interface;
- the install disk has the expected partition layout;
- the root filesystem is mounted from the disk you intended;
- UEFI has a valid boot entry if the host boots UEFI;
- SSH is reachable for the next automation step.

## Protect known-good fragments

When the install works, save the exact working pieces:

- iPXE menu entry;
- kernel/initrd URLs;
- preseed file;
- DHCP exception format;
- disk and bootloader settings;
- post-install validation commands.

Then treat them like source code. Change one thing at a time, and write down
why the change exists.

## The useful habit

Repeatable PXE is not about memorizing one perfect guide from the internet. It
is about building a small contract:

```text
target identity -> scoped boot path -> simple installer -> disk boot -> SSH proof
```

Once that contract is reliable, you can layer on more interesting work:
virtualization, containers, monitoring, backups, and application deployment.

But the base install should stay boring. Boring infrastructure is the kind you
can trust at 2 a.m.
