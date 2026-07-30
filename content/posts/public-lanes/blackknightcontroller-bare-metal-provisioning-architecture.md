---
title: Bare Metal Provisioning Starts Before the Operating System Exists
slug: bare-metal-provisioning-starts-before-the-operating-system-exists
summary: BlackKnightController models physical machines, BMCs, NICs, switch ports, PXE assets, installers, validation events, and managed resources as one lifecycle.
tags: [blackknightcontroller, bare-metal, ipmi, pxe, provisioning, resource-graph]
theme_variant: midnight
status: published
seo_title: BlackKnightController bare metal provisioning architecture
seo_description: How BlackKnightController models physical hardware, BMCs, NICs, switch ports, PXE profiles, installer events, and validation evidence before an operating system exists.
hero_image_url: /content-files/assets/bkc/bkc-edge-ownership-graph.png
---

Bare metal automation starts before SSH exists.

That is the easy detail to miss. A virtual machine usually has a name, an API
record, a disk, a console, and a network attachment before we care about it.
Physical hardware is messier. The first useful identity may be a serial number,
a BMC address, a switch port, a NIC MAC address, or a DHCP lease.

BlackKnightController needs to reason about that earlier layer.

![BlackKnightController edge ownership graph](/content-files/assets/bkc/bkc-edge-ownership-graph.png)

## The physical-first lifecycle

The lifecycle is simple enough to draw:

```text
Physical Hardware
  -> Provisioning Services
  -> Installation
  -> Validation
  -> Managed Resource
  -> Application Deployment
```

Each transition should leave evidence that can appear in the resource graph,
pipeline runs, and node detail pages.

## Resource types that exist before Linux boots

Bare-metal provisioning needs first-class nodes for:

- `physical_machine` — chassis/server identity, serial number, asset tag,
  vendor, model, desired role, and power state;
- `bmc` — IPMI, Redfish, iDRAC, or another management controller endpoint;
- `network_interface` — MAC address, switch port, VLAN, DHCP lease, and boot
  capability;
- `network_switch` and `switch_port` — observed MACs, VLAN intent, LLDP/CDP
  evidence, and attached-node relationships;
- `provisioning_profile` — desired installer, PXE/iPXE path, preseed,
  kickstart, unattended file, virtual media, or post-install enrollment steps;
- `image_asset` — installer kernel/initrd, ISO, WinPE/WIM, ESXi bundle,
  Proxmox ISO, rescue image, checksum, and cache path;
- `validation_evidence` — DHCP lease, HTTP checksum, installer callback,
  first-boot SSH banner, host key, or role check.

Those objects can exist without an operating system. That is the core
difference between hardware provisioning and normal server management.

## Relationships should be explicit

BKC should not guess topology from names when physical work is at stake. The
graph needs directional relationships:

```text
physical_machine -> controlled_by -> bmc
physical_machine -> has_interface -> network_interface
network_interface -> attached_to -> network_switch
network_switch -> has_port -> switch_port
switch_port -> observes_mac -> network_interface
service:dhcpd -> provides_dhcp -> network
service:pxe -> serves_pxe -> network
provisioning_profile -> uses -> image_asset
pipeline -> applies_profile -> provisioning_profile
physical_machine -> booted_from -> image_asset
validation_evidence -> validates -> physical_machine
```

That shape makes the UI and the pipeline safer. Before a destructive install,
the operator can see which machine, BMC, NIC, switch port, DHCP scope, image, and
installer profile are in play.

## Events turn a boot into evidence

A provisioning run should emit normalized events:

```text
discovery.detected
bmc.reachable
power.requested
dhcp.lease_observed
pxe.boot_request
ipxe.script_rendered
image.selected
image.checksum_validated
installer.started
installer.completed
first_boot.observed
enrollment.completed
validation.passed
```

When a run fails, those events still matter. A failed PXE run that proves the
right MAC requested the right boot image is not wasted work; it narrows the
next test.

## Why this matters in the lab

The recent Auzietek lab work exercised exactly this path: iDRAC/IPMI power
control, PXE boot, Debian Trixie installs, OpenStack, Proxmox, ESXi installer
handoff, Docker Swarm seeding, edge routing, and validation.

The lesson is not that BKC makes hardware magically safe. The lesson is that
hardware work becomes easier to repeat when every boundary produces evidence.

Repo direction: [BlackKnightController on GitHub](https://github.com/auzieman/BlackKnightController-main).
