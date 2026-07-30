---
title: AuziX Runtime Structure: Programs, Services, Stacks, and Work
slug: auzix-runtime-structure-programs-services-stacks-work
summary: AuziX explores how an operating system could make ownership visible again by separating installed programs, long-running services, composed stacks, and work roots.
tags: [retro, auzix, linux, operating-systems, gobo-linux, nix, containers, teaching]
theme_variant: retro
status: published
seo_title: AuziX runtime model for readable operating systems
seo_description: AuziX runtime structure explains Programs, Services, Stacks, Work, containerd, compatibility paths, and why readable OS ownership still matters.
hero_image_url: /content-files/assets/retro/auzix/auzix-midori.jpg
---

AuziX asks a stubborn question:

> What if a modern operating system made ownership obvious again?

Classic systems often felt understandable because their structure was visible.
Modern Linux is powerful, but the shape of a running machine can disappear into
`/usr`, `/var`, package scripts, container state, systemd units, and tool-specific
directories.

AuziX is an experiment in making those relationships readable.

![AuziX browser checkpoint](/content-files/assets/retro/auzix/auzix-midori.jpg)

## The root contract

The current AuziX model sketches a root tree like this:

```text
/System    OS, kernel, drivers, settings, state, logs, shared system libraries
/Programs  versioned application payloads and user-facing executable families
/Services  supervised long-running host services
/Stacks    composed service groups, container bundles, and deployment graphs
/Work      builds, sources, caches, scratch data, pipelines
/Users     user homes and profiles
/Volumes   mounted local and remote storage
/Network   network identity, routes, DNS, service discovery views
```

That structure is not only cosmetic. Each directory answers a different
operational question:

- `/Programs` answers “what is installed?”
- `/Services` answers “what is running as an operating-system service?”
- `/Stacks` answers “what pieces are deployed together?”
- `/Work` answers “where do builds, sources, caches, and pipelines happen?”

## Containerd as substrate, not identity

Containers are useful, but AuziX should not let container tooling become the
whole operating-system identity.

One proposed mapping:

```text
/Programs/containerd/<version>/Commands/containerd
/Programs/runc/<version>/Commands/runc
/Programs/nerdctl/<version>/Commands/nerdctl
/Services/containerd
/System/Settings/containerd/config.toml
/System/State/containerd
/System/Logs/containerd
/Work/Containers/content
/Work/Containers/snapshots
/Work/Containers/buildkit
```

Compatibility paths can still exist:

```text
/System/Compatibility/bin/containerd
/System/Compatibility/bin/ctr
/System/Compatibility/bin/nerdctl
/System/Compatibility/bin/runc
```

The point is not to break Linux habits for sport. The point is to avoid hiding
ownership.

## Services and stacks

`/Services` owns long-running service identity:

```text
/Services/ssh
/Services/sftp
/Services/containerd
/Services/buildkit
/Services/display-manager
```

A service receipt can describe:

- native command path;
- settings path;
- state path;
- log path;
- runtime type;
- dependencies;
- exposed sockets and ports.

`/Stacks` owns composed relationships:

```text
/Stacks/remote-access
/Stacks/native-build
/Stacks/desktop-sandbox
/Stacks/graphical-porting
```

That separation matters. A program can be installed without being a service. A
service can be part of a larger stack. A stack can include host processes,
containers, settings, firewall rules, and documentation.

## Why this belongs in Retro Users

AuziX is future-facing, but its instincts are retro in the best way. It borrows
from Amiga clarity, GoboLinux readability, Slackware simplicity, Nix-style
receipts, and modern container isolation.

![AuziX NetSurf checkpoint](/content-files/assets/retro/auzix/auzix-netsurf.jpg)

The lesson is not “everyone should run this tomorrow.” The lesson is that
operating-system structure is a design choice. If we want machines to be easier
to reason about, the filesystem, service model, build model, and documentation
should all help the operator see what is happening.

That is the same philosophical thread running through BKC: make infrastructure
visible, repeatable, and explainable.

Repo: [AuziX on GitHub](https://github.com/auzieman/AuziX).
