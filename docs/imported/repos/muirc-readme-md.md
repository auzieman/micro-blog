---
title: "MuIRC"
slug: "muirc-muirc"
summary: "Repository Markdown staged for public article/story cleanup."
status: draft
source_type: repo_markdown
source_repo: "MuIRC"
source_path: "MuIRC/README.md"
source_id: "muirc-readme-md"
captured_at: "2026-07-29"
candidate_lane: "retro-users"
tags: [aiops, docker, needs-review, repo-md, retro]
assets:
  - source: "/home/auzieman/Projects/MuIRC/docs/screenshots/MuIRC.png"
    status: "ok"
    local: "docs/images/imported-repos/muirc-readme-md/MuIRC-61b423240a6f.png"
---

# MuIRC

MuIRC is a minimal MUI-based IRC client for AmigaOS 4.1. It starts as a clean
IRC-only build, but the protocol layer is separated so other networks can be
added later.

![MuIRC current state](../images/imported-repos/muirc-readme-md/MuIRC-61b423240a6f.png)

## Structure (MVC + plugins)

```
MuIRC/
├── Makefile
├── README.md
├── docs/
│   └── screenshots/
├── include/
│   ├── controller.h
│   ├── irc_plugin.h
│   ├── protocol.h
│   └── ui.h
├── plugins/
│   └── irc/
│       └── irc.c
└── src/
    ├── main.c
    ├── controller/
    │   └── app.c
    └── view/
        └── ui.c
```

## Build (AmigaOS 4.1)

```
cd MuIRC
make
```

## Build (cross-compile)

```
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/MuIRC && make clean && make"
```

## Icons

Icon artwork is in `icons/muirc_icon_pack_v2/`. Use IconEdit to import the
128x128 PNGs for app and drawer icons.

## Status

- MUI interface with tabs (status + per-room/per-user)
- MVC split (controller + view) with IRC protocol plugin
- Non-blocking connect with timeout handling
- TLS support via AmiSSL (toggle in UI, use 6697 for TLS servers)
- /LIST and /NAMES parsing with docked room/members panels
- Join button updates channel/target and opens the room tab
- DCC send + receive (default to `PROGDIR:Downloads`, optional Recv Ready)
- Ident responder (port 113) and basic CTCP VERSION reply

## Server defaults

The UI includes a small list for LAN + public networks. Entries are labeled in
the dropdown, but the server field uses the raw hostname.

LAN:
- 192.168.1.252

EFnet:
- irc.efnet.org
- irc.colosolutions.net
- irc.choopa.net
- irc.efnet.nl
- irc.mzima.net
- irc.deft.com
- efnet.deic.eu
- irc.swepipe.se
- irc.prison.net
- efnet.tngnet.nl
- irc.underworld.no

Undernet:
- amsterdam.nl.eu.undernet.org
- oslo.no.eu.undernet.org
- budapest.hu.eu.undernet.org
- tulip.eu.ix.undernet.org
- chicago.il.us.undernet.org
- losangeles.ca.us.undernet.org
- dallas.tx.us.undernet.org
- miami.fl.us.undernet.org
- seattle.wa.us.undernet.org

DALnet:
- nibiru.ix.eu.dal.net
- lair.nl.eu.dal.net
- bifrost.ca.us.dal.net
- lion.tx.us.dal.net
- serenity.fl.us.dal.net
- choopa.nj.us.dal.net
- sakura.jp.as.dal.net
- bitcoin.uk.eu.dal.net
- halcyon.il.us.dal.net
- atw.hu.eu.dal.net
- nonstop.ix.me.dal.net

For TLS servers, check the TLS box and use port 6697 (for example: Libera).

## Next steps

- Tune room search filters and refresh UX
- Add command shortcuts (WHOIS, PING, CLEAR)
- Improve DCC receive status UI and logging
