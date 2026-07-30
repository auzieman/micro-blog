---
title: MuIRC: A Small AmigaOS 4.1 IRC Client Built in a Tight Test Loop
slug: muirc-amigaos41-irc-client-codex
summary: MuIRC is a compact AmigaOS 4.1 IRC client with MUI tabs, TLS, room/member lists, DCC support, and a plugin-shaped protocol layer.
tags: [retro, amiga, amigaos4, muirc, codex, teaching]
theme_variant: retro
status: published
seo_title: MuIRC AmigaOS 4.1 IRC client
seo_description: A retro computing article about building a small AmigaOS 4.1 IRC client with Codex and preserving classic-platform engineering lessons.
canonical_url: https://auzietek.com/index.php/node/43
---

MuIRC is a small MUI-based IRC client for AmigaOS 4.1. It started as a minimal
MVC tutorial and grew into a practical testbed with tabs, TLS support, DCC
send/receive, room/member lists, an ident responder, and a protocol layer that
could support more networks later.

Retro work has a way of forcing honesty. APIs are smaller. Toolchains are more
particular. UI choices are visible. A careless assumption shows up quickly.

![MuIRC current state](/content-files/assets/retro/projects/muirc.png)

## Why this belongs in the retro lane

Classic-platform development teaches modern engineers to pay attention to:

- event loops;
- memory and resource limits;
- packaging details;
- UI toolkit expectations;
- compatibility;
- patient testing.

Those same habits matter in modern infrastructure, even when the machines are
larger and the layers are thicker.

## What is working now

- MUI interface with status, room, and user tabs.
- Non-blocking connect with timeout handling.
- TLS through AmiSSL for servers that expect port `6697`.
- `/LIST` room listing and `/NAMES` member list parsing.
- DCC send and receive, defaulting downloads to `PROGDIR:Downloads`.
- Ident responder on port `113`.
- Basic CTCP `VERSION` response.

## How the UI is shaped

The UI is assembled with MUI objects: connection fields across the top, a
register object for tabs, and a bottom row for message input and actions.
Events route through the controller loop into the IRC plugin.

```c
WindowContents, VGroup,
  Child, HGroup,         /* server, port, TLS, nick, channel, buttons */
  End,
  Child, RegisterObject, /* status, room, and user tabs */
  End,
  Child, HGroup,         /* input + actions */
  End,
End;
```

The cross-build path stays simple:

```bash
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/MuIRC && make clean && make"
```

## Assisted development as a patient pair programmer

Codex can help draft code, explain old APIs, compare implementation paths, and
turn a crash into the next focused experiment. But the human still has to test
on the real target, decide what belongs in the project, and preserve the notes
that future maintainers will need.

That makes MuIRC a useful Auzietek story: not “AI writes software by magic,” but
“AI helps an engineer keep moving through a tricky platform with better context.”

Repo: [MuIRC on GitHub](https://github.com/auzieman/MuIRC).

Legacy source: [MuIRC on Auzietek](https://auzietek.com/index.php/node/43).
