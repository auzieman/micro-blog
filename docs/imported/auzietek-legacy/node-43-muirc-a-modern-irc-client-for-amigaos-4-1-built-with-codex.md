---
title: "MuIRC: A Modern IRC Client for AmigaOS 4.1 (Built with Codex)"
slug: "muirc-a-modern-irc-client-for-amigaos-4-1-built-with-codex"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/43"
source_id: "node-43"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "https://dtlab.auzietek.com/auzieman/MuIRC/raw/master/docs/screenshots/MuIRC.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-43/image-01-3f523d36fe5b.png"
---

MuIRC is a lightweight MUI-based IRC client for AmigaOS 4.1. It started as a minimal MVC tutorial, and grew into a practical daily driver with tabs, TLS support, DCC send/receive, and room/member lists. We used the official OS4 SDK, OS4 examples, and iterative testing on real hardware.

Walk through in youtube,

**What’s Working Now**

- MUI interface with tabs (status + per-room/per-user)
- Non-blocking connect with timeout handling
- TLS via AmiSSL (checkbox + port 6697)
- Room list (/LIST) and member list (/NAMES)
- DCC send + receive (defaults to PROGDIR:Downloads)
- Ident responder and CTCP VERSION reply

**How the UI is Built (short walkthrough)**

The UI is assembled in one pass using MUI objects: a top row for server/nick fields, a register object for tabs, and a bottom row for the input, target, and actions. Events are routed through a controller loop that dispatches connect, send, and list actions to the IRC plugin.

```
// 1) Create the window + gadgets
WindowContents, VGroup,
  Child, HGroup,  /* server, port, TLS, nick, channel, buttons */
  End,
  Child, RegisterObject, /* tabs */
  End,
  Child, HGroup,  /* input + actions */
  End,
End;
```

```
// 2) Main loop dispatch
while ((id = DoMethod(app, MUIM_Application_NewInput, &sigs)) != MUIV_Application_ReturnID_Quit)
{
    if (id == UI_ACT_CONNECT) { /* connect */ }
    if (id == UI_ACT_SEND)    { /* send */ }
    proto->poll();
}
```

**Build (Cross‑Compile)**

```
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/MuIRC && make clean && make"
```

**Screenshots**

![Legacy image 1](../images/legacy-auzietek/node-43/image-01-3f523d36fe5b.png)

**Repo**

Source: [https://dtlab.auzietek.com/auzieman/MuIRC](https://dtlab.auzietek.com/auzieman/MuIRC)

Blog tags

[AmigaOS4.1](/taxonomy/term/52)

[codex](/taxonomy/term/57)

[gcc](/taxonomy/term/53)

[irc](/taxonomy/term/58)

[jabber](/taxonomy/term/59)

Submitted by auzieman
 on Fri, 02/13/2026 - 08:51

