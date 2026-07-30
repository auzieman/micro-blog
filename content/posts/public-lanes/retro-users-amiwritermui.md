---
title: AmiWriterMUI: Building a Small AmigaOS 4.1 MUI Editor
slug: amiwritermui-amigaos41-editor-codex
summary: AmiWriterMUI is a compact AmigaOS 4.1 editor using MUI, TextEditor.mcc, tabs, drag-and-drop, theme experiments, and a repeatable Linux-to-Amiga build loop.
tags: [retro, amiga, amigaos4, mui, codex, teaching]
theme_variant: retro
status: published
seo_title: AmiWriterMUI AmigaOS 4.1 editor
seo_description: A retro computing note about building a small AmigaOS 4.1 MUI editor with Codex and learning from classic UI constraints.
---

AmiWriterMUI is a small editor experiment for AmigaOS 4.1 using MUI-style
interface ideas.

That may sound niche, but niche platforms are often excellent classrooms. They
make the relationship between toolkit, event handling, files, memory, and user
expectations easier to see.

## The lesson is not nostalgia

The lesson is constraint.

Modern software can hide a great deal behind frameworks, package managers, and
giant runtime stacks. Older environments ask the developer to be more explicit:

- what is allocated?
- who owns it?
- when does the UI update?
- how does the file get saved?
- what happens when the user does the unexpected?

Those questions still matter everywhere.

## Where Codex helps

Codex can help turn a half-remembered platform into a working checklist:

- sketch the structure;
- identify the API shape;
- generate a small first pass;
- explain compiler errors;
- revise from real test results.

The useful pattern is the same one Auzietek uses in infrastructure work: make a
small change, test it, preserve the fragment, and improve the next run.

![AmiWriterMUI default theme](/content-files/assets/retro/projects/amiwritermui/default.png)

## What we built

- MUI application scaffold and menus.
- TextEditor.mcc integration.
- File open/save through ASL.
- Tabs for multiple files.
- Clipboard history.
- Drag-and-drop file open.
- Theme switching and recolor experiments.
- A growing README and concept notes so the work can be repeated.

![AmiWriterMUI dark theme](/content-files/assets/retro/projects/amiwritermui/dark.png)

## The build-and-test loop

The workflow stayed deliberately practical:

- edit on Linux;
- cross-compile with the OS4 SDK container;
- move binaries and screenshots with Pete's FTP;
- test on AmigaOS 4.1 / A1222 hardware;
- capture screenshots and notes;
- repeat in small steps.

```bash
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/AmiWriterMUI && make clean && make"
```

When something crashed or behaved oddly, the target-side GDB loop was enough to
keep the next step focused:

```text
gdb --args AmiWriterMUI
run
bt full
```

![AmiWriterMUI light theme](/content-files/assets/retro/projects/amiwritermui/light.png)

![AmiWriterMUI solar theme](/content-files/assets/retro/projects/amiwritermui/solar.png)

Repo: [AmiWriterMUI on GitHub](https://github.com/auzieman/AmiWriterMUI).
