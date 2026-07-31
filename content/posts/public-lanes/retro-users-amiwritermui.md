---
title: AmiWriterMUI: Building a Small AmigaOS 4.1 MUI Editor
slug: amiwritermui-amigaos41-editor-codex
summary: AmiWriterMUI is a compact AmigaOS 4.1 editor using MUI, TextEditor.mcc, tabs, drag-and-drop, theme experiments, and a repeatable Linux-to-Amiga build loop.
tags: [retro, amiga, amigaos4, mui, codex, teaching]
theme_variant: retro
status: published
seo_title: AmiWriterMUI AmigaOS 4.1 editor
seo_description: A retro computing note about building a small AmigaOS 4.1 MUI editor with Codex and learning from classic UI constraints.
hero_image_url: /content-files/assets/retro/projects/amiwriterreact.png
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

![AmiWriterMUI build loop diagram](/content-files/assets/retro/projects/amiwritermui/amiwritermui-build-loop.svg)

## What we built

- MUI application scaffold and menus.
- TextEditor.mcc integration.
- File open/save through ASL.
- Tabs for multiple files.
- Clipboard history.
- Drag-and-drop file open.
- Theme switching and recolor experiments.
- A growing README and concept notes so the work can be repeated.

![AmiWriter-style Amiga editor screenshot](/content-files/assets/retro/projects/amiwriterreact.png)

The screenshots matter because this work is not just a retro-themed writing
exercise. It is the evidence that the build loop produced visible software on a
real AmigaOS-style target. A small editor is enough surface area to expose real
toolkit questions: menus, file requesters, text buffer state, event callbacks,
clipboard behavior, font assumptions, and the little places where a modern
developer expects the framework to quietly solve the problem.

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

## Why this belongs beside the infrastructure work

The Amiga lane and the BlackKnightController lane look different on the
surface, but the operating habit is the same:

```text
small change
  -> build
  -> test on the real target
  -> capture the result
  -> preserve the working fragment
  -> repeat
```

That habit scales from a tiny MUI editor to a bare-metal hypervisor lab. The
tools change. The discipline does not.

<figure class="videoWrapper">
  <iframe src="https://www.youtube-nocookie.com/embed/videoseries?list=PLzZDgKo1qG2ECpD0ZHQYKTGIuNsXMgNb7" title="Auzietek retro development playlist" loading="lazy" allowfullscreen></iframe>
</figure>

Video lane:
[Auzietek retro development playlist](https://www.youtube.com/playlist?list=PLzZDgKo1qG2ECpD0ZHQYKTGIuNsXMgNb7).

Repo: [AmiWriterMUI on GitHub](https://github.com/auzieman/AmiWriterMUI).
