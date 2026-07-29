---
title: AmiWriterMUI: Building a Small AmigaOS 4.1 Editor with Codex
slug: amiwritermui-amigaos41-editor-codex
summary: AmiWriterMUI is a compact retro-development exercise: build a useful editor while learning how older UI frameworks shape software design.
tags: [retro, amiga, amigaos4, mui, codex, teaching]
theme_variant: retro
status: published
seo_title: AmiWriterMUI AmigaOS 4.1 editor built with Codex
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
