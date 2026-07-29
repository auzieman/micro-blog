---
title: AmiWriterReact: A ReAction Text Editor for AmigaOS 4.x
slug: amiwriterreact-reaction-text-editor-amigaos4
summary: AmiWriterReact explores a ReAction-style AmigaOS editor and the practical engineering habits classic systems still teach.
tags: [retro, amiga, amigaos4, reaction, codex, teaching]
theme_variant: retro
status: published
seo_title: AmiWriterReact ReAction editor for AmigaOS 4.x
seo_description: A retro computing article about building a ReAction-style text editor for AmigaOS 4.x and applying classic engineering lessons to modern work.
---

AmiWriterReact is the sibling experiment to AmiWriterMUI: another small editor,
another UI toolkit path, another chance to learn from a platform where the
layers are close enough to touch.

That closeness is valuable.

When the stack is smaller, design decisions feel more immediate. The program
either responds cleanly or it does not. The file either saves correctly or it
does not. The interface either fits the system's habits or it feels wrong.

## Comparing toolkit paths

Trying more than one toolkit is useful because it separates the application idea
from the framework choice.

For a small editor, the core needs are stable:

- create a window;
- accept text input;
- open and save files;
- report errors clearly;
- exit without losing work.

The toolkit changes how those needs are expressed.

## Why modern engineers should care

That same separation matters in current systems. Whether the target is a web
framework, a container platform, a hypervisor API, or a monitoring stack, the
engineer should understand the job before becoming loyal to the wrapper.

Retro projects like AmiWriterReact keep that muscle sharp.
