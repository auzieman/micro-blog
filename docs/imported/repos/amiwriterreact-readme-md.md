---
title: "AmiWriterReact - ReAction text editor with menus (macro based)"
slug: "amiwriterreact-amiwriterreact-reaction-text-editor-with-menus-macro-based"
summary: "Repository Markdown staged for public article/story cleanup."
status: draft
source_type: repo_markdown
source_repo: "AmiWriterReact"
source_path: "AmiWriterReact/README.md"
source_id: "amiwriterreact-readme-md"
captured_at: "2026-07-29"
candidate_lane: "retro-users"
tags: [aiops, needs-review, repo-md, retro]
assets:
  - source: "/home/auzieman/Projects/AmiWriterReact/AmiWriterReact.png"
    status: "ok"
    local: "docs/images/imported-repos/amiwriterreact-readme-md/AmiWriterReact-87d165213c19.png"
---

# AmiWriterReact - ReAction text editor with menus (macro based)

This project is a basic text editor for **AmigaOS 4.x** built on top of
the **ReAction** GUI system.  It extends the primer version (AmiWriterMacro)
by adding a **menu bar** with **File** and **Edit** menus, a toolbar with
**Copy/Paste/Undo** buttons, and support for opening and saving files via
the **asl.library** file requester.  It still relies on the ReAction
macros (`reaction/reaction_macros.h`) and links against `-lauto` and
`-lraauto` for compatibility with the working SDK examples on the A1222.
Although using macros is discouraged for new projects, this variant
provides a convenient starting point that can later be migrated to
manual `OpenClass()` usage.

![AmiWriterReact screenshot](../images/imported-repos/amiwriterreact-readme-md/AmiWriterReact-87d165213c19.png)

## Directory structure

```text
AmiWriterReact/
├── README.md         - this file
├── Makefile          - build rules for OS 4 SDK
├── main.c            - entry point; calls controller
├── controller/
│   ├── app.h
│   └── app.c         - initializes model and launches the view
├── model/
│   ├── document.h
│   └── document.c    - document buffer logic
└── view/
    ├── mainwin.h
    └── mainwin.c     - ReAction GUI using macros (menus, toolbar, file requester)
```

## Building

To compile this project on your AmigaOS 4 system (for example on an
A1222), open a shell and type:

```sh
cd AmiWriterReact
make
```

The `Makefile` uses the OS 4 SDK’s standard include path (`SDK:Include/include_h`)
and links with `-lauto`, `-lraauto` and `-lasl`.  The first two
libraries automatically open the class interfaces for the ReAction macros, and
`-lasl` brings in the asl.library functions needed for the file requester.
After building, run `AmiWriter` to launch the program.

## Runtime notes

* The editor uses a multi-line string gadget with its own work and
  undo buffers sized to the document’s maximum length.  The status
  bar is a read-only string gadget limited to 255 characters.
* A **File** menu provides **Open...**, **Save...** and **Quit** commands.  When
  invoked, the Open/Save items display an ASL file requester and then
  load or save the document via `fread()`/`fwrite()`.  The selected path is
  combined from `rf_Dir` and `rf_File` and passed to the model.
* An **Edit** menu and a toolbar offer **Copy**, **Paste** and
  **Undo** actions.  These operate on the entire document: Copy copies
  the document text into an internal clipboard buffer, Paste appends
  the clipboard text to the document (saving the previous content for
  Undo), and Undo restores the document from the saved buffer.  Status
  messages indicate the action taken.
* The event loop still follows the ReAction example pattern: it obtains
  the window signal mask with `GetAttr(WINDOW_SigMask, ...)` and waits on
  that signal plus the app port before calling `RA_HandleInput()`.

## Docs

- `docs/drupal_article.html` contains a Drupal-ready writeup for this project.

## Future work

Once this version is running reliably, you can explore the modern OS 4
practices:

* Replace the macros with manual `OpenClass()` calls and `NewObject()`
  functions, as described in OS4 coding guidelines.
* Replace the simple string gadget with `texteditor.gadget` for multi-line
  editing.
* Integrate file requesters via `asl.library` to implement the Open and
  Save operations.
