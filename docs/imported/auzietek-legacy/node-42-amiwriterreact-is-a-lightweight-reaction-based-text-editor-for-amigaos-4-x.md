---
title: "AmiWriterReact is a lightweight ReAction-based text editor for AmigaOS 4.x."
slug: "amiwriterreact-is-a-lightweight-reaction-based-text-editor-for-amigaos-4-x"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/42"
source_id: "node-42"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "https://auzietek.com/sites/default/files/inline-images/AmiWriterReact.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-42/image-01-87d165213c19.png"
---

**Intro**

AmiWriterReact is a lightweight ReAction-based text editor for AmigaOS 4.x. It uses the OS4 SDK, ReAction macros, and a simple MVC-style layout (controller/model/view). This build focuses on the fundamentals: menus, file I/O, and a clean controller/view split.

**What We Built**

- ReAction window with menus and toolbar
- File open/save via asl.library requester
- Copy/Paste/Undo actions
- Simple document model and status messages
- Controller-driven app flow (clear MVC separation)

**Build Notes**

```
cd AmiWriterReact
make
```

**Workflow Notes**

We kept the loop tight: edit on Linux, build with the OS4 SDK, then test on real hardware (A1222). Pete’s FTP made moving builds and screenshots fast.

**Debugging**

When something crashed or behaved oddly, we used GDB on target:

```
gdb --args AmiWriterReact
run
```

If it crashes:

```
bt full
```

To capture a full backtrace:

```
set logging file gdb_bt.txt
set logging on
bt full
set logging off
```

**SDK Walkthrough (quick build‑up)**

Like a classic magazine article, this is the “build it up” view without every line.

**Code Structure: React vs MUI**

AmiWriterReact splits work across files (controller/model/view) to keep ReAction UI logic readable, while AmiWriterMUI intentionally stays in a single `main.c` to emphasize a compact tutorial flow. Different structure, same goal: clarity.

**1) Open libraries and interfaces**

ReAction apps still need clean library open/close. This project keeps it simple and centralized so errors are easier to spot.

```
struct Library *IntuitionBase = NULL;
struct IntuitionIFace *IIntuition = NULL;

IntuitionBase = OpenLibrary("intuition.library", 50);
IIntuition = (struct IntuitionIFace *)GetInterface((struct Library *)IntuitionBase, "main", 1, NULL);
```

**2) Build the window + gadgets**

The UI is assembled with ReAction macros and a clear controller/view split. Menu, toolbar, and the text gadget are wired in one place for readability.

```
Object *window = WindowObject,
    WA_Title, "AmiWriterReact",
    WindowContents, VGroup,
        Child, toolbar,
        Child, editor_gadget,
    End,
End;
```

**3) Run the input loop**

The main loop waits on the window’s signal mask and routes actions from menus and buttons into the controller.

```
ULONG sigs = 0;
GetAttr(WINDOW_SigMask, window, &sigs);
while (running)
{
    ULONG rc = RA_HandleInput(window, &sigs);
    // handle menu IDs + gadget IDs
}
```

**4) File IO via ASL**

Open/Save uses asl.library and feeds the selected path into the document model.

```
if (pick_file("Open file", FALSE, path, sizeof(path)))
    load_document(path);
```

That’s the basic structure: open libs, build the UI, wire actions, and keep the app logic in a clean controller path.

**Screenshots**

![AmiWriterReact](../images/legacy-auzietek/node-42/image-01-87d165213c19.png)

g

**Video**

Watch the walkthrough: [https://youtu.be/Wb904ngIYY0](https://youtu.be/Wb904ngIYY0)

**Repo**

Source: [https://dtlab.auzietek.com/auzieman/AmiWriterReact](https://dtlab.auzietek.com/auzieman/AmiWriterReact)

Submitted by auzieman
 on Sun, 12/21/2025 - 10:43

