---
title: "Introducing AmiWriterMUI: Codex‑Accelerated AmigaOS 4.1 Development"
slug: "introducing-amiwritermui-codex-accelerated-amigaos-4-1-development"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/41"
source_id: "node-41"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "https://dtlab.auzietek.com/auzieman/AmiWriterMUI/raw/master/screenshot.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-41/image-01-6eded826909c.png"
  - source: "https://dtlab.auzietek.com/auzieman/AmiWriterMUI/raw/master/screenshot2.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-41/image-02-a1d67d83452f.png"
  - source: "https://dtlab.auzietek.com/auzieman/AmiWriterMUI/raw/master/screenshot3.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-41/image-03-edbe05778880.png"
  - source: "https://dtlab.auzietek.com/auzieman/AmiWriterMUI/raw/master/screenshot4.png"
    status: "ok"
    local: "docs/images/legacy-auzietek/node-41/image-04-3830c29744e8.png"
---

**Intro**

We just finished a solid development sprint on AmiWriterMUI — a lightweight MUI text editor for AmigaOS 4.1. It’s built with the OS4 SDK and TextEditor.mcc, and it’s been rapidly iterated with Codex as a coding partner. This is a living tutorial: the project is small on purpose, but it now has tabs, drag‑drop, toolbar actions, and basic syntax coloring.

**Why This Matters**

AmigaOS 4.1 isn’t exactly mainstream — which makes it a perfect target for AI‑assisted development. Codex helped with platform‑specific MUI patterns, pulling from OS4 SDK examples, and cleaning up builds and runtime behavior. The result is a working editor and a documented path that others can follow.

**What We Built**

- MUI app scaffold + menus
- TextEditor.mcc integration
- File open/save with ASL
- Tabs for multiple files
- Clipboard history
- Drag‑and‑drop file open
- Theme switching + recolor pass
- A growing README + concept docs

**Example Prompt (for your own project)**

```
You are Codex acting as a coding partner for AmigaOS 4.1 MUI development.
Stay inside the OS4 SDK and its Examples folder; avoid AROS/OS3.1 snippets.
Build a minimal text editor using TextEditor.mcc with:
- menu actions (Open/Save/Copy/Paste/Undo/Recolor)
- tabs via RegisterObject
- ASL file requester
- drag-and-drop file open
- simple syntax coloring for C
Keep everything in main.c with clear helper sections, and add a README
explaining the flow + build instructions (docker cross-compile).

When errors appear, fix them in small steps and explain why.
Prefer OS4 interfaces and proper library open/close patterns.
```

**Workflow Notes (practical details)**

We kept the workflow tight and repeatable:

- **Target OS**: AmigaOS 4.1 Final (fully updated) on A1222
- **Tools on Amiga**: CodeBench, SGrab, and Pete’s FTP
- **SDK**: Official OS4 SDK (local, with Examples used as primary references)
- **Cross‑compile**: Docker container rolfkopman/os4env

**Cross‑compile command**

```
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/AmiWriterMUI && make clean && make"
```

**File transfer (Linux <-> Amiga)**

We used FTP to move binaries and screenshots quickly:

1. Run Pete’s FTP on AmigaOS 4.1
2. From Linux, push/pull the project folder or just the built binary
3. Test on hardware, then sync back screenshots and notes

This kept the loop fast: build on Linux, test on real hardware, document results.

**Debugging (GDB quick notes)**

When something crashed or behaved oddly, we used GDB on target:

```
gdb --args AmiWriterMUI
run
```

If it crashes:

```
bt full
```

To capture a full backtrace to a file for review:

```
set logging file gdb_bt.txt
set logging on
bt full
set logging off
```

Then copy `gdb_bt.txt` back to Linux for analysis and keep iterating.

**Screenshots**

![Legacy image 1](../images/legacy-auzietek/node-41/image-01-6eded826909c.png)

![Legacy image 2](../images/legacy-auzietek/node-41/image-02-a1d67d83452f.png)

![Legacy image 3](../images/legacy-auzietek/node-41/image-03-edbe05778880.png)

![Legacy image 4](../images/legacy-auzietek/node-41/image-04-3830c29744e8.png)

**SDK Walkthrough (quick build‑up)**

This isn’t every line of code, but it shows the order we assembled the app — like the old magazine “build it up” articles.

**1) Open OS4 libraries + interfaces**

We keep the OS4 pattern: open libraries first, grab interfaces, and cleanly close them later.

```
static BOOL open_libraries(void)
{
    MUIMasterBase = OpenLibrary("muimaster.library", 20);
    IMUIMaster = (struct MUIMasterIFace *)GetInterface(MUIMasterBase, "main", 1, NULL);

    IntuitionBase = (struct IntuitionBase *)OpenLibrary("intuition.library", 50);
    IIntuition = (struct IntuitionIFace *)GetInterface((struct Library *)IntuitionBase, "main", 1, NULL);

    AslBase = OpenLibrary("asl.library", 50);
    IAsl = (struct AslIFace *)GetInterface(AslBase, "main", 1, NULL);

    DOSBase = (struct DosLibrary *)OpenLibrary("dos.library", 50);
    IDOS = (struct DOSIFace *)GetInterface((struct Library *)DOSBase, "main", 1, NULL);
    return TRUE;
}
```

**2) Create the editor and its scrollbar**

TextEditor.mcc drives the edit area. We attach a vertical prop via `MUIA_TextEditor_Slider` so scrolling works correctly.

```
APTR vprop = PropObject, PropFrame,
    MUIA_Prop_Horiz, FALSE,
    MUIA_FixWidth, 10,
End;

APTR editor = TextEditorObject,
    MUIA_TextEditor_FixedFont, TRUE,
    MUIA_TextEditor_WrapMode,  TRUE,
    MUIA_TextEditor_Slider, vprop,
End;
```

**3) Build the window layout**

Toolbar on top, tab register in the middle, status line at the bottom.

```
app = ApplicationObject,
    MUIA_Application_Menustrip, MUI_MakeObject(MUIO_MenustripNM, menuBar, MUIO_MenustripNM_CommandKeyCheck),
    SubWindow, window = WindowObject,
        WindowContents, VGroup,
            Child, HGroup,  /* toolbar */
            Child, RegisterObject,  /* tabs */
            Child, status_text,     /* status */
        End,
    End,
End;
```

**4) Wire menu + toolbar actions**

We use MUI notifies to funnel actions into the input loop (Open/Save/Copy/Paste/Undo/Recolor).

```
DoMethod(app, MUIM_Notify, MUIA_Application_MenuAction, MEN_OPEN,
    app, 2, MUIM_Application_ReturnID, MEN_OPEN);

DoMethod(btn_save, MUIM_Notify, MUIA_Pressed, FALSE,
    app, 2, MUIM_Application_ReturnID, MEN_SAVE);
```

**5) File open/save with ASL**

Use an ASL requester to fetch a path, then load/save with plain C file IO.

```
if (pick_file("Open file", FALSE, path, sizeof(path)))
    load_file_into_tab(tab, path);
```

**6) Colorize when needed**

The colorizer is intentionally simple: scan text line‑by‑line, set blocks for keywords/strings/comments, and keep it predictable.

```
apply_syntax_colors(editor);
set(editor, MUIA_TextEditor_ColorMap, color_map);
```

That’s the core flow: open libs, build the UI, wire actions, then iterate on features and polish.

**Repo**

Source and notes are here:
[https://dtlab.auzietek.com/auzieman/AmiWriterMUI](https://dtlab.auzietek.com/auzieman/AmiWriterMUI)

**What’s Next**

We’ll keep refining color palettes, improve recolor performance, and continue adding editor ergonomics (shortcuts, maybe tabs/toolbar polish). The goal is a clear tutorial‑style codebase that new AmigaOS 4.1 developers can actually learn from.

**Credits & Thanks**

Big thanks to the AmigaOS 4.1 ecosystem that keeps development alive:

- [Hyperion Entertainment](https://www.hyperion-entertainment.com/) (AmigaOS 4.1)
- [OS4Depot](https://os4depot.net/) (app distribution + community sharing)
- [A‑EON Blog](https://blog.a-eon.biz/) (A1222/Tabor updates)
- [AmigaWorld.net](https://amigaworld.net/) (OS4 dev/community)
- [OS4Coding.net](https://www.os4coding.net/) (dev resources)
- [CodeBench](https://codebench.co.uk/) (primary IDE on target hardware)
- [TextEditor.mcc](https://sourceforge.net/projects/texteditor-mcc/) maintainers
- [Trevor’s Blog](https://www.trevor-dickinson.net/) (A1222 hardware support — thanks Trevor!)

Blog tags

[AmigaOS4.1](/taxonomy/term/52)

[gcc](/taxonomy/term/53)

[MUI](/taxonomy/term/54)

[gui](/taxonomy/term/55)

[development](/taxonomy/term/56)

Submitted by auzieman
 on Sat, 12/20/2025 - 15:27

