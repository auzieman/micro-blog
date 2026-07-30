---
title: "AmiWriterMUI"
slug: "amiwritermui-amiwritermui"
summary: "Repository Markdown staged for public article/story cleanup."
status: draft
source_type: repo_markdown
source_repo: "AmiWriterMUI"
source_path: "AmiWriterMUI/README.md"
source_id: "amiwritermui-readme-md"
captured_at: "2026-07-29"
candidate_lane: "retro-users"
tags: [aiops, docker, needs-review, repo-md, retro]
assets:
  - source: "screenshot.png"
    status: "missing"
    local: ""
  - source: "screenshot2.png"
    status: "missing"
    local: ""
  - source: "screenshot3.png"
    status: "missing"
    local: ""
  - source: "screenshot4.png"
    status: "missing"
    local: ""
---

# AmiWriterMUI

AmiWriterMUI is a minimal MUI-based text editor for AmigaOS 4.1 that uses
TextEditor.mcc. It supports open/save, copy/paste/undo, tabs, drag-and-drop,
and a first-pass C syntax colorizer. This repo is intentionally small and
approachable so you can use it as a template or tutorial for your own MUI apps.

## Screenshots
![Editor (Default theme)](screenshot.png)
![Editor (Dark+ theme)](screenshot2.png)
![Editor (Light theme)](screenshot3.png)
![Editor (Solar theme)](screenshot4.png)

## Requirements (target)
- AmigaOS 4.1
- MUI (muimaster.library)
- TextEditor.mcc installed (usually `MUI:Libs/mui/TextEditor.mcc`)

## Build (cross-compile with docker)
This project expects the OS4 SDK inside the docker image.

1. Start the build:

```
sudo docker run --rm -v /home/auzieman/Projects:/Projects \
  rolfkopman/os4env /bin/bash -lc "cd /Projects/AmiWriterMUI && make clean && make"
```

2. The output binary is:

```
AmiWriterMUI/AmiWriterMUI
```

## Run (target)
1. Copy `AmiWriterMUI` to your AmigaOS 4.1 machine.
2. Ensure `TextEditor.mcc` is installed in `MUI:Libs/mui/`.
3. Launch from Workbench or Shell.

If the editor gadget fails to open, the class is usually missing.

## Example workflow
- Use Pete's FTP + a daemon on your workstation to push/pull files.
- Use prompt chains with Codex or Copilot; this project was produced with the 5.2 model + Codex.
- Build iterations in docker until the cross-compile succeeds.
- Swap to Flowerpot or real hardware for native testing, then sync files back.
- Establish the git repo early and document changes as you go.
- Use SGrab on AmigaOS 4.1 for documentation and AI review of outcomes.
- When ready, publish to sites like OS4Depot.

## What This Example Shows
- Creating a MUI application and window
- Wiring a GadTools-style menu into MUI
- Using TextEditor.mcc for editable text
- Handling menu actions and the MUI input loop
- Using ASL for open/save
- Applying basic C syntax coloring
- Managing tabs with a Register group
- Handling drag-and-drop via AppMessage hooks

## Features
- File open/save via ASL requester
- Copy/Paste/Undo via menu and toolbar
- Tabs with per-file titles
- Drag-and-drop open (drops open in a tab)
- Clipboard history popup (quick paste of recent selections)
- Basic C syntax coloring (keywords, strings, comments, numbers, types, constants, preprocessor lines)
- Manual `Recolor` action in the Edit menu
- Theme menu with Dark+, Light, and Solar palettes
- Dark editor background and tuned palette for contrast
- Status bar messages for common actions

## Notes
- Coloring is applied on file load and when you use `Edit -> Recolor`.
- The palette is VSCode Dark+ inspired; the editor background is set explicitly.
- Highlighting is intentionally simple and meant as a starting point.
- Syntax highlighting is best-effort and intentionally simple.
- If TextEditor.mcc is missing, the app will not open its editor gadget.
- Clipboard history only captures marked selections.

## Quick Code Walkthrough
- `open_libraries()` opens `muimaster.library`, `intuition.library`, `asl.library`, and `dos.library` and grabs interfaces.
- `init_color_map()` sets up a small pen palette and assigns it to `MUIA_TextEditor_ColorMap`.
- The window is created via `ApplicationObject` and `WindowObject`, with a toolbar row and `RegisterObject` for tabs.
- Menu actions are wired with `MUIM_Notify` and handled in the `MUIM_Application_NewInput` loop.
- `apply_syntax_colors()` scans the editor text and uses `MUIM_TextEditor_SetBlock` to color ranges.
- Drag-and-drop uses an AppMessage hook to open files into tabs.

## Code Concepts
- See `docs/CONCEPTS.md` for a guided tour of tabs, history, file IO, and the colorizer.
- See `docs/PROJECT_NOTES.md` for build setup and collaboration notes.

## Issue Helper (Gogs)
If your container SSL intercept breaks validation, this helper posts issues
with certificate verification disabled.

```
export GOGS_BASE="https://dtlab.auzietek.com"
export GOGS_REPO="auzieman/AmiWriterMUI"
python3 tools/post_issue.py --token "...your token..." \
  --title "Bug: double paste" --body "Alt-V triggers twice."

Optional extras:
```
python3 tools/post_issue.py --token "...your token..." \
  --assignee "auzieman" --labels 1 2 --verbose \
  --title "Test issue" --body "Testing post_issue.py from Codex"
```
```

## Troubleshooting
- If the build fails with missing `exec/types.h`, you are compiling outside
  the OS4 SDK environment. Use the docker command above.
- If `Open...` or `Save...` does nothing, verify ASL is available and
  `asl.library` opens successfully on the target.
- If the toolbar or history popup looks empty, verify `TextEditor.mcc` is installed.

## Next Steps
- Improve the highlighter (multi-line strings, macros, more keywords)
- Add auto-recolor on idle or after edits
- Add a status bar for line/column and modified state
- Add toolbar icons and a clipboard macro picker
