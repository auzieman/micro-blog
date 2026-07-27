# 2026-07-27 lab draft polish receipt

Status: live lab metadata polish, draft-only.

Target deployment:

```text
micro-blog lab canary
linux-users.lab.auzietek.com
retro-users.lab.auzietek.com
```

## Scope

This pass intentionally updated metadata only:

- title
- slug
- summary
- SEO title
- SEO description
- tags for one retro article missing tags

Article bodies were not rewritten yet. The imported body content remains source
material for a later editorial pass.

## Updated Linux drafts

```text
ART-213CAECA683C
  Using Linux find Regex Without the Three-Line Command
  linux-find-regex-without-the-three-line-command

ART-08D53E026180
  A Practical Puppet Setup for Small Linux Environments
  practical-puppet-setup-small-linux-environments
```

## Updated Retro drafts

```text
ART-322317C4F453
  AmiWriterMUI: Building a Small AmigaOS 4.1 Editor with Codex
  amiwritermui-amigaos41-editor-codex

ART-E4AD07FE40E9
  AmiWriterReact: A ReAction Text Editor for AmigaOS 4.x
  amiwriterreact-reaction-text-editor-amigaos4

ART-3942BEBDF7FC
  MuIRC: A Small AmigaOS 4.1 IRC Client Built with Codex
  muirc-amigaos41-irc-client-codex
```

## Next editorial pass

Before publishing these drafts:

- rewrite the first two paragraphs for the target audience;
- keep source traceability to the old Auzietek Drupal URL;
- preserve useful code blocks and screenshots;
- remove WIP phrasing unless the post is explicitly a lab note;
- add GitHub and YouTube evidence cards where relevant;
- verify rendered post pages before changing status to `published`.

No production hostnames or public certificates were changed in this pass.
