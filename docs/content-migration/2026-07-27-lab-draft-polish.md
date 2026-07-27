# 2026-07-27 lab draft polish receipt

Status: live lab content population and review pass.

Target deployment:

```text
micro-blog lab canary
linux-users.lab.auzietek.com
retro-users.lab.auzietek.com
```

## Scope

Initial pass updated metadata only:

- title
- slug
- summary
- SEO title
- SEO description
- tags for one retro article missing tags

Later lab review guidance changed the migrated Linux/retro articles from draft
to published inside the lab canary so the lane pages can be reviewed directly.
This is lab publication only, not production promotion.

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

## Article body polish started

```text
ART-3942BEBDF7FC
  Source example reviewed:
    https://auzietek.com/index.php/node/43
  Preserved:
    YouTube iframe
    localized screenshot
    source repository link
  Updated:
    exact legacy source URL
    opening paragraphs
    media intro sentence
    summary / SEO description
  Status:
    still draft
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

## Lab publication

The imported Linux and Retro articles were made visible in the lab lanes for
review.

```text
linux-pro published count: 8
retro published count: 4
```

Tags were normalized so lane filters include the migrated articles:

```text
linux-pro articles include: linux
retro articles include: retro
```

Because one population step updated the write model directly, the projection
service was force-refreshed so Redis/read-model state matched the published
article table.

## Additional Linux source imported

```text
ART-F2D326A50D99
  Source:
    https://auzietek.com/node/44
  Published lab title:
    RX-Demo Part 1: Building Observability Into a Cloud-Native App
  Slug:
    rx-demo-part-1-cloud-native-observability
  Preserved:
    YouTube iframe
    localized screenshots
    GitHub link to rx-demo
  Lane:
    linux-users.lab.auzietek.com
  Status:
    published in lab
```

## Lab review URLs

```text
http://linux-users.lab.auzietek.com/blog?page_size=20
http://retro-users.lab.auzietek.com/blog?page_size=20
```
