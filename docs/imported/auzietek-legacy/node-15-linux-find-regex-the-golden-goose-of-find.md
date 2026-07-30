---
title: "linux find regex the golden goose of find,"
slug: "linux-find-regex-the-golden-goose-of-find"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/15"
source_id: "node-15"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

This often buggs the snot out of me because I don't regex enough and because of how many tools use regex differently.

Lots of write ups on find my remind us of things like -name "*.gz" -o -name "*.zip" but if your looking for a lot of matching paterns and don't feel like writing a 3 line long find command you might give the regex features a go..

Not all finds were created equal so first up pick your mode and to get a quick list try this. .

```
find ./ -regextype taco find: Unknown regular expression type `taco'; valid types are `findutils-default',
```

```
`awk', `egrep', `emacs', `gnu-awk', `grep', `posix-awk', `posix-basic', `posix-egrep',

`posix-extended'.
```

Myself I like how egrep works so I went with it, old schoolers may go for the posix types..

```
find ./ -regextype egrep -regex ".*.(gz|zip|tar|rar|7z)"
```

The above example should find any files ending in the compression types listed above.  Not sure of case.. try -iregex instead..

Ok but what would this example look like with just regular -name?

```
find ./ -name "*.gz" -o -name "*.zip" -o -name "tar" -o -name "rar" -o -name "7z"
```

Both work but I think in this case -regex is cleaner..

Blog tags

[linux](/taxonomy/term/7)

[linux shell awk regex](/taxonomy/term/6)

Submitted by auzieman
 on Wed, 07/20/2022 - 09:54

