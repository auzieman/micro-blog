---
title: Using Linux find Regex Without the Three-Line Command
slug: linux-find-regex-without-the-three-line-command
summary: A small Linux shell pattern for replacing piles of repeated find clauses with one readable regular expression.
tags: [linux, shell, find, regex, teaching]
theme_variant: linux-pro
status: published
seo_title: Linux find regex without long command chains
seo_description: Learn how to use find -regex and -iregex to search for families of files with one readable Linux command.
canonical_url: https://auzietek.com/node/15
---

Small shell habits compound.

When a command turns into a long pile of repeated `-name` clauses, it often
means the command is fighting the problem instead of describing it. That does
not make anyone a bad Linux user. It just means the shell is offering a cleaner
shape.

`find` can match a family of names with a regular expression:

```bash
find ./ -regextype egrep -iregex '.*\.(gz|zip|tar|rar|7z)'
```

That command says what it means:

- start below the current directory;
- use a familiar extended regex style;
- ignore case;
- match common archive extensions.

## Why this is better

The longer form is easy to write in a hurry:

```bash
find ./ -name '*.zip' -o -name '*.tar' -o -name '*.gz'
```

But it becomes awkward as the list grows. Parentheses and quoting rules can also
make `find` behave differently than the person reading the command expects.

The regex version is usually easier to:

- copy into a runbook;
- review in a pull request;
- expand with one more extension;
- teach to a newer engineer.

## A useful operator rule

If a command looks like it is repeating the same idea three or four times, pause
and ask whether the tool has a pattern language.

Sometimes the simple version is still best. But when you are searching for a
family of files, a compact expression often reduces mistakes and makes the
intent easier to preserve.

Legacy source: [Auzietek Linux find note](https://auzietek.com/node/15).
