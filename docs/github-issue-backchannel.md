# GitHub Issues as an evidence backchannel

GitHub Issues can become a lightweight backchannel between the public repos,
Codex, BKC, and future local models.

This grows out of the older `ai_worker` concept, which already had Kanboard
integration, feature toggles, and local-model hooks. The newer pattern is a
split lane:

```text
Kanboard
  private planning, accepted/public decisions, internal sequencing

GitHub Issues
  repo-facing bugs, tutorial requests, reproducible public tasks

Ollama / local model
  summarize, rank evidence, draft reproduction steps, suggest fixes

BKC fragments
  preserve known-good evidence and operational state
```

So this is not a replacement for Kanboard. It is the public/repo-facing version
of:

```text
found -> reproduced -> confirmed -> fixed -> validated -> closed
```

## Suggested labels

```text
bug
content
migration
pipeline
validated
needs-evidence
good-first-issue
ollama-candidate
```

## Issue body pattern

```markdown
## Observation

What was noticed?

## Evidence

- URL / command / screenshot / file path
- expected result
- actual result

## Suspected cause

Optional. Keep it humble unless proven.

## Fix direction

What should change?

## Validation

- [ ] test command
- [ ] smoke URL
- [ ] screenshot
- [ ] commit link
```

## Helper

Dry-run an issue:

```bash
python3 scripts/github_issue.py \
  --repo auzieman/micro-blog \
  --title "Legacy image localization saved GitHub HTML as PNG" \
  --label bug \
  --label migration \
  --body-file docs/issues/legacy-image-localization-html-not-image.md
```

Create it after review:

```bash
python3 scripts/github_issue.py \
  --repo auzieman/micro-blog \
  --title "Legacy image localization saved GitHub HTML as PNG" \
  --label bug \
  --label migration \
  --body-file docs/issues/legacy-image-localization-html-not-image.md \
  --create
```

The helper reads the token from:

```text
~/.secrets/github-auzieman-token
```

or from `GITHUB_TOKEN`.

## Owner guardrail

The helper refuses to create issues outside the `auzieman/*` repo namespace by
default.

That is intentional. Public issues should represent Auzie/Auzietek-authored
engineering work, not arbitrary prompts from someone who discovered an agent or
automation hook.

If a future organization-owned repo is approved, change the allowed owner
explicitly:

```bash
python3 scripts/github_issue.py \
  --repo auzietek/some-approved-repo \
  --allowed-owner auzietek \
  --title "Approved org issue" \
  --body-file issue.md \
  --create
```

Do not wire public comments, Discord messages, or anonymous web input directly
to issue creation. Treat them as observations that require owner review first.

## Why this pairs well with local LLMs

Once Ollama/OpenWebUI is back in the lab, issues can become a simple queue:

- summarize issue;
- retrieve related fragments;
- suggest reproduction commands;
- draft a fix plan;
- link commits and validation output.

That gives us a public-ish breadcrumb trail without needing Discord or another
chat system as the source of truth.

Discord or chat can still be useful for human conversation, but issues should be
the durable engineering artifact when the work belongs to a repo.
