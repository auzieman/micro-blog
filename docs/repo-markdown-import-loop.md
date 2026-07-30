# Repository Markdown import loop

Legacy Drupal pages are not the only source for public Auzietek content.
Several project repositories already contain strong Markdown, screenshots, and
operator notes. Those files should be staged before they are rewritten into
public articles.

## Why this exists

Repo Markdown usually has better signal than scraped HTML:

- it is already Markdown;
- images often live beside the docs;
- commands and code blocks are cleaner;
- the source path tells the story context;
- project READMEs can become public case studies.

## Capture examples

Stage selected repos:

```bash
python3 scripts/repo_markdown_to_staging.py \
  --source /home/auzieman/Projects/AuziX/docs \
  --source /home/auzieman/Projects/MuIRC/README.md \
  --source /home/auzieman/Projects/BlackKnightController/docs
```

Stage one specific project:

```bash
python3 scripts/repo_markdown_to_staging.py \
  --source /home/auzieman/Projects/rx-demo
```

## Output

Markdown drafts:

```text
docs/imported/repos/
```

Localized relative images:

```text
docs/images/imported-repos/
```

## Promotion rule

Do not publish staged repo Markdown directly.

Use it as source evidence, then write a public-facing article with:

- a clearer title and summary;
- a lane assignment;
- public-safe wording;
- useful screenshots;
- links back to the source repo when appropriate.

## Prompt steering

Each mature repo should eventually include a prompt file such as:

```text
docs/prompts/public-story.md
docs/prompts/maintenance.md
docs/prompts/release-notes.md
```

Those prompt files should tell Codex how to transform the repo into public
articles, examples, pipeline fragments, and documentation. This pairs well with
BKC fragments: prompts steer the writing, fragments preserve operational truth.
