# Legacy Markdown import loop

The public-lane migration should use a structured evidence loop instead of
copying directly from memory into polished public posts.

## Loop

1. Capture legacy page HTML into staged Markdown.
2. Localize owned screenshots/images under `docs/images/legacy-auzietek/`.
3. Preserve source URL, node id, capture date, asset list, and review tags.
4. Review the staged Markdown.
5. Clean style, remove Drupal chrome, and assign lane:
   - `auzietek`
   - `blackknightcontroller`
   - `linux-users`
   - `retro-users`
   - `archive`
6. Promote the cleaned article into `content/posts/public-lanes/`.
7. Run filesystem sync and smoke checks.

## Capture examples

Capture specific legacy nodes:

```bash
python3 scripts/legacy_drupal_to_markdown.py --node 14 --node 15 --node 44 --insecure
```

Capture a range:

```bash
python3 scripts/legacy_drupal_to_markdown.py --node-range 13-44 --insecure
```

Capture a specific URL:

```bash
python3 scripts/legacy_drupal_to_markdown.py \
  --url https://auzietek.com/index.php/node/43 \
  --insecure
```

## Output

Markdown:

```text
docs/imported/auzietek-legacy/
```

Localized images:

```text
docs/images/legacy-auzietek/node-*/
```

The generated files are drafts by default. They are not automatically public
content.

## Why this exists

The first public-lane pass over-compressed several legacy articles. This loop
keeps useful explanation, code, screenshots, and historical voice available
before editorial cleanup begins.
