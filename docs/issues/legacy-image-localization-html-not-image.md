## Observation

The legacy Drupal capture localized some GitHub-hosted image URLs as `.png`
files, but the saved files are actually GitHub HTML pages.

## Evidence

Example local files:

```text
docs/images/legacy-auzietek/node-41/image-01-6eded826909c.png
docs/images/legacy-auzietek/node-43/image-01-135ed9b8e728.png
```

Observed with:

```bash
file docs/images/legacy-auzietek/node-41/image-01-6eded826909c.png
file docs/images/legacy-auzietek/node-43/image-01-135ed9b8e728.png
```

Actual:

```text
HTML document, Unicode text, UTF-8 text
```

Expected:

```text
PNG image data
```

## Suspected cause

Some legacy article image URLs point at GitHub web pages or redirected
repository/profile paths rather than raw image assets. The importer trusted the
URL suffix more than the response `Content-Type`.

## Fix direction

- In `scripts/legacy_drupal_to_markdown.py`, reject downloaded assets whose
  `Content-Type` is not image-like.
- Prefer the response content type for extension selection.
- Mark rejected assets in front matter as `download_rejected_non_image`.
- Remove or regenerate the bad staged files.

## Validation

- [ ] importer skips HTML responses for image tags
- [ ] `file docs/images/legacy-auzietek/node-*/*` reports image content for
      localized image assets
- [ ] staged Markdown records rejected image sources clearly
