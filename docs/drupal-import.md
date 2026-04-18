# Drupal Import

`micro-blog` can import article content from a Drupal API source through:

- `POST /admin/import/drupal`

This route is designed for Drupal JSON:API first. It will also work with similar JSON payloads if you provide a `field_map`.

## Recommended Drupal source setup

Use a read-only Drupal endpoint for your article content type.

Preferred option:

- Drupal JSON:API

Typical example:

```text
https://your-drupal-site.example/jsonapi/node/article
```

Useful query additions:

- `include=field_tags` only if that relationship actually exists on the bundle
- `page[limit]=25`

Example:

```text
https://your-drupal-site.example/jsonapi/node/article?page[limit]=25
```

## Expected default field mapping

By default the importer looks for:

- title: `attributes.title`
- summary: `attributes.body.summary`
- body html: `attributes.body.processed`
- body raw: `attributes.body.value`
- slug/path: `attributes.path.alias`
- created: `attributes.created`
- updated: `attributes.changed`
- source URL: `links.self.href`
- hero image: `relationships.field_image`

Tags are resolved from:

- `relationships.field_tags.data`
- `included[].attributes.name`

## Example import request

```bash
curl -X POST http://localhost:8080/admin/import/drupal \
  -H "Content-Type: application/json" \
  -d '{
    "admin_email": "auzieman@gmail.com",
    "endpoint_url": "https://your-drupal-site.example/jsonapi/node/article",
    "params": {
      "page[limit]": "10"
    },
    "source_base_url": "https://your-drupal-site.example",
    "status": "draft"
  }'
```

## Secured source example

If the Drupal endpoint needs a token or custom header:

```bash
curl -X POST http://localhost:8080/admin/import/drupal \
  -H "Content-Type: application/json" \
  -d '{
    "admin_email": "auzieman@gmail.com",
    "endpoint_url": "https://your-drupal-site.example/jsonapi/node/article",
    "headers": {
      "Authorization": "Bearer YOUR_TOKEN"
    },
    "params": {
      "page[limit]": "10"
    },
    "source_base_url": "https://your-drupal-site.example",
    "status": "draft"
  }'
```

## Custom field mapping

If your Drupal content type uses different field names, override them with `field_map`.

Example:

```json
{
  "admin_email": "auzieman@gmail.com",
  "endpoint_url": "https://your-drupal-site.example/jsonapi/node/linux_post",
  "params": {
    "include": "field_linux_tags"
  },
  "field_map": {
    "title": "attributes.title",
    "summary": "attributes.field_intro.value",
    "body_html": "attributes.field_article_body.processed",
    "body_raw": "attributes.field_article_body.value",
    "slug": "attributes.path.alias",
    "tags": "attributes.field_keywords"
  },
  "source_base_url": "https://your-drupal-site.example",
  "status": "draft"
}
```

Supported `field_map` keys:

- `title`
- `summary`
- `body_html`
- `body_raw`
- `slug`
- `created_at`
- `updated_at`
- `source_url`
- `tags`
- `hero_image_url`

## Practical migration notes

- Import as `draft` first.
- Review article formatting before publishing.
- Drupal `processed` HTML is usually the best source for first-pass fidelity.
- Imported Drupal HTML should generally use `body_format=html`.
- If you want cleaner markdown later, that can be a second pass; this importer currently prioritizes speed and content preservation.
- The importer stores `source_url` so migrated content still has provenance.
- `theme_variant` should stay minimal and presentation-only.

## Next likely enhancement

If you want repeatable migrations instead of one-shot imports, the next useful addition is:

- paging through Drupal automatically
- storing the original Drupal node id
- idempotent update logic instead of always creating new article ids
