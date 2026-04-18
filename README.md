# Micro Blog

`micro-blog` is a single-admin, API-first blog platform built as a real microservice demo:

- Flask API for authoring and read access
- Python worker for markdown render, revision tracking, and publish flow
- Python projection service for the public Redis read model
- Flask UI with a simple Linux-focused blog layout
- Python load generator
- local OTEL collector, Prometheus, Grafana, Loki, and Promtail via Docker Compose

The project is intentionally narrow:

- one site
- one admin identity
- markdown-first posts
- publish workflow instead of a giant CMS surface
- import-friendly article pipeline for Drupal, WordPress, or other sources
- light presentation choices instead of a heavy theme engine

The default public theme is `midnight`.

## Services

- `blog-api`
- `blog-worker`
- `blog-projection`
- `blog-ui`
- `loadgen`

## Local startup

```bash
cd /home/auzieman/Projects/micro-blog
docker compose build
docker compose --profile local-observability up -d
docker compose up -d
docker compose --profile load up -d loadgen
```

Endpoints:

- API: `http://localhost:8080`
- UI: `http://localhost:8081`
- RabbitMQ: `http://localhost:15672`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Loki: `http://localhost:3100`

## Admin model

Current admin identity is configured by `ADMIN_EMAIL` and defaults to:

- `auzieman@gmail.com`

The current scaffold uses a simple API/email gate, not full Google OAuth yet. That is the next auth pass once the local stack is stable.

## Blog API

- `GET /healthz`
- `GET /readyz`
- `GET /fault-modes`
- `GET /posts?page=1&page_size=10`
- `GET /posts/<slug>`
- `POST /admin/posts`
- `PUT /admin/posts/<article_id>`
- `POST /admin/posts/<article_id>/publish`
- `POST /admin/import-sample`
- `POST /admin/import/drupal`
- `POST /admin/import/filesystem`

Article payloads support a small publishing-oriented model:

- `title`
- `summary`
- `body_format` (`markdown` or `html`)
- `markdown_body`
- `hero_image_url`
- `theme_variant` (`aurora`, `paper`, `midnight`)
- `tags`
- `status`

## Sample content

The scaffold includes a few Linux-focused sample posts in:

- [sample_posts.json](./src/api/sample_posts.json)
- [content/posts/linux](./content/posts/linux)

These are paraphrased seed articles inspired by AuzieTek Linux article titles and snippets so you have realistic starting content without copying the original posts verbatim.

## Drupal import

The API now supports a Drupal source import route:

- `POST /admin/import/drupal`

See:

- [docs/drupal-import.md](./docs/drupal-import.md)

The default parser expects Drupal JSON:API-style responses and will try these common fields automatically:

- `attributes.title`
- `attributes.body.summary`
- `attributes.body.processed`
- `attributes.body.value`
- `attributes.path.alias`
- taxonomy tags from `relationships.field_tags.data` plus `included`
- body format inferred from HTML vs raw source
- hero image resolution from `relationships.field_image`

Imported Drupal posts should usually be queued as `draft` first, reviewed, then published.

## Filesystem import

The admin UI also supports previewing and importing local markdown content from the mounted [`content`](./content) directory.

Expected format:

- optional front matter delimited by `---`
- markdown body below it
- adjacent relative images such as `images/banner.svg`

Supported front matter keys:

- `title`
- `summary`
- `tags`
- `slug`
- `hero_image`
- `theme_variant`
- `status`

Relative image paths are rewritten to `/content-files/...` so imported markdown can keep working images without adding a full media library.

If you want this to auto-import on local startup, set:

- `AUTO_IMPORT_FILESYSTEM_ON_BOOT=true`

## Scope guardrails

This project is meant to stay a dynamic publishing system, not a full CMS.

What it should do well:

- author posts
- import posts from systems like Drupal
- render a clean public reading surface
- keep API, worker, projection, and observability first-class

What it should avoid for now:

- multi-user editorial workflows
- deep media management
- block layout builders
- large plugin or theme ecosystems
