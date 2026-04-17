import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse
from pathlib import Path

import pika
import requests
from flask import Flask, jsonify, request
from opentelemetry.instrumentation.flask import FlaskInstrumentor

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope
from blog_shared.read_model import BlogReadModelStore

configure_logging()
logger = logging.getLogger("microblog.api")
telemetry = BlogTelemetry("blog-api")
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

store = BlogReadModelStore(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
COMMAND_QUEUE = "blog.commands"
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
SAMPLE_POSTS_PATH = Path(__file__).with_name("sample_posts.json")
DEFAULT_DRUPAL_FIELD_MAP = {
    "source_id": "id",
    "title": "attributes.title",
    "summary": "attributes.body.summary",
    "body_html": "attributes.body.processed",
    "body_raw": "attributes.body.value",
    "slug": "attributes.path.alias",
    "hero_image_url": "relationships.field_image.data.0.meta.drupal_internal__target_id",
    "created_at": "attributes.created",
    "updated_at": "attributes.changed",
    "source_url": "links.self.href",
}


class ImageSrcParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sources: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "img":
            return
        for key, value in attrs:
            if key.lower() == "src" and value:
                self.sources.append(value)


def publish_command(message: dict) -> None:
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=COMMAND_QUEUE,
        body=json.dumps(message).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    connection.close()


def apply_api_fault(fault_mode: str | None) -> None:
    if fault_mode == "api-error":
        raise RuntimeError("Injected API error.")
    if fault_mode == "api-slow":
        time.sleep(5)


def ensure_admin(request_email: str | None) -> None:
    if request_email and request_email.lower() == ADMIN_EMAIL.lower():
        return
    if request.headers.get("X-Admin-Email", "").lower() == ADMIN_EMAIL.lower():
        return
    raise PermissionError("Admin identity required.")


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\\s-]", "", value).strip().lower()
    return re.sub(r"[-\\s]+", "-", value).strip("-") or uuid.uuid4().hex[:8]


def article_stub(slug: str) -> dict:
    return {
        "article_id": slug,
        "slug": slug,
        "title": slug.replace("-", " ").title(),
        "summary": "Draft article placeholder.",
        "body_format": "markdown",
        "markdown_body": "",
        "html_body": "<p>Draft article placeholder.</p>",
        "hero_image_url": None,
        "theme_variant": "aurora",
        "tags": [],
        "status": "draft",
        "revision": 0,
        "author_email": ADMIN_EMAIL,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "published_at": None,
    }


def extract_image_urls_from_html(html: str, source_base_url: str) -> list[str]:
    parser = ImageSrcParser()
    parser.feed(html)
    normalized = []
    for src in parser.sources:
        if src.startswith("/") and source_base_url:
            normalized.append(f"{source_base_url}{src}")
        else:
            normalized.append(src)
    return normalized


def absolutize_url(value: str, source_base_url: str) -> str:
    if value.startswith(("http://", "https://", "data:")):
        return value
    if value.startswith("/") and source_base_url:
        return f"{source_base_url}{value}"
    return value


def normalize_embedded_asset_urls(html: str, source_base_url: str) -> str:
    if not source_base_url or not html:
        return html
    html = re.sub(r'(src=")(/[^"]+)"', lambda m: f'{m.group(1)}{source_base_url}{m.group(2)}"', html)
    html = re.sub(r"(src=\')(/[^']+)\'", lambda m: f"{m.group(1)}{source_base_url}{m.group(2)}'", html)
    html = re.sub(r'(href=")(/[^"]+)"', lambda m: f'{m.group(1)}{source_base_url}{m.group(2)}"', html)
    html = re.sub(r"(href=\')(/[^']+)\'", lambda m: f"{m.group(1)}{source_base_url}{m.group(2)}'", html)
    return html


def normalize_drupal_endpoint_url(endpoint_url: str) -> str:
    endpoint_url = endpoint_url.strip()
    if endpoint_url.endswith("/"):
        endpoint_url = endpoint_url[:-1]
    parsed = urlparse(endpoint_url)
    if parsed.path in ("", "/"):
        return f"{endpoint_url}/jsonapi"
    return endpoint_url


def dig(data: dict | list | None, dotted_path: str | None):
    if data is None or not dotted_path:
        return None

    current = data
    for part in dotted_path.split("."):
        if current is None:
            return None
        if isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                return None
            if index >= len(current):
                return None
            current = current[index]
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def normalize_slug(value: str | None, title: str) -> str:
    if not value:
        return slugify(title)
    value = value.strip()
    if value.startswith("/"):
        value = value.rsplit("/", 1)[-1]
    return slugify(value)


def resolve_drupal_tags(item: dict, included_by_key: dict, field_map: dict) -> list[str]:
    mapped = dig(item, field_map.get("tags"))
    if isinstance(mapped, list):
        values = []
        for entry in mapped:
            if isinstance(entry, str):
                values.append(entry)
            elif isinstance(entry, dict):
                name = entry.get("name") or dig(entry, "attributes.name")
                if name:
                    values.append(name)
        if values:
            return values

    relationship_data = dig(item, "relationships.field_tags.data") or dig(item, "relationships.tags.data") or []
    tags = []
    for ref in relationship_data:
        if not isinstance(ref, dict):
            continue
        key = f"{ref.get('type')}::{ref.get('id')}"
        included = included_by_key.get(key)
        if included:
            name = dig(included, "attributes.name")
            if name:
                tags.append(name)
    return tags


def resolve_drupal_hero_image(item: dict, included_by_key: dict, field_map: dict, source_base_url: str) -> str | None:
    mapped = dig(item, field_map.get("hero_image_url"))
    if isinstance(mapped, str) and mapped.startswith(("http://", "https://")):
        return mapped

    relationship_data = dig(item, "relationships.field_image.data")
    if isinstance(relationship_data, list) and relationship_data:
        relationship_data = relationship_data[0]

    if isinstance(relationship_data, dict):
        key = f"{relationship_data.get('type')}::{relationship_data.get('id')}"
        included = included_by_key.get(key)
        image_url = dig(included, "attributes.uri.url") or dig(included, "links.self.href")
        if isinstance(image_url, str):
            if image_url.startswith("internal:") and source_base_url:
                return f"{source_base_url}{image_url.replace('internal:', '', 1)}"
            if image_url.startswith("/") and source_base_url:
                return f"{source_base_url}{image_url}"
            return image_url
    return mapped if isinstance(mapped, str) else None


def drupal_items_from_response(document: dict, payload: dict) -> list[dict]:
    item_path = payload.get("item_path", "data")
    items = dig(document, item_path)
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return items
    raise ValueError(f"Drupal response path '{item_path}' did not resolve to a list.")


def drupal_import_commands(document: dict, payload: dict) -> list[dict]:
    field_map = {**DEFAULT_DRUPAL_FIELD_MAP, **(payload.get("field_map") or {})}
    items = drupal_items_from_response(document, payload)
    included = document.get("included") or []
    included_by_key = {
        f"{entry.get('type')}::{entry.get('id')}": entry
        for entry in included
        if isinstance(entry, dict)
    }
    source_base_url = payload.get("source_base_url", "").rstrip("/")
    status = payload.get("status", "draft")
    default_body_format = payload.get("body_format")
    default_theme_variant = payload.get("theme_variant", "aurora")
    keyword_filter = (payload.get("keyword_filter") or "").strip().lower()
    limit = int(payload.get("limit") or 0)
    selected_source_ids = {str(value) for value in (payload.get("selected_source_ids") or [])}
    commands = []

    for item in items[: limit or None]:
        source_id = str(dig(item, field_map.get("source_id")) or "")
        if selected_source_ids and source_id not in selected_source_ids:
            continue
        title = dig(item, field_map["title"])
        body_html = dig(item, field_map.get("body_html"))
        body_raw = dig(item, field_map.get("body_raw"))
        body = body_html or body_raw
        if not title or not body:
            continue
        if body_html:
            body_html = normalize_embedded_asset_urls(body_html, source_base_url)
            body = body_html or body_raw
        body_format = default_body_format or ("html" if body_html else "markdown")
        image_urls = extract_image_urls_from_html(body_html, source_base_url) if body_html else []

        raw_slug = dig(item, field_map.get("slug"))
        slug = normalize_slug(raw_slug, title)
        source_url = dig(item, field_map.get("source_url"))
        if source_url and source_base_url and source_url.startswith("/"):
            source_url = f"{source_base_url}{source_url}"
        if not source_url and source_base_url and raw_slug:
            if str(raw_slug).startswith("/"):
                source_url = f"{source_base_url}{raw_slug}"
            else:
                source_url = f"{source_base_url}/{raw_slug}"
        tags = resolve_drupal_tags(item, included_by_key, field_map)
        hero_image_url = resolve_drupal_hero_image(item, included_by_key, field_map, source_base_url)
        if not hero_image_url and image_urls:
            hero_image_url = image_urls[0]

        if keyword_filter:
            haystack = " ".join(
                [
                    title,
                    dig(item, field_map.get("summary")) or "",
                    slug,
                    " ".join(tags),
                    source_url or "",
                ]
            ).lower()
            if keyword_filter not in haystack:
                continue

        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": f"ART-{uuid.uuid4().hex[:8].upper()}",
                "source_id": source_id,
                "title": title,
                "slug": slug,
                "summary": dig(item, field_map.get("summary")) or "",
                "body_format": body_format,
                "markdown_body": body,
                "hero_image_url": hero_image_url,
                "image_urls": image_urls,
                "theme_variant": default_theme_variant,
                "tags": tags,
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": status,
                "source_url": source_url,
                "source_created_at": dig(item, field_map.get("created_at")),
                "source_updated_at": dig(item, field_map.get("updated_at")),
                "fault_mode": None,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return commands


def drupal_preview_items(document: dict, payload: dict) -> list[dict]:
    commands = drupal_import_commands(document, {**payload, "selected_source_ids": []})
    preview = []
    for command in commands:
        preview.append(
            {
                "source_id": command.get("source_id"),
                "title": command["title"],
                "slug": command["slug"],
                "summary": command.get("summary", ""),
                "tags": command.get("tags", []),
                "body_format": command.get("body_format", "markdown"),
                "hero_image_url": command.get("hero_image_url"),
                "image_urls": command.get("image_urls", []),
                "theme_variant": command.get("theme_variant", "aurora"),
                "source_url": command.get("source_url"),
            }
        )
    return preview


def drupal_index_preview(document: dict, endpoint_url: str) -> list[dict]:
    links = document.get("links") or {}
    preview = []
    for name, value in links.items():
        href = value.get("href") if isinstance(value, dict) else value
        if not isinstance(href, str):
            continue
        if "/jsonapi/node/" in href or "/jsonapi/" in href:
            preview.append({"name": name, "href": href})
    preview.sort(key=lambda item: item["href"])
    return preview


def is_drupal_index_document(document: dict) -> bool:
    links = document.get("links")
    data = document.get("data")
    return isinstance(links, dict) and isinstance(data, list) and len(data) == 0


def fetch_drupal_document(payload: dict) -> tuple[dict, str]:
    endpoint_url = normalize_drupal_endpoint_url(payload["endpoint_url"])
    method = payload.get("method", "GET").upper()
    headers = payload.get("headers") or {}
    params = payload.get("params") or {}
    timeout = int(payload.get("timeout_seconds", 20))
    verify_tls = not bool(payload.get("allow_insecure_tls"))
    response = requests.request(method, endpoint_url, headers=headers, params=params, timeout=timeout, verify=verify_tls)
    response.raise_for_status()
    return response.json(), endpoint_url


@app.get("/healthz")
def healthz():
    return jsonify("Healthy")


@app.get("/readyz")
def readyz():
    return jsonify("Healthy")


@app.get("/fault-modes")
def fault_modes():
    return jsonify(
        {
            "api": ["api-error", "api-slow"],
            "worker": ["worker-fail", "worker-slow", "worker-transient-once", "publish-fail"],
            "projection": ["projection-fail", "projection-slow", "cache-fail"],
        }
    )


@app.get("/posts")
def list_posts():
    route = "/posts"
    started = time.perf_counter()
    result = "success"
    page = int(request.args.get("page", "1"))
    page_size = int(request.args.get("page_size", "10"))
    tag = request.args.get("tag")
    with event_scope(logger, "api.list", page=page, page_size=page_size, tag=tag) as log:
        try:
            payload = store.list(page, page_size, tag)
            return jsonify(payload)
        except Exception as exc:
            result = "error"
            log.exception("Post list failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "GET", result, (time.perf_counter() - started) * 1000.0)


@app.get("/posts/<slug>")
def get_post(slug: str):
    route = "/posts/{slug}"
    started = time.perf_counter()
    result = "success"
    fault_mode = request.args.get("faultMode")
    with event_scope(logger, "api.read", slug=slug, fault_mode=fault_mode) as log:
        try:
            apply_api_fault(fault_mode)
            payload = store.get_by_slug(slug) or article_stub(slug)
            return jsonify(payload)
        except Exception as exc:
            result = "error"
            log.exception("Post read failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "GET", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/posts")
def create_post():
    route = "/admin/posts"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    article_id = payload.get("article_id") or f"ART-{uuid.uuid4().hex[:8].upper()}"
    title = payload["title"]
    fault_mode = payload.get("fault_mode")
    with event_scope(logger, "api.create", article_id=article_id, fault_mode=fault_mode) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            apply_api_fault(fault_mode)
            command = {
                "command_type": "UpsertArticleCommand",
                "article_id": article_id,
                "title": title,
                "slug": payload.get("slug") or slugify(title),
                "summary": payload.get("summary", ""),
                "body_format": payload.get("body_format", "markdown"),
                "markdown_body": payload["markdown_body"],
                "hero_image_url": payload.get("hero_image_url"),
                "theme_variant": payload.get("theme_variant", "aurora"),
                "tags": payload.get("tags", []),
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": payload.get("status", "draft"),
                "fault_mode": fault_mode,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
            publish_command(command)
            telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"article_id": article_id, "status": "UpsertQueued"}), 202
        except KeyError as exc:
            result = "bad_request"
            return jsonify({"error": f"Missing field: {exc}"}), 400
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Post create failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.put("/admin/posts/<article_id>")
def update_post(article_id: str):
    route = "/admin/posts/{id}"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    fault_mode = payload.get("fault_mode")
    with event_scope(logger, "api.update", article_id=article_id, fault_mode=fault_mode) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            apply_api_fault(fault_mode)
            command = {
                "command_type": "UpsertArticleCommand",
                "article_id": article_id,
                "title": payload["title"],
                "slug": payload.get("slug") or slugify(payload["title"]),
                "summary": payload.get("summary", ""),
                "body_format": payload.get("body_format", "markdown"),
                "markdown_body": payload["markdown_body"],
                "hero_image_url": payload.get("hero_image_url"),
                "theme_variant": payload.get("theme_variant", "aurora"),
                "tags": payload.get("tags", []),
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": payload.get("status", "draft"),
                "fault_mode": fault_mode,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
            publish_command(command)
            telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"article_id": article_id, "status": "UpdateQueued"}), 202
        except KeyError as exc:
            result = "bad_request"
            return jsonify({"error": f"Missing field: {exc}"}), 400
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Post update failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "PUT", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/posts/<article_id>/publish")
def publish_post(article_id: str):
    route = "/admin/posts/{id}/publish"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(silent=True) or {}
    fault_mode = payload.get("fault_mode")
    with event_scope(logger, "api.publish", article_id=article_id, fault_mode=fault_mode) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            apply_api_fault(fault_mode)
            command = {
                "command_type": "PublishArticleCommand",
                "article_id": article_id,
                "fault_mode": fault_mode,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
            publish_command(command)
            telemetry.publish(COMMAND_QUEUE, "PublishArticleCommand")
            return jsonify({"article_id": article_id, "status": "PublishQueued"}), 202
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Post publish failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/import-sample")
def import_sample():
    route = "/admin/import-sample"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(silent=True) or {}
    with event_scope(logger, "api.import_sample") as log:
        try:
            ensure_admin(payload.get("admin_email"))
            rows = json.loads(SAMPLE_POSTS_PATH.read_text())
            imported = 0
            for row in rows:
                command = {
                    "command_type": "UpsertArticleCommand",
                    "article_id": row.get("article_id") or f"ART-{uuid.uuid4().hex[:8].upper()}",
                    "title": row["title"],
                    "slug": row.get("slug") or slugify(row["title"]),
                    "summary": row["summary"],
                    "body_format": row.get("body_format", "markdown"),
                    "markdown_body": row["markdown_body"],
                    "hero_image_url": row.get("hero_image_url"),
                    "theme_variant": row.get("theme_variant", "aurora"),
                    "tags": row.get("tags", []),
                    "author_email": ADMIN_EMAIL,
                    "status": row.get("status", "draft"),
                    "source_url": row.get("source_url"),
                    "fault_mode": None,
                    "requested_at": datetime.now(timezone.utc).isoformat(),
                }
                publish_command(command)
                imported += 1
            telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "ImportQueued", "count": imported}), 202
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Sample import failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/import/drupal")
def import_drupal():
    route = "/admin/import/drupal"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    with event_scope(logger, "api.import_drupal", endpoint_url=payload.get("endpoint_url")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            document, endpoint_url = fetch_drupal_document(payload)

            if payload.get("dry_run"):
                if is_drupal_index_document(document):
                    endpoints = drupal_index_preview(document, endpoint_url)
                    return (
                        jsonify(
                            {
                                "status": "DrupalEndpointDiscovery",
                                "count": len(endpoints),
                                "endpoint_url": endpoint_url,
                                "endpoints": endpoints,
                            }
                        ),
                        200,
                    )
                preview = drupal_preview_items(document, payload)
                return jsonify({"status": "DrupalImportPreview", "count": len(preview), "items": preview}), 200

            commands = drupal_import_commands(document, payload)

            for command in commands:
                publish_command(command)

            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return (
                jsonify(
                    {
                        "status": "DrupalImportQueued",
                        "count": len(commands),
                        "endpoint_url": endpoint_url,
                    }
                ),
                202,
            )
        except KeyError as exc:
            result = "bad_request"
            return jsonify({"error": f"Missing field: {exc}"}), 400
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except requests.HTTPError as exc:
            result = "source_error"
            return jsonify({"error": f"Drupal source request failed: {exc}"}), 502
        except requests.exceptions.SSLError as exc:
            result = "tls_error"
            return jsonify({"error": f"Drupal source TLS validation failed: {exc}"}), 502
        except Exception as exc:
            result = "error"
            log.exception("Drupal import failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
