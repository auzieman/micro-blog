import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from pathlib import Path

import pika
import psycopg
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from opentelemetry.metrics import Observation
from opentelemetry.instrumentation.flask import FlaskInstrumentor

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope
from blog_shared.read_model import BlogReadModelStore

configure_logging()
logger = logging.getLogger("microblog.api")
telemetry = BlogTelemetry("blog-api")
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

store = BlogReadModelStore(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://blog:Str0ngP@ssword!@localhost:5432/microblog")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
COMMAND_QUEUE = "blog.commands"
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
SAMPLE_POSTS_PATH = Path(__file__).with_name("sample_posts.json")
CONTENT_IMPORT_ROOT = Path(os.getenv("CONTENT_IMPORT_ROOT", "/content"))
CONTENT_PUBLIC_BASE = os.getenv("CONTENT_PUBLIC_BASE", "/content-files").rstrip("/")
AUTO_IMPORT_FILESYSTEM_ON_BOOT = os.getenv("AUTO_IMPORT_FILESYSTEM_ON_BOOT", "false").lower() == "true"
DEFAULT_PUBLIC_BLOG_LISTING_PATH = os.getenv("DEFAULT_PUBLIC_BLOG_LISTING_PATH", "/blogs")
DEFAULT_DRUPAL_FIELD_MAP = {
    "source_id": "id",
    "source_nid": "attributes.drupal_internal__nid",
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


def parse_front_matter(document: str) -> tuple[dict, str]:
    if not document.startswith("---\n"):
        return {}, document
    end = document.find("\n---\n", 4)
    if end == -1:
        return {}, document
    metadata = {}
    front_matter = document[4:end]
    body = document[end + 5 :]
    current_key = None
    list_accumulator: list[str] = []
    for raw_line in front_matter.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        if line.startswith("  - ") or line.startswith("- "):
            if current_key:
                list_accumulator.append(line.split("- ", 1)[1].strip())
            continue
        if current_key and list_accumulator:
            metadata[current_key] = list_accumulator[:]
            list_accumulator = []
        current_key = None
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if value:
            if value.startswith("[") and value.endswith("]"):
                metadata[key] = [part.strip().strip("\"'") for part in value[1:-1].split(",") if part.strip()]
            else:
                metadata[key] = value
        else:
            current_key = key
            list_accumulator = []
    if current_key and list_accumulator:
        metadata[current_key] = list_accumulator
    return metadata, body


def filesystem_public_url(relative_path: str) -> str:
    normalized = relative_path.strip().replace("\\", "/").lstrip("/")
    return f"{CONTENT_PUBLIC_BASE}/{normalized}"


def rewrite_markdown_asset_paths(body: str, asset_root: str) -> str:
    if not asset_root:
        return body

    def replace_image(match):
        alt_text = match.group(1)
        target = match.group(2).strip()
        if target.startswith(("http://", "https://", "data:", "/", "#")):
            return match.group(0)
        return f"![{alt_text}]({filesystem_public_url(f'{asset_root}/{target}')})"

    def replace_link(match):
        label = match.group(1)
        target = match.group(2).strip()
        if target.startswith(("http://", "https://", "mailto:", "/", "#")):
            return match.group(0)
        lowered = target.lower()
        if not lowered.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".avif")):
            return match.group(0)
        return f"[{label}]({filesystem_public_url(f'{asset_root}/{target}')})"

    body = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace_image, body)
    body = re.sub(r"(?<!!)\[([^\]]+)\]\(([^)]+)\)", replace_link, body)
    return body


def filesystem_preview_items(payload: dict) -> list[dict]:
    root_path = Path(payload.get("root_path") or CONTENT_IMPORT_ROOT).resolve()
    content_subdir = (payload.get("content_subdir") or "").strip().strip("/")
    scan_root = (root_path / content_subdir).resolve() if content_subdir else root_path
    keyword_filter = (payload.get("keyword_filter") or "").strip().lower()
    selected_source_ids = {str(value) for value in (payload.get("selected_source_ids") or [])}
    status = payload.get("status", "draft")
    default_theme_variant = payload.get("theme_variant", "midnight")
    limit = int(payload.get("limit") or 0)
    preview = []

    if not scan_root.exists():
        raise FileNotFoundError(f"Filesystem import root does not exist: {scan_root}")
    if not scan_root.is_dir():
        raise ValueError(f"Filesystem import root is not a directory: {scan_root}")
    if root_path not in [scan_root, *scan_root.parents]:
        raise ValueError("Filesystem import path must stay under the configured content root.")

    files = sorted(scan_root.rglob("*.md"))
    for file_path in files:
        relative_path = file_path.relative_to(root_path).as_posix()
        if selected_source_ids and relative_path not in selected_source_ids:
            continue
        metadata, body = parse_front_matter(file_path.read_text(encoding="utf-8"))
        title = str(metadata.get("title") or file_path.stem.replace("-", " ").title())
        tags = metadata.get("tags") or []
        if isinstance(tags, str):
            tags = [part.strip() for part in tags.split(",") if part.strip()]
        hero_image = metadata.get("hero_image") or metadata.get("hero_image_url")
        asset_root = Path(relative_path).parent.as_posix()
        if asset_root == ".":
            asset_root = ""
        if isinstance(hero_image, str) and hero_image and not hero_image.startswith(("http://", "https://", "/")):
            hero_image = filesystem_public_url(f"{asset_root}/{hero_image}" if asset_root else hero_image)
        rewritten_body = rewrite_markdown_asset_paths(body, asset_root)
        image_paths = re.findall(r"!\[[^\]]*\]\(([^)]+)\)", rewritten_body)
        preview_item = {
            "source_id": relative_path,
            "source_nid": None,
            "title": title,
            "slug": str(metadata.get("slug") or slugify(title)),
            "summary": str(metadata.get("summary") or ""),
            "tags": tags,
            "body_format": str(metadata.get("body_format") or "markdown"),
            "hero_image_url": hero_image or (image_paths[0] if image_paths else None),
            "image_urls": image_paths,
            "theme_variant": str(metadata.get("theme_variant") or metadata.get("theme") or default_theme_variant),
            "source_url": filesystem_public_url(relative_path),
            "markdown_body": rewritten_body,
            "status": str(metadata.get("status") or status),
            "source_path": relative_path,
        }
        if keyword_filter:
            haystack = " ".join(
                [
                    preview_item["title"],
                    preview_item["summary"],
                    preview_item["slug"],
                    " ".join(preview_item["tags"]),
                    preview_item["source_path"],
                ]
            ).lower()
            if keyword_filter not in haystack:
                continue
        preview.append(preview_item)
        if limit and len(preview) >= limit:
            break
    return preview


def filesystem_import_commands(payload: dict) -> list[dict]:
    preview = filesystem_preview_items(payload)
    commands = []
    for item in preview:
        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": f"ART-{uuid.uuid4().hex[:8].upper()}",
                "source_id": item["source_id"],
                "source_nid": item["source_nid"],
                "title": item["title"],
                "slug": item["slug"],
                "summary": item["summary"],
                "body_format": item["body_format"],
                "markdown_body": item["markdown_body"],
                "hero_image_url": item["hero_image_url"],
                "image_urls": item["image_urls"],
                "theme_variant": item["theme_variant"],
                "tags": item["tags"],
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": item["status"],
                "source_url": item["source_url"],
                "source_created_at": None,
                "source_updated_at": None,
                "fault_mode": None,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return commands


def run_boot_filesystem_import() -> None:
    payload = {
        "admin_email": ADMIN_EMAIL,
        "root_path": str(CONTENT_IMPORT_ROOT),
        "status": "draft",
        "theme_variant": "midnight",
    }
    try:
        commands = filesystem_import_commands(payload)
        for command in commands:
            publish_command(command)
        if commands:
            telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
        logger.info(
            "Boot filesystem import complete",
            extra={"event.name": "api.import_filesystem_boot", "count": len(commands), "root_path": str(CONTENT_IMPORT_ROOT)},
        )
    except Exception:
        logger.exception("Boot filesystem import failed", extra={"event.name": "api.import_filesystem_boot"})


def list_admin_posts(page: int = 1, page_size: int = 20) -> dict:
    safe_page = max(page, 1)
    safe_size = max(1, min(page_size, 100))
    offset = (safe_page - 1) * safe_size
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from articles")
            total = cur.fetchone()[0]
            cur.execute(
                """
                select article_id, slug, title, summary, body_format, hero_image_url, theme_variant, tags, status, source_url, updated_at, published_at
                from articles
                order by updated_at desc
                limit %s offset %s
                """,
                (safe_size, offset),
            )
            rows = cur.fetchall()
    items = []
    for row in rows:
        items.append(
            {
                "article_id": row[0],
                "slug": row[1],
                "title": row[2],
                "summary": row[3],
                "body_format": row[4],
                "hero_image_url": row[5],
                "theme_variant": row[6],
                "tags": row[7],
                "status": row[8],
                "source_url": row[9],
                "updated_at": row[10].isoformat() if row[10] else None,
                "published_at": row[11].isoformat() if row[11] else None,
            }
        )
    return {"items": items, "total": total, "page": safe_page, "page_size": safe_size}


def admin_post_status_counts() -> list[dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select status, count(*)
                from articles
                group by status
                """
            )
            rows = cur.fetchall()
    return [{"status": row[0] or "unknown", "count": row[1]} for row in rows]


def observe_article_status(options):
    try:
        counts = admin_post_status_counts()
        total = sum(item["count"] for item in counts)
        observations = [Observation(total, {"status": "all"})]
        for item in counts:
            observations.append(Observation(item["count"], {"status": item["status"]}))
        return observations
    except Exception as exc:
        logger.warning(
            "Article status metric observation failed",
            extra={"event.name": "api.article_status_observe", "error_type": type(exc).__name__},
        )
        return []


telemetry.meter.create_observable_gauge(
    "blog.article.status_total",
    callbacks=[observe_article_status],
    description="Current article counts by status from the write model",
)


def fetch_html_document(url: str, allow_insecure_tls: bool, timeout: int = 20) -> str:
    response = requests.get(url, timeout=timeout, verify=not allow_insecure_tls)
    response.raise_for_status()
    return response.text


def extract_node_links_from_listing(listing_html: str, site_url: str) -> list[dict]:
    soup = BeautifulSoup(listing_html, "html.parser")
    discovered = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        match = re.match(r"^/node/(\d+)$", href)
        if not match:
            continue
        node_id = match.group(1)
        absolute_url = urljoin(site_url.rstrip("/") + "/", href.lstrip("/"))
        title = " ".join(anchor.get_text(" ", strip=True).split())
        if node_id not in discovered:
            discovered[node_id] = {
                "source_nid": node_id,
                "source_url": absolute_url,
                "listing_title": title,
            }
    return list(discovered.values())


def parse_public_article_page(page_html: str, source_url: str) -> dict:
    soup = BeautifulSoup(page_html, "html.parser")
    title = None
    for selector in ("h1.page-title", "h1", "meta[property='og:title']", "title"):
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            title = (node.get("content") or "").strip()
        else:
            title = " ".join(node.get_text(" ", strip=True).split())
        if title:
            break

    content_node = None
    for selector in (
        "article.node",
        "article[role='article']",
        ".node__content",
        ".field--name-body .field__item",
        ".text-formatted",
        "main article",
        "main",
    ):
        content_node = soup.select_one(selector)
        if content_node:
            break

    if not content_node:
        raise ValueError(f"Unable to locate article body for {source_url}")

    image_urls = []
    for image in content_node.select("img[src]"):
        image_urls.append(urljoin(source_url, image.get("src", "").strip()))

    summary = ""
    meta_description = soup.select_one("meta[name='description']")
    if meta_description and meta_description.get("content"):
        summary = meta_description.get("content", "").strip()
    if not summary:
        first_paragraph = content_node.select_one("p")
        if first_paragraph:
            summary = " ".join(first_paragraph.get_text(" ", strip=True).split())

    tags = []
    for anchor in soup.select("a[href*='/taxonomy/term/']"):
        tag_text = " ".join(anchor.get_text(" ", strip=True).split())
        if tag_text and tag_text not in tags:
            tags.append(tag_text)

    return {
        "title": title or source_url.rstrip("/").rsplit("/", 1)[-1],
        "summary": summary,
        "body_html": str(content_node),
        "hero_image_url": image_urls[0] if image_urls else None,
        "image_urls": image_urls,
        "tags": tags,
    }


def public_crawl_preview_items(payload: dict) -> list[dict]:
    site_url = payload["site_url"].rstrip("/")
    listing_url = payload.get("listing_url") or f"{site_url}{DEFAULT_PUBLIC_BLOG_LISTING_PATH}"
    nid_filter = (payload.get("nid_filter") or "").strip()
    keyword_filter = (payload.get("keyword_filter") or "").strip().lower()
    allow_insecure_tls = bool(payload.get("allow_insecure_tls"))
    limit = int(payload.get("limit") or 0)
    selected_source_ids = {str(value) for value in (payload.get("selected_source_ids") or [])}
    theme_variant = payload.get("theme_variant", "midnight")

    preview = []
    node_rows = []
    if nid_filter:
        node_rows = [{"source_nid": nid_filter, "source_url": f"{site_url}/node/{nid_filter}", "listing_title": ""}]
    else:
        listing_html = fetch_html_document(listing_url, allow_insecure_tls)
        node_rows = extract_node_links_from_listing(listing_html, site_url)

    for row in node_rows:
        source_id = row["source_nid"]
        if selected_source_ids and source_id not in selected_source_ids:
            continue
        article_html = fetch_html_document(row["source_url"], allow_insecure_tls)
        parsed = parse_public_article_page(article_html, row["source_url"])
        title = parsed["title"] or row.get("listing_title") or f"node-{row['source_nid']}"
        slug = slugify(title)
        item = {
            "source_id": source_id,
            "source_nid": row["source_nid"],
            "title": title,
            "slug": slug,
            "summary": parsed["summary"],
            "tags": parsed["tags"],
            "body_format": "html",
            "hero_image_url": parsed["hero_image_url"],
            "image_urls": parsed["image_urls"],
            "theme_variant": theme_variant,
            "source_url": row["source_url"],
            "markdown_body": parsed["body_html"],
            "status": payload.get("status", "draft"),
        }
        if keyword_filter:
            haystack = " ".join([item["title"], item["summary"], item["slug"], " ".join(item["tags"]), item["source_url"]]).lower()
            if keyword_filter not in haystack:
                continue
        preview.append(item)
        if limit and len(preview) >= limit:
            break
    return preview


def public_crawl_import_commands(payload: dict) -> list[dict]:
    preview = public_crawl_preview_items(payload)
    commands = []
    for item in preview:
        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": f"ART-{uuid.uuid4().hex[:8].upper()}",
                "source_id": item["source_id"],
                "source_nid": item["source_nid"],
                "title": item["title"],
                "slug": item["slug"],
                "summary": item["summary"],
                "body_format": "html",
                "markdown_body": item["markdown_body"],
                "hero_image_url": item["hero_image_url"],
                "image_urls": item["image_urls"],
                "theme_variant": item["theme_variant"],
                "tags": item["tags"],
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": item["status"],
                "source_url": item["source_url"],
                "source_created_at": None,
                "source_updated_at": None,
                "fault_mode": None,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return commands


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
    nid_filter = (payload.get("nid_filter") or "").strip()
    limit = int(payload.get("limit") or 0)
    selected_source_ids = {str(value) for value in (payload.get("selected_source_ids") or [])}
    commands = []

    for item in items[: limit or None]:
        source_id = str(dig(item, field_map.get("source_id")) or "")
        source_nid = str(dig(item, field_map.get("source_nid")) or "")
        if selected_source_ids and source_id not in selected_source_ids:
            continue
        if nid_filter and source_nid != nid_filter:
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
                "source_nid": source_nid,
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
                "source_nid": command.get("source_nid"),
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
    params = dict(payload.get("params") or {})
    timeout = int(payload.get("timeout_seconds", 20))
    verify_tls = not bool(payload.get("allow_insecure_tls"))
    try:
        response = requests.request(method, endpoint_url, headers=headers, params=params, timeout=timeout, verify=verify_tls)
        response.raise_for_status()
        return response.json(), endpoint_url
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 400 and params.get("include"):
            retry_params = dict(params)
            retry_params.pop("include", None)
            logger.warning(
                "Retrying Drupal fetch without include parameter",
                extra={"event.name": "api.import_drupal_retry", "endpoint_url": endpoint_url, "dropped_include": params.get("include")},
            )
            retry_response = requests.request(
                method,
                endpoint_url,
                headers=headers,
                params=retry_params,
                timeout=timeout,
                verify=verify_tls,
            )
            retry_response.raise_for_status()
            return retry_response.json(), endpoint_url
        raise


def filtered_drupal_index_preview(document: dict) -> list[dict]:
    endpoints = drupal_index_preview(document, "")
    node_endpoints = [item for item in endpoints if "/jsonapi/node/" in item["href"]]
    allowed_suffixes = {"/jsonapi/node/blog_post", "/jsonapi/node/article"}
    preferred = [item for item in node_endpoints if any(item["href"].endswith(suffix) for suffix in allowed_suffixes)]
    return preferred or node_endpoints


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


@app.get("/admin/posts")
def list_admin_posts_route():
    route = "/admin/posts"
    started = time.perf_counter()
    result = "success"
    page = int(request.args.get("page", "1"))
    page_size = int(request.args.get("page_size", "20"))
    admin_email = request.args.get("admin_email")
    with event_scope(logger, "api.admin_list", page=page, page_size=page_size) as log:
        try:
            ensure_admin(admin_email)
            payload = list_admin_posts(page, page_size)
            return jsonify(payload)
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Admin post list failed")
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
                    endpoints = filtered_drupal_index_preview(document)
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
            detail = ""
            if exc.response is not None:
                body = (exc.response.text or "").strip()
                if body:
                    detail = f" Response body: {body[:600]}"
            return jsonify({"error": f"Drupal source request failed: {exc}.{detail}"}), 502
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


@app.post("/admin/import/filesystem")
def import_filesystem():
    route = "/admin/import/filesystem"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    with event_scope(logger, "api.import_filesystem", root_path=payload.get("root_path"), content_subdir=payload.get("content_subdir")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            if payload.get("dry_run"):
                preview = filesystem_preview_items(payload)
                return jsonify({"status": "FilesystemImportPreview", "count": len(preview), "items": preview}), 200

            commands = filesystem_import_commands(payload)
            for command in commands:
                publish_command(command)
            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "FilesystemImportQueued", "count": len(commands)}), 202
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except (FileNotFoundError, ValueError) as exc:
            result = "bad_request"
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            result = "error"
            log.exception("Filesystem import failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/import/public-crawl")
def import_public_crawl():
    route = "/admin/import/public-crawl"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    with event_scope(logger, "api.import_public_crawl", site_url=payload.get("site_url"), listing_url=payload.get("listing_url")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            if payload.get("dry_run"):
                preview = public_crawl_preview_items(payload)
                return jsonify({"status": "PublicCrawlPreview", "count": len(preview), "items": preview}), 200

            commands = public_crawl_import_commands(payload)
            for command in commands:
                publish_command(command)
            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "PublicCrawlQueued", "count": len(commands)}), 202
        except requests.HTTPError as exc:
            result = "source_error"
            detail = ""
            if exc.response is not None:
                body = (exc.response.text or "").strip()
                if body:
                    detail = f" Response body: {body[:600]}"
            return jsonify({"error": f"Public crawl source request failed: {exc}.{detail}"}), 502
        except requests.exceptions.SSLError as exc:
            result = "tls_error"
            return jsonify({"error": f"Public crawl TLS validation failed: {exc}"}), 502
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Public crawl import failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


if __name__ == "__main__":
    if AUTO_IMPORT_FILESYSTEM_ON_BOOT:
        run_boot_filesystem_import()
    app.run(host="0.0.0.0", port=8080)
