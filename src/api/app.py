import json
import logging
import mimetypes
import os
import re
import time
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import markdown
import pika
import psycopg
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.metrics import Observation

from blog_shared import (
    BlogTelemetry,
    build_rss_xml,
    build_sitemap_xml,
    configure_logging,
    event_scope,
    render_post_preview,
    slugify,
    strip_html,
)
from blog_shared.read_model import BlogReadModelStore
from import_utils import coerce_bool, filesystem_preview_items as build_filesystem_preview_items
from import_utils import (
    collect_asset_urls_from_html,
    plan_bootstrap_sync_actions,
    parse_front_matter,
    parse_public_article_page,
    rewrite_html_asset_urls,
    rewrite_markdown_asset_paths,
    stable_import_article_id,
)

configure_logging()
logger = logging.getLogger("microblog.api")
telemetry = BlogTelemetry("blog-api")
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(2 * 1024 * 1024)))

store = BlogReadModelStore(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://blog:Str0ngP@ssword!@localhost:5432/microblog")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
COMMAND_QUEUE = "blog.commands"
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
SAMPLE_POSTS_PATH = Path(__file__).with_name("sample_posts.json")
CONTENT_IMPORT_ROOT = Path(os.getenv("CONTENT_IMPORT_ROOT", "/content"))
CONTENT_PUBLIC_BASE = os.getenv("CONTENT_PUBLIC_BASE", "/content-files").rstrip("/")
IMPORTED_ASSET_DIR = os.getenv("IMPORTED_ASSET_DIR", "imports/assets").strip("/")
AUTO_IMPORT_FILESYSTEM_ON_BOOT = os.getenv("AUTO_IMPORT_FILESYSTEM_ON_BOOT", "false").lower() == "true"
DEFAULT_PUBLIC_BLOG_LISTING_PATH = os.getenv("DEFAULT_PUBLIC_BLOG_LISTING_PATH", "/blogs")
ENABLE_HSTS = coerce_bool(os.getenv("ENABLE_HSTS"), False)
SITE_URL = os.getenv("SITE_URL", "http://localhost:8081").rstrip("/")
SITE_NAME = os.getenv("SITE_NAME", "Micro Blog")
DEFAULT_OG_IMAGE = os.getenv("DEFAULT_OG_IMAGE", "")
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


def render_body(body_format: str, body: str) -> str:
    if body_format == "html":
        return body
    return markdown.markdown(body, extensions=["fenced_code", "tables", "codehilite"])


def filesystem_preview_items(payload: dict) -> list[dict]:
    return build_filesystem_preview_items(payload, slugify, CONTENT_IMPORT_ROOT, CONTENT_PUBLIC_BASE)


def filesystem_import_commands(payload: dict) -> list[dict]:
    preview = filesystem_preview_items(payload)
    commands = []
    for item in preview:
        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": stable_import_article_id("filesystem", item["source_id"], item["slug"]),
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
                "seo_title": item.get("seo_title"),
                "seo_description": item.get("seo_description"),
                "canonical_url": item.get("canonical_url"),
                "og_image_url": item.get("og_image_url"),
                "source_kind": "filesystem",
                "source_fingerprint": item.get("fingerprint"),
                "fault_mode": None,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return commands


def fetch_articles_by_ids(article_ids: list[str]) -> dict[str, dict]:
    if not article_ids:
        return {}
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select article_id, slug, title, summary, body_format, markdown_body, html_body,
                       hero_image_url, theme_variant, tags, status, revision, author_email, source_url,
                       seo_title, seo_description, canonical_url, og_image_url, deleted_at, updated_at, published_at
                from articles
                where article_id = any(%s)
                """,
                (article_ids,),
            )
            rows = cur.fetchall()
    return {row[0]: serialize_article_row(row) for row in rows}


def bootstrap_filesystem_sync(payload: dict) -> dict:
    sync_mode = (payload.get("sync_mode") or "update").strip().lower()
    if sync_mode not in {"skip", "update", "reset"}:
        raise ValueError("sync_mode must be one of: skip, update, reset")

    commands = filesystem_import_commands(payload)
    existing = fetch_articles_by_ids([command["article_id"] for command in commands])
    plan = plan_bootstrap_sync_actions(commands, existing, sync_mode)

    for item in plan["planned"]:
        if item["action"] == "delete":
            publish_command(
                {
                    "command_type": "SoftDeleteArticleCommand",
                    "article_id": item["article_id"],
                    "fault_mode": None,
                    "requested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue
        publish_command(item["command"])

    if plan["count"] or plan["reset_deleted"]:
        telemetry.publish(COMMAND_QUEUE, "BootstrapFilesystemSync")

    return {
        "status": "BootstrapFilesystemSyncQueued",
        "mode": sync_mode,
        "count": plan["count"],
        "skipped": plan["skipped"],
        "reset_deleted": plan["reset_deleted"],
        "items": [
            {
                "article_id": command["article_id"],
                "slug": command["slug"],
                "title": command["title"],
                "source_id": command["source_id"],
                "status": command["status"],
            }
            for command in commands
        ],
    }


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
    except Exception:
        logger.exception("Boot filesystem import failed", extra={"event.name": "api.import_filesystem_boot"})


def ensure_write_model_extensions() -> None:
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("select to_regclass('public.articles')")
            if cur.fetchone()[0] is None:
                return
            cur.execute("alter table articles add column if not exists seo_title text")
            cur.execute("alter table articles add column if not exists seo_description text")
            cur.execute("alter table articles add column if not exists canonical_url text")
            cur.execute("alter table articles add column if not exists og_image_url text")
            cur.execute("alter table articles add column if not exists deleted_at timestamptz")
            cur.execute(
                """
                create table if not exists article_slug_aliases (
                  alias_slug text primary key,
                  article_id text not null references articles(article_id) on delete cascade,
                  created_at timestamptz not null default now()
                )
                """
            )
            cur.execute(
                """
                create table if not exists article_revisions (
                  article_id text not null references articles(article_id) on delete cascade,
                  revision integer not null,
                  snapshot jsonb not null,
                  created_at timestamptz not null default now(),
                  primary key(article_id, revision)
                )
                """
            )


def serialize_article_row(row) -> dict:
    return {
        "article_id": row[0],
        "slug": row[1],
        "title": row[2],
        "summary": row[3],
        "body_format": row[4],
        "markdown_body": row[5],
        "html_body": row[6],
        "hero_image_url": row[7],
        "theme_variant": row[8],
        "tags": row[9],
        "status": row[10],
        "revision": row[11],
        "author_email": row[12],
        "source_url": row[13],
        "seo_title": row[14],
        "seo_description": row[15],
        "canonical_url": row[16],
        "og_image_url": row[17],
        "deleted_at": row[18].isoformat() if row[18] else None,
        "updated_at": row[19].isoformat() if row[19] else None,
        "published_at": row[20].isoformat() if row[20] else None,
    }


def fetch_article_with_aliases(cur, article_id: str) -> dict | None:
    cur.execute(
        """
        select article_id, slug, title, summary, body_format, markdown_body, html_body,
               hero_image_url, theme_variant, tags, status, revision, author_email, source_url,
               seo_title, seo_description, canonical_url, og_image_url, deleted_at, updated_at, published_at
        from articles
        where article_id = %s
        """,
        (article_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    article = serialize_article_row(row)
    cur.execute("select alias_slug from article_slug_aliases where article_id = %s order by created_at asc, alias_slug asc", (article_id,))
    article["slug_aliases"] = [alias_row[0] for alias_row in cur.fetchall()]
    return article


def fetch_admin_post(article_id: str) -> dict | None:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            return fetch_article_with_aliases(cur, article_id)


def list_admin_posts(page: int = 1, page_size: int = 20) -> dict:
    safe_page = max(page, 1)
    safe_size = max(1, min(page_size, 100))
    offset = (safe_page - 1) * safe_size
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) from articles")
                total = cur.fetchone()[0]
                cur.execute(
                    """
                    select article_id, slug, title, summary, body_format, markdown_body, html_body,
                           hero_image_url, theme_variant, tags, status, revision, author_email, source_url,
                           seo_title, seo_description, canonical_url, og_image_url, deleted_at, updated_at, published_at
                    from articles
                    order by updated_at desc
                    limit %s offset %s
                    """,
                    (safe_size, offset),
                )
                items = [serialize_article_row(row) for row in cur.fetchall()]
    except psycopg.errors.UndefinedTable:
        return {"items": [], "total": 0, "page": safe_page, "page_size": safe_size}
    return {"items": items, "total": total, "page": safe_page, "page_size": safe_size}


def list_revisions(article_id: str) -> list[dict]:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select revision, snapshot, created_at
                    from article_revisions
                    where article_id = %s
                    order by revision desc
                    """,
                    (article_id,),
                )
                rows = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        return []
    revisions = []
    for revision, snapshot, created_at in rows:
        revisions.append(
            {
                "revision": revision,
                "created_at": created_at.isoformat() if created_at else None,
                "snapshot": snapshot,
                "title": snapshot.get("title"),
                "slug": snapshot.get("slug"),
                "status": snapshot.get("status"),
            }
        )
    return revisions


def admin_post_status_counts() -> list[dict]:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("select status, count(*) from articles group by status")
                rows = cur.fetchall()
    except psycopg.errors.UndefinedTable:
        return []
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
        logger.warning("Article status metric observation failed", extra={"event.name": "api.article_status_observe", "error_type": type(exc).__name__})
        return []


telemetry.meter.create_observable_gauge(
    "blog.article.status_total",
    callbacks=[observe_article_status],
    description="Current article counts by status from the write model",
)


@app.after_request
def apply_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "img-src 'self' data: https:; "
        "style-src 'self' 'unsafe-inline' https:; "
        "script-src 'self' 'unsafe-inline' https:; "
        "font-src 'self' https: data:; "
        "connect-src 'self' http: https:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'"
    )
    if ENABLE_HSTS:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


def fetch_html_document(url: str, allow_insecure_tls: bool, timeout: int = 20) -> str:
    response = requests.get(url, timeout=timeout, verify=not allow_insecure_tls)
    response.raise_for_status()
    return response.text


def content_public_url(relative_path: Path) -> str:
    return f"{CONTENT_PUBLIC_BASE}/{relative_path.as_posix().lstrip('/')}"


def guess_asset_extension(asset_url: str, content_type: str) -> str:
    path = urlparse(asset_url).path
    suffix = Path(path).suffix.lower()
    if suffix:
        return suffix
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    guessed = mimetypes.guess_extension(normalized) if normalized else None
    return guessed or ".bin"


def download_remote_asset(asset_url: str, source_kind: str, source_id: str, allow_insecure_tls: bool) -> str:
    if not asset_url or asset_url.startswith(("data:", CONTENT_PUBLIC_BASE)):
        return asset_url
    parsed = urlparse(asset_url)
    if parsed.scheme not in {"http", "https"}:
        return asset_url
    source_segment = slugify(source_id) or "import"
    digest = uuid.uuid5(uuid.NAMESPACE_URL, asset_url).hex
    response = requests.get(asset_url, timeout=20, verify=not allow_insecure_tls)
    response.raise_for_status()
    extension = guess_asset_extension(asset_url, response.headers.get("Content-Type", ""))
    relative_path = Path(IMPORTED_ASSET_DIR) / slugify(source_kind) / source_segment / f"{digest}{extension}"
    target_path = CONTENT_IMPORT_ROOT / relative_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(response.content)
    return content_public_url(relative_path)


def localize_import_assets(body_html: str, hero_image_url: str | None, source_kind: str, source_id: str, allow_insecure_tls: bool) -> tuple[str, str | None, list[str]]:
    replacements: dict[str, str] = {}
    localized_urls: list[str] = []
    for asset_url in collect_asset_urls_from_html(body_html):
        try:
            local_url = download_remote_asset(asset_url, source_kind, source_id, allow_insecure_tls)
        except Exception:
            logger.warning("Asset download skipped", extra={"event.name": "api.asset_download_skip", "asset_url": asset_url, "source_kind": source_kind})
            continue
        replacements[asset_url] = local_url
        localized_urls.append(local_url)

    localized_hero = hero_image_url
    if hero_image_url and hero_image_url.startswith(("http://", "https://")):
        try:
            localized_hero = download_remote_asset(hero_image_url, source_kind, source_id, allow_insecure_tls)
        except Exception:
            logger.warning("Hero asset download skipped", extra={"event.name": "api.hero_asset_download_skip", "asset_url": hero_image_url, "source_kind": source_kind})

    localized_body = rewrite_html_asset_urls(body_html, replacements)
    return localized_body, localized_hero, localized_urls


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
            discovered[node_id] = {"source_nid": node_id, "source_url": absolute_url, "listing_title": title}
    return list(discovered.values())


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
        item = {
            "source_id": source_id,
            "source_nid": row["source_nid"],
            "title": title,
            "slug": slugify(title),
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
    localize_assets = payload.get("localize_assets", True)
    allow_insecure_tls = bool(payload.get("allow_insecure_tls"))
    for item in preview:
        body_html = item["markdown_body"]
        hero_image_url = item["hero_image_url"]
        image_urls = item["image_urls"]
        if localize_assets:
            body_html, hero_image_url, image_urls = localize_import_assets(body_html, hero_image_url, "public-crawl", str(item["source_id"]), allow_insecure_tls)
        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": stable_import_article_id("public-crawl", item["source_id"], item["source_url"]),
                "source_id": item["source_id"],
                "source_nid": item["source_nid"],
                "title": item["title"],
                "slug": item["slug"],
                "summary": item["summary"],
                "body_format": "html",
                "markdown_body": body_html,
                "hero_image_url": hero_image_url,
                "image_urls": image_urls,
                "theme_variant": item["theme_variant"],
                "tags": item["tags"],
                "author_email": payload.get("admin_email") or ADMIN_EMAIL,
                "status": item["status"],
                "source_url": item["source_url"],
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


def article_stub(slug: str) -> dict:
    html_body = "<p>Draft article placeholder.</p>"
    return render_post_preview(
        {
            "article_id": slug,
            "slug": slug,
            "title": slug.replace("-", " ").title(),
            "summary": "Draft article placeholder.",
            "body_format": "markdown",
            "markdown_body": "",
            "html_body": html_body,
            "hero_image_url": None,
            "theme_variant": "midnight",
            "tags": [],
            "status": "draft",
            "revision": 0,
            "author_email": ADMIN_EMAIL,
        },
        SITE_URL,
        SITE_NAME,
        DEFAULT_OG_IMAGE or None,
    )["article"]


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
    included_by_key = {f"{entry.get('type')}::{entry.get('id')}": entry for entry in included if isinstance(entry, dict)}
    source_base_url = payload.get("source_base_url", "").rstrip("/")
    status = payload.get("status", "draft")
    default_body_format = payload.get("body_format")
    default_theme_variant = payload.get("theme_variant", "aurora")
    keyword_filter = (payload.get("keyword_filter") or "").strip().lower()
    nid_filter = (payload.get("nid_filter") or "").strip()
    limit = int(payload.get("limit") or 0)
    selected_source_ids = {str(value) for value in (payload.get("selected_source_ids") or [])}
    localize_assets = payload.get("localize_assets", True)
    allow_insecure_tls = bool(payload.get("allow_insecure_tls"))
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
            source_url = f"{source_base_url}/{str(raw_slug).lstrip('/')}"
        tags = resolve_drupal_tags(item, included_by_key, field_map)
        hero_image_url = resolve_drupal_hero_image(item, included_by_key, field_map, source_base_url)
        if not hero_image_url and image_urls:
            hero_image_url = image_urls[0]
        if keyword_filter:
            haystack = " ".join([title, dig(item, field_map.get("summary")) or "", slug, " ".join(tags), source_url or ""]).lower()
            if keyword_filter not in haystack:
                continue
        if body_format == "html" and localize_assets:
            body, hero_image_url, image_urls = localize_import_assets(body, hero_image_url, "drupal", source_id or source_nid or slug, allow_insecure_tls)
        commands.append(
            {
                "command_type": "UpsertArticleCommand",
                "article_id": stable_import_article_id("drupal", source_id, source_url or slug),
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
                "fault_mode": None,
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return commands


def drupal_preview_items(document: dict, payload: dict) -> list[dict]:
    commands = drupal_import_commands(document, {**payload, "selected_source_ids": []})
    return [
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
        for command in commands
    ]


def drupal_index_preview(document: dict) -> list[dict]:
    links = document.get("links") or {}
    preview = []
    for name, value in links.items():
        href = value.get("href") if isinstance(value, dict) else value
        if isinstance(href, str) and "/jsonapi/" in href:
            preview.append({"name": name, "href": href})
    preview.sort(key=lambda item: item["href"])
    return preview


def is_drupal_index_document(document: dict) -> bool:
    return isinstance(document.get("links"), dict) and isinstance(document.get("data"), list) and len(document.get("data", [])) == 0


def fetch_drupal_document(payload: dict) -> tuple[dict, str]:
    endpoint_url = normalize_drupal_endpoint_url(payload["endpoint_url"])
    method = payload.get("method", "GET").upper()
    headers = payload.get("headers") or {}
    params = dict(payload.get("params") or {})
    timeout = int(payload.get("timeout_seconds", 20))
    verify_tls = not bool(payload.get("allow_insecure_tls"))
    response = requests.request(method, endpoint_url, headers=headers, params=params, timeout=timeout, verify=verify_tls)
    response.raise_for_status()
    return response.json(), endpoint_url


def filtered_drupal_index_preview(document: dict) -> list[dict]:
    endpoints = drupal_index_preview(document)
    node_endpoints = [item for item in endpoints if "/jsonapi/node/" in item["href"]]
    preferred = [item for item in node_endpoints if item["href"].endswith("/jsonapi/node/blog_post") or item["href"].endswith("/jsonapi/node/article")]
    return preferred or node_endpoints


def build_upsert_command(payload: dict, article_id: str | None = None) -> dict:
    title = payload["title"]
    return {
        "command_type": "UpsertArticleCommand",
        "article_id": article_id or payload.get("article_id") or f"ART-{uuid.uuid4().hex[:8].upper()}",
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
        "source_url": payload.get("source_url"),
        "seo_title": payload.get("seo_title"),
        "seo_description": payload.get("seo_description"),
        "canonical_url": payload.get("canonical_url"),
        "og_image_url": payload.get("og_image_url"),
        "fault_mode": payload.get("fault_mode"),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }


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
            return jsonify(store.list(page, page_size, tag))
        except Exception as exc:
            result = "error"
            log.exception("Post list failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "GET", result, (time.perf_counter() - started) * 1000.0)


@app.get("/posts/all")
def list_all_posts():
    route = "/posts/all"
    started = time.perf_counter()
    result = "success"
    tag = request.args.get("tag")
    try:
        return jsonify({"items": store.list_all(tag)})
    except Exception as exc:
        result = "error"
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
            payload, canonical_slug = store.resolve_slug(slug)
            if not payload:
                return jsonify({"error": f"Post '{slug}' not found."}), 404
            response = dict(payload)
            if canonical_slug and canonical_slug != slug:
                response["redirect_slug"] = canonical_slug
                response["redirect_url"] = f"/post/{canonical_slug}"
            return jsonify(response)
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
            return jsonify(list_admin_posts(page, page_size))
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


@app.get("/admin/posts/<article_id>")
def get_admin_post(article_id: str):
    route = "/admin/posts/{id}"
    started = time.perf_counter()
    result = "success"
    admin_email = request.args.get("admin_email")
    try:
        ensure_admin(admin_email)
        article = fetch_admin_post(article_id)
        if not article:
            return jsonify({"error": "Article not found."}), 404
        return jsonify(article)
    except PermissionError as exc:
        result = "forbidden"
        return jsonify({"error": str(exc)}), 403
    except Exception as exc:
        result = "error"
        telemetry.error("blog-api", type(exc).__name__)
        return jsonify({"error": str(exc)}), 500
    finally:
        telemetry.api(route, "GET", result, (time.perf_counter() - started) * 1000.0)


@app.get("/admin/posts/<article_id>/revisions")
def get_admin_post_revisions(article_id: str):
    route = "/admin/posts/{id}/revisions"
    started = time.perf_counter()
    result = "success"
    admin_email = request.args.get("admin_email")
    try:
        ensure_admin(admin_email)
        return jsonify({"article_id": article_id, "items": list_revisions(article_id)})
    except PermissionError as exc:
        result = "forbidden"
        return jsonify({"error": str(exc)}), 403
    except Exception as exc:
        result = "error"
        telemetry.error("blog-api", type(exc).__name__)
        return jsonify({"error": str(exc)}), 500
    finally:
        telemetry.api(route, "GET", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/posts/preview")
def preview_post():
    route = "/admin/posts/preview"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    try:
        ensure_admin(payload.get("admin_email"))
        article = dict(payload)
        article["html_body"] = render_body(article.get("body_format", "markdown"), article.get("markdown_body", ""))
        preview = render_post_preview(article, SITE_URL, SITE_NAME, DEFAULT_OG_IMAGE or None)
        preview["article"]["html_body"] = article["html_body"]
        return jsonify(preview)
    except PermissionError as exc:
        result = "forbidden"
        return jsonify({"error": str(exc)}), 403
    except Exception as exc:
        result = "error"
        telemetry.error("blog-api", type(exc).__name__)
        return jsonify({"error": str(exc)}), 500
    finally:
        telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/posts")
def create_post():
    route = "/admin/posts"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    article_id = payload.get("article_id") or f"ART-{uuid.uuid4().hex[:8].upper()}"
    with event_scope(logger, "api.create", article_id=article_id, fault_mode=payload.get("fault_mode")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            apply_api_fault(payload.get("fault_mode"))
            command = build_upsert_command(payload, article_id)
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
    with event_scope(logger, "api.update", article_id=article_id, fault_mode=payload.get("fault_mode")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            apply_api_fault(payload.get("fault_mode"))
            command = build_upsert_command(payload, article_id)
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


def queue_simple_command(route: str, command_type: str, article_id: str, payload: dict):
    started = time.perf_counter()
    result = "success"
    try:
        ensure_admin(payload.get("admin_email"))
        apply_api_fault(payload.get("fault_mode"))
        command = {
            "command_type": command_type,
            "article_id": article_id,
            "restore_status": payload.get("restore_status"),
            "fault_mode": payload.get("fault_mode"),
            "requested_at": datetime.now(timezone.utc).isoformat(),
        }
        publish_command(command)
        telemetry.publish(COMMAND_QUEUE, command_type)
        return jsonify({"article_id": article_id, "status": f"{command_type}Queued"}), 202
    except PermissionError as exc:
        result = "forbidden"
        return jsonify({"error": str(exc)}), 403
    except Exception as exc:
        result = "error"
        telemetry.error("blog-api", type(exc).__name__)
        return jsonify({"error": str(exc)}), 500
    finally:
        telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/posts/<article_id>/publish")
def publish_post(article_id: str):
    return queue_simple_command("/admin/posts/{id}/publish", "PublishArticleCommand", article_id, request.get_json(silent=True) or {})


@app.post("/admin/posts/<article_id>/unpublish")
def unpublish_post(article_id: str):
    return queue_simple_command("/admin/posts/{id}/unpublish", "UnpublishArticleCommand", article_id, request.get_json(silent=True) or {})


@app.post("/admin/posts/<article_id>/delete")
def delete_post(article_id: str):
    return queue_simple_command("/admin/posts/{id}/delete", "SoftDeleteArticleCommand", article_id, request.get_json(silent=True) or {})


@app.post("/admin/posts/<article_id>/restore")
def restore_post(article_id: str):
    return queue_simple_command("/admin/posts/{id}/restore", "RestoreArticleCommand", article_id, request.get_json(silent=True) or {})


@app.post("/admin/posts/<article_id>/remirror")
def remirror_post(article_id: str):
    return queue_simple_command("/admin/posts/{id}/remirror", "RemirrorArticleCommand", article_id, request.get_json(silent=True) or {})


@app.post("/admin/posts/<article_id>/hard-delete")
def hard_delete_post(article_id: str):
    payload = request.get_json(silent=True) or {}
    confirmation = (payload.get("confirm_article_id") or "").strip()
    if confirmation != article_id:
        return jsonify({"error": "Hard delete confirmation must match the article ID exactly."}), 400
    return queue_simple_command("/admin/posts/{id}/hard-delete", "HardDeleteArticleCommand", article_id, payload)


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
            for row in rows:
                publish_command(build_upsert_command({**row, "admin_email": ADMIN_EMAIL}, row.get("article_id")))
            telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "SampleImportQueued", "count": len(rows)}), 202
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
            if is_drupal_index_document(document):
                endpoints = filtered_drupal_index_preview(document)
                return jsonify({"status": "DrupalEndpointDiscovery", "endpoint_url": endpoint_url, "count": len(endpoints), "endpoints": endpoints})
            if payload.get("dry_run"):
                items = drupal_preview_items(document, payload)
                return jsonify({"status": "DrupalPreviewReady", "endpoint_url": endpoint_url, "count": len(items), "items": items})
            commands = drupal_import_commands(document, payload)
            for command in commands:
                publish_command(command)
            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "DrupalImportQueued", "endpoint_url": endpoint_url, "count": len(commands)}), 202
        except requests.exceptions.SSLError as exc:
            result = "bad_gateway"
            return jsonify({"error": str(exc)}), 502
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
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
    with event_scope(logger, "api.import_filesystem", root_path=payload.get("root_path")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            if payload.get("dry_run"):
                items = filesystem_preview_items(payload)
                return jsonify({"status": "FilesystemPreviewReady", "count": len(items), "items": items})
            commands = filesystem_import_commands(payload)
            for command in commands:
                publish_command(command)
            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "FilesystemImportQueued", "count": len(commands)}), 202
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Filesystem import failed")
            telemetry.error("blog-api", type(exc).__name__)
            return jsonify({"error": str(exc)}), 500
        finally:
            telemetry.api(route, "POST", result, (time.perf_counter() - started) * 1000.0)


@app.post("/admin/bootstrap/filesystem-sync")
def bootstrap_filesystem():
    route = "/admin/bootstrap/filesystem-sync"
    started = time.perf_counter()
    result = "success"
    payload = request.get_json(force=True)
    with event_scope(logger, "api.bootstrap_filesystem", sync_mode=payload.get("sync_mode")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            sync_payload = {
                "root_path": payload.get("root_path") or str(CONTENT_IMPORT_ROOT),
                "content_subdir": payload.get("content_subdir", "").strip(),
                "status": payload.get("status", "published"),
                "theme_variant": payload.get("theme_variant", "midnight"),
                "keyword_filter": payload.get("keyword_filter", "").strip(),
                "limit": payload.get("limit"),
                "sync_mode": payload.get("sync_mode", "update"),
                "admin_email": payload.get("admin_email") or ADMIN_EMAIL,
            }
            return jsonify(bootstrap_filesystem_sync(sync_payload)), 202
        except PermissionError as exc:
            result = "forbidden"
            return jsonify({"error": str(exc)}), 403
        except Exception as exc:
            result = "error"
            log.exception("Filesystem bootstrap sync failed")
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
    with event_scope(logger, "api.import_public_crawl", site_url=payload.get("site_url")) as log:
        try:
            ensure_admin(payload.get("admin_email"))
            if payload.get("dry_run"):
                items = public_crawl_preview_items(payload)
                return jsonify({"status": "PublicCrawlPreviewReady", "count": len(items), "items": items})
            commands = public_crawl_import_commands(payload)
            for command in commands:
                publish_command(command)
            if commands:
                telemetry.publish(COMMAND_QUEUE, "UpsertArticleCommand")
            return jsonify({"status": "PublicCrawlImportQueued", "count": len(commands)}), 202
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
    ensure_write_model_extensions()
    if AUTO_IMPORT_FILESYSTEM_ON_BOOT:
        run_boot_filesystem_import()
    app.run(host="0.0.0.0", port=8080)
