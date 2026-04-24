import json
import logging
import os
import time
from datetime import datetime, timezone

import markdown
import pika
import psycopg

from blog_shared import BlogTelemetry, configure_logging, event_scope, slugify

configure_logging()
logger = logging.getLogger("microblog.worker")
telemetry = BlogTelemetry("blog-worker")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://blog:Str0ngP@ssword!@localhost:5432/microblog")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
COMMAND_QUEUE = "blog.commands"
EVENT_QUEUE = "blog.events"
_fault_once_seen: set[str] = set()


def ensure_schema() -> None:
    started = time.perf_counter()
    result = "success"
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists articles (
                      article_id text primary key,
                      slug text not null unique,
                      title text not null,
                      summary text not null,
                      body_format text not null default 'markdown',
                      markdown_body text not null,
                      html_body text not null,
                      hero_image_url text,
                      theme_variant text not null default 'aurora',
                      tags jsonb not null default '[]'::jsonb,
                      status text not null,
                      revision integer not null default 1,
                      author_email text not null,
                      source_url text,
                      seo_title text,
                      seo_description text,
                      canonical_url text,
                      og_image_url text,
                      deleted_at timestamptz,
                      updated_at timestamptz not null,
                      published_at timestamptz
                    )
                    """
                )
                cur.execute("alter table articles add column if not exists body_format text not null default 'markdown'")
                cur.execute("alter table articles add column if not exists hero_image_url text")
                cur.execute("alter table articles add column if not exists theme_variant text not null default 'aurora'")
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
    except Exception:
        result = "error"
        raise
    finally:
        telemetry.db("ensure_schema", result, (time.perf_counter() - started) * 1000.0)


def apply_worker_fault(fault_mode: str | None, article_id: str) -> None:
    if fault_mode == "worker-fail":
        raise RuntimeError("Injected worker failure.")
    if fault_mode == "worker-slow":
        time.sleep(5)
    if fault_mode == "worker-transient-once" and article_id not in _fault_once_seen:
        _fault_once_seen.add(article_id)
        raise RuntimeError("Injected transient worker failure.")


def publish_event(channel, event: dict, fault_mode: str | None) -> None:
    if fault_mode == "publish-fail":
        raise RuntimeError("Injected publish failure.")
    channel.queue_declare(queue=EVENT_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=EVENT_QUEUE,
        body=json.dumps(event).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )


def render_body(body_format: str, body: str) -> str:
    if body_format == "html":
        return body
    return markdown.markdown(body, extensions=["fenced_code", "tables", "codehilite"])


def resolve_unique_slug(cur, desired_slug: str, article_id: str) -> str:
    base = slugify(desired_slug)
    candidate = base
    counter = 2
    while True:
        cur.execute(
            """
            select 1
            from articles
            where slug = %s and article_id <> %s
            """,
            (candidate, article_id),
        )
        article_conflict = cur.fetchone() is not None
        cur.execute(
            """
            select 1
            from article_slug_aliases
            where alias_slug = %s and article_id <> %s
            """,
            (candidate, article_id),
        )
        alias_conflict = cur.fetchone() is not None
        if not article_conflict and not alias_conflict:
            return candidate
        candidate = f"{base}-{counter}"
        counter += 1


def fetch_article(cur, article_id: str) -> dict | None:
    cur.execute(
        """
        select article_id, slug, title, summary, body_format, markdown_body, html_body,
               hero_image_url, theme_variant, tags, status, revision, author_email,
               source_url, seo_title, seo_description, canonical_url, og_image_url,
               deleted_at, updated_at, published_at
        from articles
        where article_id = %s
        """,
        (article_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cur.execute(
        """
        select alias_slug
        from article_slug_aliases
        where article_id = %s
        order by created_at asc, alias_slug asc
        """,
        (article_id,),
    )
    aliases = [alias_row[0] for alias_row in cur.fetchall()]
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
        "slug_aliases": aliases,
    }


def write_revision(cur, article: dict) -> None:
    cur.execute(
        """
        insert into article_revisions (article_id, revision, snapshot, created_at)
        values (%s, %s, %s::jsonb, %s)
        on conflict (article_id, revision) do update set
          snapshot = excluded.snapshot,
          created_at = excluded.created_at
        """,
        (
            article["article_id"],
            article["revision"],
            json.dumps(article),
            datetime.now(timezone.utc),
        ),
    )


def upsert_article(cur, article_id: str, payload: dict, now: datetime) -> dict:
    existing = fetch_article(cur, article_id)
    desired_slug = payload.get("slug") or payload.get("title") or article_id
    slug = resolve_unique_slug(cur, desired_slug, article_id)
    if existing and existing["slug"] != slug:
        cur.execute(
            """
            insert into article_slug_aliases (alias_slug, article_id, created_at)
            values (%s, %s, %s)
            on conflict (alias_slug) do update set
              article_id = excluded.article_id,
              created_at = excluded.created_at
            """,
            (existing["slug"], article_id, now),
        )
    cur.execute("delete from article_slug_aliases where alias_slug = %s and article_id = %s", (slug, article_id))

    body_format = payload.get("body_format", existing["body_format"] if existing else "markdown")
    markdown_body = payload.get("markdown_body", existing["markdown_body"] if existing else "")
    html_body = render_body(body_format, markdown_body)
    status = payload.get("status", existing["status"] if existing else "draft")
    revision = (existing["revision"] if existing else 0) + 1
    previously_published_at = existing["published_at"] if existing else None
    published_at = previously_published_at
    deleted_at = None
    if status == "published":
        published_at = previously_published_at or now.isoformat()
    elif status == "deleted":
        deleted_at = now.isoformat()

    cur.execute(
        """
        insert into articles (
          article_id, slug, title, summary, body_format, markdown_body, html_body,
          hero_image_url, theme_variant, tags, status, revision, author_email,
          source_url, seo_title, seo_description, canonical_url, og_image_url,
          deleted_at, updated_at, published_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (article_id) do update set
          slug = excluded.slug,
          title = excluded.title,
          summary = excluded.summary,
          body_format = excluded.body_format,
          markdown_body = excluded.markdown_body,
          html_body = excluded.html_body,
          hero_image_url = excluded.hero_image_url,
          theme_variant = excluded.theme_variant,
          tags = excluded.tags,
          status = excluded.status,
          revision = excluded.revision,
          author_email = excluded.author_email,
          source_url = excluded.source_url,
          seo_title = excluded.seo_title,
          seo_description = excluded.seo_description,
          canonical_url = excluded.canonical_url,
          og_image_url = excluded.og_image_url,
          deleted_at = excluded.deleted_at,
          updated_at = excluded.updated_at,
          published_at = excluded.published_at
        """,
        (
            article_id,
            slug,
            payload["title"],
            payload.get("summary", ""),
            body_format,
            markdown_body,
            html_body,
            payload.get("hero_image_url"),
            payload.get("theme_variant", existing["theme_variant"] if existing else "aurora"),
            json.dumps(payload.get("tags", existing["tags"] if existing else [])),
            status,
            revision,
            payload.get("author_email") or (existing["author_email"] if existing else ""),
            payload.get("source_url", existing["source_url"] if existing else None),
            payload.get("seo_title"),
            payload.get("seo_description"),
            payload.get("canonical_url"),
            payload.get("og_image_url"),
            deleted_at,
            now,
            datetime.fromisoformat(published_at) if published_at else None,
        ),
    )
    article = fetch_article(cur, article_id)
    if not article:
        raise KeyError(f"Article {article_id} not found after upsert.")
    write_revision(cur, article)
    return article


def publish_article(cur, article_id: str, now: datetime) -> dict:
    existing = fetch_article(cur, article_id)
    if not existing:
        raise KeyError(f"Article {article_id} not found.")
    cur.execute(
        """
        update articles
        set status = 'published',
            deleted_at = null,
            revision = revision + 1,
            updated_at = %s,
            published_at = coalesce(published_at, %s)
        where article_id = %s
        """,
        (now, now, article_id),
    )
    article = fetch_article(cur, article_id)
    write_revision(cur, article)
    return article


def soft_delete_article(cur, article_id: str, now: datetime) -> dict:
    existing = fetch_article(cur, article_id)
    if not existing:
        raise KeyError(f"Article {article_id} not found.")
    cur.execute(
        """
        update articles
        set status = 'deleted',
            deleted_at = %s,
            revision = revision + 1,
            updated_at = %s
        where article_id = %s
        """,
        (now, now, article_id),
    )
    article = fetch_article(cur, article_id)
    write_revision(cur, article)
    return article


def restore_article(cur, article_id: str, payload: dict, now: datetime) -> dict:
    existing = fetch_article(cur, article_id)
    if not existing:
        raise KeyError(f"Article {article_id} not found.")
    restore_status = payload.get("restore_status")
    if not restore_status:
        restore_status = "published" if existing.get("published_at") else "draft"
    cur.execute(
        """
        update articles
        set status = %s,
            deleted_at = null,
            revision = revision + 1,
            updated_at = %s
        where article_id = %s
        """,
        (restore_status, now, article_id),
    )
    article = fetch_article(cur, article_id)
    write_revision(cur, article)
    return article


def unpublish_article(cur, article_id: str, now: datetime) -> dict:
    existing = fetch_article(cur, article_id)
    if not existing:
        raise KeyError(f"Article {article_id} not found.")
    cur.execute(
        """
        update articles
        set status = 'draft',
            deleted_at = null,
            revision = revision + 1,
            updated_at = %s
        where article_id = %s
        """,
        (now, article_id),
    )
    article = fetch_article(cur, article_id)
    write_revision(cur, article)
    return article


def remirror_article(cur, article_id: str) -> dict:
    article = fetch_article(cur, article_id)
    if not article:
        raise KeyError(f"Article {article_id} not found.")
    return article


def hard_delete_article(cur, article_id: str) -> dict:
    article = fetch_article(cur, article_id)
    if not article:
        raise KeyError(f"Article {article_id} not found.")
    cur.execute("delete from articles where article_id = %s", (article_id,))
    return {
        "article_id": article_id,
        "slug": article["slug"],
        "title": article["title"],
        "summary": article.get("summary", ""),
        "body_format": article.get("body_format", "markdown"),
        "html_body": article.get("html_body", ""),
        "markdown_body": article.get("markdown_body", ""),
        "hero_image_url": article.get("hero_image_url"),
        "theme_variant": article.get("theme_variant"),
        "tags": article.get("tags", []),
        "revision": article.get("revision", 0) + 1,
        "author_email": article.get("author_email"),
        "source_url": article.get("source_url"),
        "seo_title": article.get("seo_title"),
        "seo_description": article.get("seo_description"),
        "canonical_url": article.get("canonical_url"),
        "og_image_url": article.get("og_image_url"),
        "deleted_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "published_at": article.get("published_at"),
        "status": "hard_deleted",
        "slug_aliases": article.get("slug_aliases", []),
    }


def article_event(article: dict, fault_mode: str | None) -> dict:
    return {
        "event_type": "ArticleChangedEvent",
        **article,
        "fault_mode": fault_mode,
    }


def handle_command(ch, method, properties, body):
    started = time.perf_counter()
    result = "success"
    payload = json.loads(body.decode("utf-8"))
    command_type = payload["command_type"]
    article_id = payload["article_id"]
    fault_mode = payload.get("fault_mode")

    with event_scope(logger, "worker.consume", article_id=article_id, command_type=command_type, fault_mode=fault_mode) as log:
        try:
            apply_worker_fault(fault_mode, article_id)
            now = datetime.now(timezone.utc)
            with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
                with conn.cursor() as cur:
                    if command_type == "UpsertArticleCommand":
                        article = upsert_article(cur, article_id, payload, now)
                    elif command_type == "PublishArticleCommand":
                        article = publish_article(cur, article_id, now)
                    elif command_type == "SoftDeleteArticleCommand":
                        article = soft_delete_article(cur, article_id, now)
                    elif command_type == "RestoreArticleCommand":
                        article = restore_article(cur, article_id, payload, now)
                    elif command_type == "UnpublishArticleCommand":
                        article = unpublish_article(cur, article_id, now)
                    elif command_type == "RemirrorArticleCommand":
                        article = remirror_article(cur, article_id)
                    elif command_type == "HardDeleteArticleCommand":
                        article = hard_delete_article(cur, article_id)
                    else:
                        raise ValueError(f"Unsupported command type: {command_type}")

            event = article_event(article, fault_mode)
            telemetry.db(command_type, "success", 1.0)
            publish_event(ch, event, fault_mode)
            telemetry.publish(EVENT_QUEUE, event["event_type"])
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            result = "failure"
            log.exception("Worker command handling failed")
            telemetry.error("blog-worker", type(exc).__name__)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=fault_mode == "worker-transient-once")
        finally:
            telemetry.queue(COMMAND_QUEUE, "consume", command_type, result, (time.perf_counter() - started) * 1000.0)


def main() -> None:
    ensure_schema()
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=8)
    channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=handle_command)
    logger.info("Worker consuming commands", extra={"event.name": "worker.start", "queue": COMMAND_QUEUE})
    channel.start_consuming()


if __name__ == "__main__":
    main()
