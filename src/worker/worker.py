import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import markdown
import pika
import psycopg

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope

configure_logging()
logger = logging.getLogger("microblog.worker")
telemetry = BlogTelemetry("blog-worker")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://blog:Str0ngP@ssword!@localhost:5432/microblog")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
COMMAND_QUEUE = "blog.commands"
EVENT_QUEUE = "blog.events"
_fault_once_seen: set[str] = set()


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\\s-]", "", value).strip().lower()
    return re.sub(r"[-\\s]+", "-", value).strip("-")


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
                      updated_at timestamptz not null,
                      published_at timestamptz
                    )
                    """
                )
                cur.execute("alter table articles add column if not exists body_format text not null default 'markdown'")
                cur.execute("alter table articles add column if not exists hero_image_url text")
                cur.execute("alter table articles add column if not exists theme_variant text not null default 'aurora'")
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
    return markdown.markdown(body, extensions=["fenced_code", "tables"])


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
                        body_format = payload.get("body_format", "markdown")
                        source_body = payload["markdown_body"]
                        html_body = render_body(body_format, source_body)
                        cur.execute("select revision, published_at from articles where article_id = %s", (article_id,))
                        row = cur.fetchone()
                        revision = (row[0] if row else 0) + 1
                        published_at = row[1] if row else None
                        cur.execute(
                            """
                            insert into articles (article_id, slug, title, summary, body_format, markdown_body, html_body, hero_image_url, theme_variant, tags, status, revision, author_email, source_url, updated_at, published_at)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
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
                              updated_at = excluded.updated_at,
                              published_at = excluded.published_at
                            """,
                            (
                                article_id,
                                payload.get("slug") or slugify(payload["title"]),
                                payload["title"],
                                payload.get("summary", ""),
                                body_format,
                                source_body,
                                html_body,
                                payload.get("hero_image_url"),
                                payload.get("theme_variant", "aurora"),
                                json.dumps(payload.get("tags", [])),
                                payload.get("status", "draft"),
                                revision,
                                payload.get("author_email"),
                                payload.get("source_url"),
                                now,
                                published_at if payload.get("status") != "published" else now,
                            ),
                        )
                    elif command_type == "PublishArticleCommand":
                        cur.execute(
                            """
                            update articles
                            set status = 'published', published_at = %s, updated_at = %s
                            where article_id = %s
                            returning article_id, slug, title, summary, body_format, html_body, markdown_body, hero_image_url, theme_variant, tags, revision, author_email, source_url, updated_at, published_at, status
                            """,
                            (now, now, article_id),
                        )
                        if not cur.fetchone():
                            raise KeyError(f"Article {article_id} not found.")
                    else:
                        raise ValueError(f"Unsupported command type: {command_type}")

                    cur.execute(
                        """
                        select article_id, slug, title, summary, body_format, html_body, markdown_body, hero_image_url, theme_variant, tags, revision, author_email, source_url, updated_at, published_at, status
                        from articles
                        where article_id = %s
                        """,
                        (article_id,),
                    )
                    article = cur.fetchone()
                    event = {
                        "event_type": "ArticleChangedEvent",
                        "article_id": article[0],
                        "slug": article[1],
                        "title": article[2],
                        "summary": article[3],
                        "body_format": article[4],
                        "html_body": article[5],
                        "markdown_body": article[6],
                        "hero_image_url": article[7],
                        "theme_variant": article[8],
                        "tags": article[9],
                        "revision": article[10],
                        "author_email": article[11],
                        "source_url": article[12],
                        "updated_at": article[13].isoformat(),
                        "published_at": article[14].isoformat() if article[14] else None,
                        "status": article[15],
                        "fault_mode": fault_mode,
                    }
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
