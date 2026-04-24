import json
import logging
import os
import time

import pika
import psycopg

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope
from blog_shared.read_model import BlogReadModelStore

configure_logging()
logger = logging.getLogger("microblog.projection")
telemetry = BlogTelemetry("blog-projection")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "dbname=microblog user=blog password=Str0ngP@ssword! host=localhost port=5432")
EVENT_QUEUE = "blog.events"
store = BlogReadModelStore(REDIS_URL)


def apply_projection_fault(fault_mode: str | None) -> None:
    if fault_mode == "projection-fail":
        raise RuntimeError("Injected projection failure.")
    if fault_mode == "projection-slow":
        time.sleep(5)
    if fault_mode == "cache-fail":
        time.sleep(2)
        raise RuntimeError("Injected cache failure.")


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


def backfill_published_articles() -> int:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select a.article_id, a.slug, a.title, a.summary, a.body_format, a.html_body, a.markdown_body,
                       a.hero_image_url, a.theme_variant, a.tags, a.revision, a.author_email, a.source_url,
                       a.updated_at, a.published_at, a.status, a.seo_title, a.seo_description,
                       a.canonical_url, a.og_image_url, a.deleted_at,
                       coalesce(
                         (
                           select jsonb_agg(alias_slug order by created_at asc, alias_slug asc)
                           from article_slug_aliases
                           where article_id = a.article_id
                         ),
                         '[]'::jsonb
                       ) as slug_aliases
                from articles a
                where a.status = 'published' and a.deleted_at is null
                order by a.updated_at desc
                """
            )
            rows = cur.fetchall()
    count = 0
    for article in rows:
        payload = {
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
            "updated_at": article[13].isoformat() if article[13] else None,
            "published_at": article[14].isoformat() if article[14] else None,
            "status": article[15],
            "seo_title": article[16],
            "seo_description": article[17],
            "canonical_url": article[18],
            "og_image_url": article[19],
            "deleted_at": article[20].isoformat() if article[20] else None,
            "slug_aliases": article[21] or [],
            "fault_mode": None,
        }
        store.upsert(payload)
        count += 1
    return count


def handle_event(ch, method, properties, body):
    started = time.perf_counter()
    result = "success"
    payload = json.loads(body.decode("utf-8"))
    event_type = payload["event_type"]
    article_id = payload["article_id"]
    fault_mode = payload.get("fault_mode")
    with event_scope(logger, "projection.update", article_id=article_id, event_type=event_type, fault_mode=fault_mode) as log:
        try:
            apply_projection_fault(fault_mode)
            cache_started = time.perf_counter()
            if payload.get("status") == "published" and not payload.get("deleted_at"):
                store.upsert(payload)
            else:
                store.remove(article_id)
            telemetry.cache("redis", "upsert", "success", (time.perf_counter() - cache_started) * 1000.0)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            result = "failure"
            log.exception("Projection update failed")
            telemetry.error("blog-projection", type(exc).__name__)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            telemetry.queue(EVENT_QUEUE, "consume", event_type, result, (time.perf_counter() - started) * 1000.0)


def main() -> None:
    ensure_write_model_extensions()
    backfilled = backfill_published_articles()
    logger.info(
        "Projection backfilled published articles",
        extra={"event.name": "projection.backfill", "count": backfilled},
    )
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=EVENT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=8)
    channel.basic_consume(queue=EVENT_QUEUE, on_message_callback=handle_event)
    logger.info("Projection consuming events", extra={"event.name": "projection.start", "queue": EVENT_QUEUE})
    channel.start_consuming()


if __name__ == "__main__":
    main()
