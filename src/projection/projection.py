import json
import logging
import os
import time

import pika

from blog_shared.observability import BlogTelemetry, configure_logging, event_scope
from blog_shared.read_model import BlogReadModelStore

configure_logging()
logger = logging.getLogger("microblog.projection")
telemetry = BlogTelemetry("blog-projection")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
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
            if payload.get("status") == "published":
                store.upsert(payload, ttl_seconds=86400)
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
