import json
import os
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from blog_shared.observability import BlogTelemetry, configure_logging

configure_logging()
telemetry = BlogTelemetry(os.getenv("SERVICE_NAME", "microblog-loadgen"))

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "auzieman@gmail.com")
RPS = int(os.getenv("RPS", "2"))
DURATION = int(os.getenv("DURATION_SECONDS", "0"))
WORKERS = int(os.getenv("WORKERS", "4"))
MIX_READ = float(os.getenv("MIX_READ", "0.50"))
MIX_CREATE = float(os.getenv("MIX_CREATE", "0.20"))
MIX_PUBLISH = float(os.getenv("MIX_PUBLISH", "0.20"))
MIX_LIST = float(os.getenv("MIX_LIST", "0.10"))
FAULT_RATE = float(os.getenv("FAULT_RATE", "0.0"))
PROGRESS_INTERVAL_SECONDS = int(os.getenv("PROGRESS_INTERVAL_SECONDS", "30"))

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})
_articles: list[dict] = []
_lock = threading.Lock()


def log_event(event_name: str, **fields):
    print(json.dumps({"event.domain": "microblog", "event.name": event_name, **fields}, sort_keys=True), flush=True)


def make_title() -> str:
    noun = random.choice(["Bash", "Linux", "Docker", "Kubernetes", "Regex", "Observability"])
    suffix = "".join(random.choices(string.ascii_uppercase, k=4))
    return f"{noun} Trick {suffix}"


def remember(article_id: str, slug: str) -> None:
    with _lock:
        if not any(item["article_id"] == article_id for item in _articles):
            _articles.append({"article_id": article_id, "slug": slug})


def pick_article() -> dict | None:
    with _lock:
        return random.choice(_articles) if _articles else None


def maybe_fault() -> str | None:
    if FAULT_RATE <= 0:
        return None
    return random.choice(["api-slow", "worker-slow", "projection-slow"]) if random.random() < FAULT_RATE else None


def do_list():
    started = time.perf_counter()
    try:
        response = session.get(f"{BASE_URL}/posts", params={"page": 1, "page_size": 10}, timeout=5)
        response.raise_for_status()
        telemetry.synthetic("list", "ok", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        telemetry.synthetic("list", "error", (time.perf_counter() - started) * 1000.0, type(exc).__name__)


def do_read():
    article = pick_article()
    if not article:
        return do_list()
    started = time.perf_counter()
    try:
        response = session.get(f"{BASE_URL}/posts/{article['slug']}", timeout=5)
        response.raise_for_status()
        telemetry.synthetic("read", "ok", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        telemetry.synthetic("read", "error", (time.perf_counter() - started) * 1000.0, type(exc).__name__)


def do_create():
    title = make_title()
    payload = {
        "title": title,
        "summary": f"A short operations note about {title.lower()}",
        "markdown_body": f"# {title}\n\nThis is synthetic seed content for the micro blog.",
        "tags": ["linux", "automation"],
        "status": "draft",
        "admin_email": ADMIN_EMAIL,
        "fault_mode": maybe_fault(),
    }
    started = time.perf_counter()
    try:
        response = session.post(f"{BASE_URL}/admin/posts", json=payload, timeout=5)
        response.raise_for_status()
        body = response.json()
        article_id = body["article_id"]
        slug = title.lower().replace(" ", "-")
        remember(article_id, slug)
        telemetry.synthetic("create", "ok", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        telemetry.synthetic("create", "error", (time.perf_counter() - started) * 1000.0, type(exc).__name__)


def do_publish():
    article = pick_article()
    if not article:
        return do_create()
    started = time.perf_counter()
    try:
        response = session.post(
            f"{BASE_URL}/admin/posts/{article['article_id']}/publish",
            json={"admin_email": ADMIN_EMAIL, "fault_mode": maybe_fault()},
            timeout=5,
        )
        response.raise_for_status()
        telemetry.synthetic("publish", "ok", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        telemetry.synthetic("publish", "error", (time.perf_counter() - started) * 1000.0, type(exc).__name__)


def pick_action():
    total = MIX_READ + MIX_CREATE + MIX_PUBLISH + MIX_LIST
    roll = random.random()
    if roll < MIX_READ / total:
        return do_read
    if roll < (MIX_READ + MIX_CREATE) / total:
        return do_create
    if roll < (MIX_READ + MIX_CREATE + MIX_PUBLISH) / total:
        return do_publish
    return do_list


def main():
    interval = 1.0 / max(RPS, 1)
    deadline = None if DURATION <= 0 else time.monotonic() + DURATION
    sent = 0
    last_progress = time.monotonic()
    log_event("loadgen.start", base_url=BASE_URL, rps=RPS, duration_seconds=DURATION)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while deadline is None or time.monotonic() < deadline:
            pool.submit(pick_action())
            sent += 1
            now = time.monotonic()
            if now - last_progress >= max(PROGRESS_INTERVAL_SECONDS, 5):
                log_event("loadgen.progress", sent=sent, known_articles=len(_articles))
                last_progress = now
            time.sleep(interval)
    log_event("loadgen.done", sent=sent, known_articles=len(_articles))


if __name__ == "__main__":
    main()
