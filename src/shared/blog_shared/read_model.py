import json
from datetime import datetime, timezone
from typing import Any

import redis


class BlogReadModelStore:
    def __init__(self, redis_url: str):
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.list_key = "blog:posts:index"

    def upsert(self, post: dict[str, Any], ttl_seconds: int | None = None) -> None:
        article_id = post["article_id"]
        slug = post["slug"]
        updated_at = post.get("updated_at") or datetime.now(timezone.utc).isoformat()
        score = int(datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp() * 1000)
        encoded = json.dumps(post)
        if ttl_seconds:
            self.client.set(self._id_key(article_id), encoded, ex=ttl_seconds)
            self.client.set(self._slug_key(slug), encoded, ex=ttl_seconds)
        else:
            self.client.set(self._id_key(article_id), encoded)
            self.client.set(self._slug_key(slug), encoded)
        self.client.zadd(self.list_key, {slug: score})

    def get_by_slug(self, slug: str) -> dict[str, Any] | None:
        value = self.client.get(self._slug_key(slug))
        return json.loads(value) if value else None

    def list(self, page: int = 1, page_size: int = 10, tag: str | None = None) -> dict[str, Any]:
        safe_page = max(page, 1)
        safe_size = max(1, min(page_size, 100))
        slugs = self.client.zrevrange(self.list_key, 0, -1)
        items = []
        valid_slugs = []
        stale_slugs = []
        for slug in slugs:
            item = self.get_by_slug(slug)
            if item is None:
                stale_slugs.append(slug)
                continue
            valid_slugs.append(slug)
            items.append(item)
        if stale_slugs:
            self.client.zrem(self.list_key, *stale_slugs)
        if tag:
            items = [item for item in items if tag in item.get("tags", [])]
        total = len(items)
        start = (safe_page - 1) * safe_size
        stop = start + safe_size
        paged_items = items[start:stop]
        return {"items": paged_items, "total": total, "page": safe_page, "page_size": safe_size, "tag": tag}

    def _id_key(self, article_id: str) -> str:
        return f"blog:article:{article_id}"

    def _slug_key(self, slug: str) -> str:
        return f"blog:slug:{slug}"
