from __future__ import annotations

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
        aliases = list(dict.fromkeys(post.get("slug_aliases", [])))

        existing = self.get_by_article_id(article_id)
        if existing:
            self._clear_slug_material(existing.get("slug"), existing.get("slug_aliases", []))

        if ttl_seconds:
            self.client.set(self._id_key(article_id), encoded, ex=ttl_seconds)
            self.client.set(self._slug_key(slug), encoded, ex=ttl_seconds)
        else:
            self.client.set(self._id_key(article_id), encoded)
            self.client.set(self._slug_key(slug), encoded)
        self.client.zadd(self.list_key, {slug: score})

        for alias in aliases:
            if alias and alias != slug:
                if ttl_seconds:
                    self.client.set(self._alias_key(alias), slug, ex=ttl_seconds)
                else:
                    self.client.set(self._alias_key(alias), slug)

    def remove(self, article_id: str) -> None:
        existing = self.get_by_article_id(article_id)
        if not existing:
            return
        self._clear_slug_material(existing.get("slug"), existing.get("slug_aliases", []))
        self.client.delete(self._id_key(article_id))

    def resolve_slug(self, slug: str) -> tuple[dict[str, Any] | None, str | None]:
        value = self.client.get(self._slug_key(slug))
        if value:
            return json.loads(value), None
        alias_target = self.client.get(self._alias_key(slug))
        if not alias_target:
            return None, None
        target_value = self.client.get(self._slug_key(alias_target))
        return (json.loads(target_value), alias_target) if target_value else (None, None)

    def get_by_slug(self, slug: str) -> dict[str, Any] | None:
        post, _redirect = self.resolve_slug(slug)
        return post

    def get_by_article_id(self, article_id: str) -> dict[str, Any] | None:
        value = self.client.get(self._id_key(article_id))
        return json.loads(value) if value else None

    def list(self, page: int = 1, page_size: int = 10, tag: str | None = None) -> dict[str, Any]:
        safe_page = max(page, 1)
        safe_size = max(1, min(page_size, 1000))
        items = self.list_all(tag)
        total = len(items)
        start = (safe_page - 1) * safe_size
        stop = start + safe_size
        paged_items = items[start:stop]
        return {"items": paged_items, "total": total, "page": safe_page, "page_size": safe_size, "tag": tag}

    def list_all(self, tag: str | None = None) -> list[dict[str, Any]]:
        slugs = self.client.zrevrange(self.list_key, 0, -1)
        items = []
        stale_slugs = []
        for slug in slugs:
            value = self.client.get(self._slug_key(slug))
            if not value:
                stale_slugs.append(slug)
                continue
            item = json.loads(value)
            if tag and tag not in item.get("tags", []):
                continue
            items.append(item)
        if stale_slugs:
            self.client.zrem(self.list_key, *stale_slugs)
        return items

    def _clear_slug_material(self, slug: str | None, aliases: list[str] | None) -> None:
        if slug:
            self.client.delete(self._slug_key(slug))
            self.client.zrem(self.list_key, slug)
        for alias in aliases or []:
            self.client.delete(self._alias_key(alias))

    def _id_key(self, article_id: str) -> str:
        return f"blog:article:{article_id}"

    def _slug_key(self, slug: str) -> str:
        return f"blog:slug:{slug}"

    def _alias_key(self, slug: str) -> str:
        return f"blog:alias:{slug}"
