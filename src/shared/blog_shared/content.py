import html
import json
import re
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any
from urllib.parse import urljoin
from xml.sax.saxutils import escape


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\s-]", "", (value or "")).strip().lower()
    value = re.sub(r"[-\s]+", "-", value).strip("-")
    return value or "post"


def unique_slug(candidate: str, taken: set[str]) -> str:
    base = slugify(candidate)
    if base not in taken:
        return base
    index = 2
    while True:
        slug = f"{base}-{index}"
        if slug not in taken:
            return slug
        index += 1


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return " ".join(text.split())


def truncate_text(value: str | None, limit: int) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    shortened = text[: limit - 1].rsplit(" ", 1)[0].strip()
    return (shortened or text[: limit - 1]).rstrip(" ,.;:-") + "…"


def canonical_post_url(site_url: str, slug: str, override: str | None = None) -> str:
    if override:
        return override.strip()
    return urljoin(site_url.rstrip("/") + "/", f"post/{slug}")


def article_description(article: dict[str, Any]) -> str:
    explicit = (article.get("seo_description") or "").strip()
    if explicit:
        return explicit
    summary = (article.get("summary") or "").strip()
    if summary:
        return truncate_text(summary, 160)
    body_source = article.get("html_body") or article.get("markdown_body") or ""
    return truncate_text(strip_html(body_source), 160)


def article_title(article: dict[str, Any], site_name: str) -> str:
    explicit = (article.get("seo_title") or "").strip()
    if explicit:
        return explicit
    title = (article.get("title") or "Untitled").strip()
    return f"{title} | {site_name}"


def article_og_image(article: dict[str, Any], default_image: str | None = None) -> str | None:
    return article.get("og_image_url") or article.get("hero_image_url") or default_image


def article_public_metadata(article: dict[str, Any], site_url: str, site_name: str, default_image: str | None = None) -> dict[str, Any]:
    canonical_url = canonical_post_url(site_url, article["slug"], article.get("canonical_url"))
    description = article_description(article)
    title = article_title(article, site_name)
    og_image = article_og_image(article, default_image)
    return {
        "title": title,
        "description": description,
        "canonical_url": canonical_url,
        "og_image_url": og_image,
        "twitter_card": "summary_large_image" if og_image else "summary",
    }


def iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def article_json_ld(article: dict[str, Any], site_url: str, site_name: str, default_image: str | None = None) -> str:
    metadata = article_public_metadata(article, site_url, site_name, default_image)
    published = iso_to_datetime(article.get("published_at")) or iso_to_datetime(article.get("updated_at")) or datetime.now(timezone.utc)
    updated = iso_to_datetime(article.get("updated_at")) or published
    payload = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": (article.get("seo_title") or article.get("title") or "Untitled").strip(),
        "description": metadata["description"],
        "datePublished": published.isoformat(),
        "dateModified": updated.isoformat(),
        "mainEntityOfPage": metadata["canonical_url"],
        "author": {
            "@type": "Person",
            "email": article.get("author_email"),
        },
        "publisher": {
            "@type": "Organization",
            "name": site_name,
        },
    }
    if metadata["og_image_url"]:
        payload["image"] = [metadata["og_image_url"]]
    return json.dumps(payload, separators=(",", ":"))


def build_sitemap_xml(posts: list[dict[str, Any]], site_url: str) -> str:
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for post in posts:
        canonical = canonical_post_url(site_url, post["slug"], post.get("canonical_url"))
        updated = post.get("updated_at") or post.get("published_at")
        lines.append("  <url>")
        lines.append(f"    <loc>{escape(canonical)}</loc>")
        if updated:
            lines.append(f"    <lastmod>{escape(updated)}</lastmod>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines)


def build_rss_xml(posts: list[dict[str, Any]], site_url: str, site_name: str, site_description: str) -> str:
    build_date = format_datetime(datetime.now(timezone.utc))
    items = []
    for post in posts:
        metadata = article_public_metadata(post, site_url, site_name)
        pub_date = iso_to_datetime(post.get("published_at")) or iso_to_datetime(post.get("updated_at")) or datetime.now(timezone.utc)
        body = post.get("html_body") or ""
        items.append(
            "\n".join(
                [
                    "    <item>",
                    f"      <title>{escape(post.get('title') or 'Untitled')}</title>",
                    f"      <link>{escape(metadata['canonical_url'])}</link>",
                    f"      <guid>{escape(metadata['canonical_url'])}</guid>",
                    f"      <description>{escape(metadata['description'])}</description>",
                    f"      <pubDate>{escape(format_datetime(pub_date))}</pubDate>",
                    f"      <content:encoded><![CDATA[{body}]]></content:encoded>",
                    "    </item>",
                ]
            )
        )
    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
            "  <channel>",
            f"    <title>{escape(site_name)}</title>",
            f"    <link>{escape(site_url.rstrip('/') + '/')}</link>",
            f"    <description>{escape(site_description)}</description>",
            f"    <lastBuildDate>{escape(build_date)}</lastBuildDate>",
            *items,
            "  </channel>",
            "</rss>",
        ]
    )


def render_post_preview(payload: dict[str, Any], site_url: str, site_name: str, default_image: str | None = None) -> dict[str, Any]:
    article = dict(payload)
    article.setdefault("slug", slugify(article.get("slug") or article.get("title") or "preview"))
    article.setdefault("title", "Untitled")
    article.setdefault("summary", "")
    article.setdefault("tags", [])
    article.setdefault("body_format", "markdown")
    article.setdefault("theme_variant", "midnight")
    metadata = article_public_metadata(article, site_url, site_name, default_image)
    return {
        "article": article,
        "metadata": metadata,
        "json_ld": article_json_ld(article, site_url, site_name, default_image),
    }
