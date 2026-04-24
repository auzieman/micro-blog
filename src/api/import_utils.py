import re
import hashlib
import json
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def stable_import_article_id(source_kind: str, source_id: str | None, fallback: str = "") -> str:
    raw = (source_id or fallback or "").strip()
    if not raw:
        raise ValueError("Stable import article IDs require a source identifier or fallback.")
    digest = hashlib.sha1(f"{source_kind}:{raw}".encode("utf-8")).hexdigest()[:12].upper()
    return f"ART-{digest}"


def article_fingerprint(payload: dict) -> str:
    normalized = {
        "title": payload.get("title") or "",
        "slug": payload.get("slug") or "",
        "summary": payload.get("summary") or "",
        "body_format": payload.get("body_format") or "markdown",
        "markdown_body": payload.get("markdown_body") or "",
        "hero_image_url": payload.get("hero_image_url") or "",
        "theme_variant": payload.get("theme_variant") or "",
        "tags": sorted(payload.get("tags") or []),
        "status": payload.get("status") or "",
        "seo_title": payload.get("seo_title") or "",
        "seo_description": payload.get("seo_description") or "",
        "canonical_url": payload.get("canonical_url") or "",
        "og_image_url": payload.get("og_image_url") or "",
        "source_url": payload.get("source_url") or "",
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def plan_bootstrap_sync_actions(commands: list[dict], existing_by_id: dict[str, dict], sync_mode: str) -> dict:
    if sync_mode not in {"skip", "update", "reset"}:
        raise ValueError("sync_mode must be one of: skip, update, reset")
    planned = []
    skipped = 0
    reset_deleted = 0
    for command in commands:
        current = existing_by_id.get(command["article_id"])
        desired_fingerprint = article_fingerprint(command)
        current_fingerprint = article_fingerprint(current or {})
        if sync_mode == "skip" and current:
            skipped += 1
            continue
        if sync_mode == "update" and current and desired_fingerprint == current_fingerprint:
            skipped += 1
            continue
        if sync_mode == "reset" and current:
            planned.append({"action": "delete", "article_id": command["article_id"]})
            reset_deleted += 1
        planned.append({"action": "upsert", "command": command})
    return {
        "planned": planned,
        "skipped": skipped,
        "reset_deleted": reset_deleted,
        "count": sum(1 for item in planned if item["action"] == "upsert"),
    }


def collect_asset_urls_from_html(html: str) -> list[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for selector, attribute in (("img[src]", "src"), ("source[src]", "src"), ("a[href]", "href")):
        for node in soup.select(selector):
            value = (node.get(attribute) or "").strip()
            if not value or value.startswith(("data:", "#")):
                continue
            lowered = value.lower()
            if attribute == "href" and not lowered.endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".avif", ".bmp", ".ico")
            ):
                continue
            if value not in seen:
                seen.add(value)
                urls.append(value)
    return urls


def rewrite_html_asset_urls(html: str, replacements: dict[str, str]) -> str:
    if not html or not replacements:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for selector, attribute in (("img[src]", "src"), ("source[src]", "src"), ("a[href]", "href")):
        for node in soup.select(selector):
            value = (node.get(attribute) or "").strip()
            if value in replacements:
                node[attribute] = replacements[value]
    return str(soup)


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


def coerce_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def filesystem_public_url(relative_path: str, content_public_base: str) -> str:
    normalized = relative_path.strip().replace("\\", "/").lstrip("/")
    return f"{content_public_base.rstrip('/')}/{normalized}"


def rewrite_markdown_asset_paths(body: str, asset_root: str, content_public_base: str) -> str:
    if not asset_root:
        return body

    def replace_image(match):
        alt_text = match.group(1)
        target = match.group(2).strip()
        if target.startswith(("http://", "https://", "data:", "/", "#")):
            return match.group(0)
        return f"![{alt_text}]({filesystem_public_url(f'{asset_root}/{target}', content_public_base)})"

    def replace_link(match):
        label = match.group(1)
        target = match.group(2).strip()
        if target.startswith(("http://", "https://", "mailto:", "/", "#")):
            return match.group(0)
        lowered = target.lower()
        if not lowered.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".avif")):
            return match.group(0)
        return f"[{label}]({filesystem_public_url(f'{asset_root}/{target}', content_public_base)})"

    body = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace_image, body)
    body = re.sub(r"(?<!!)\[([^\]]+)\]\(([^)]+)\)", replace_link, body)
    return body


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
        ".field--name-body .field__item",
        ".field--name-body",
        ".node__content .field--name-body .field__item",
        "article.node",
        "article[role='article']",
        ".node__content",
        ".text-formatted",
        "main article",
        "main",
    ):
        content_node = soup.select_one(selector)
        if content_node:
            break

    if not content_node:
        raise ValueError(f"Unable to locate article body for {source_url}")

    content_node = BeautifulSoup(str(content_node), "html.parser")
    root = content_node
    for selector in (
        "nav",
        "header",
        "footer",
        "aside",
        ".links",
        ".menu",
        ".tabs",
        ".breadcrumb",
        ".region",
        ".comment-forbidden",
        ".comment-form",
        ".pager",
        "form",
        "script",
        "style",
    ):
        for node in root.select(selector):
            node.decompose()

    for anchor in root.select("a[href]"):
        text = " ".join(anchor.get_text(" ", strip=True).split()).lower()
        href = anchor.get("href", "").strip().lower()
        if text in {"log in", "login", "register", "sign up", "my blog", "read more"}:
            anchor.decompose()
            continue
        if href.startswith(("/user", "/blog", "/blogs", "/comment", "/node/add")):
            anchor.decompose()

    for tag_name, attribute in (("img", "src"), ("a", "href"), ("source", "src")):
        for node in root.select(f"{tag_name}[{attribute}]"):
            value = node.get(attribute, "").strip()
            if value:
                node[attribute] = urljoin(source_url, value)

    image_urls = [urljoin(source_url, image.get("src", "").strip()) for image in root.select("img[src]")]
    summary = ""
    meta_description = soup.select_one("meta[name='description']")
    if meta_description and meta_description.get("content"):
        summary = meta_description.get("content", "").strip()
    if not summary:
        first_paragraph = root.select_one("p")
        if first_paragraph:
            summary = " ".join(first_paragraph.get_text(" ", strip=True).split())
    if not image_urls:
        og_image = soup.select_one("meta[property='og:image']")
        if og_image and og_image.get("content"):
            image_urls.append(urljoin(source_url, og_image.get("content").strip()))

    tags = []
    for anchor in soup.select("a[href*='/taxonomy/term/']"):
        tag_text = " ".join(anchor.get_text(" ", strip=True).split())
        if tag_text and tag_text not in tags:
            tags.append(tag_text)

    return {
        "title": title or source_url.rstrip("/").rsplit("/", 1)[-1],
        "summary": summary,
        "body_html": str(root),
        "hero_image_url": image_urls[0] if image_urls else None,
        "image_urls": image_urls,
        "tags": tags,
    }


def filesystem_preview_items(payload: dict, slugify_fn, content_import_root: Path, content_public_base: str) -> list[dict]:
    root_path = Path(payload.get("root_path") or content_import_root).resolve()
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
            hero_image = filesystem_public_url(f"{asset_root}/{hero_image}" if asset_root else hero_image, content_public_base)
        rewritten_body = rewrite_markdown_asset_paths(body, asset_root, content_public_base)
        image_paths = re.findall(r"!\[[^\]]*\]\(([^)]+)\)", rewritten_body)
        preview_item = {
            "source_id": relative_path,
            "source_nid": None,
            "title": title,
            "slug": str(metadata.get("slug") or slugify_fn(title)),
            "summary": str(metadata.get("summary") or ""),
            "tags": tags,
            "body_format": str(metadata.get("body_format") or "markdown"),
            "hero_image_url": hero_image or (image_paths[0] if image_paths else None),
            "image_urls": image_paths,
            "theme_variant": str(metadata.get("theme_variant") or metadata.get("theme") or default_theme_variant),
            "source_url": filesystem_public_url(relative_path, content_public_base),
            "markdown_body": rewritten_body,
            "status": str(metadata.get("status") or status),
            "source_path": relative_path,
            "seo_title": str(metadata.get("seo_title") or ""),
            "seo_description": str(metadata.get("seo_description") or ""),
            "canonical_url": str(metadata.get("canonical_url") or ""),
            "og_image_url": str(metadata.get("og_image_url") or ""),
        }
        preview_item["fingerprint"] = article_fingerprint(preview_item)
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
