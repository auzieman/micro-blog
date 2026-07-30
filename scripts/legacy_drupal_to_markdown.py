#!/usr/bin/env python3
"""Capture legacy Drupal/live pages into Markdown plus localized images.

This is intentionally a staging importer, not a publisher. It creates a
repeatable evidence layer under docs/imported/ so article cleanup can happen
from preserved Markdown instead of from memory or hand-copied snippets.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import mimetypes
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


BLOCK_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "div",
    "dl",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "ul",
}

SKIP_SELECTORS = [
    "script",
    "style",
    "noscript",
    "svg",
    "form",
    ".contextual",
    ".tabs",
    ".visually-hidden",
    ".feed-icons",
    ".breadcrumb",
    ".node__links",
    ".links",
    ".comment-wrapper",
]


def slugify(value: str, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:90].strip("-") or fallback


def collapse_blank_lines(text: str) -> str:
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


def front_matter(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def pick_body(soup: BeautifulSoup) -> Tag:
    candidates: list[tuple[int, Tag]] = []
    selectors = [
        "article .field--name-body",
        "article .node__content",
        "article",
        "main .field--name-body",
        "main article",
        "main",
        ".field--name-body",
        ".node__content",
        "#content",
    ]
    for selector in selectors:
        for el in soup.select(selector):
            text = el.get_text("\n", strip=True)
            if len(text) > 120:
                candidates.append((len(text), el))
    if not candidates:
        body = soup.body or soup
        return body
    return max(candidates, key=lambda item: item[0])[1]


def clean_body(body: Tag) -> None:
    for selector in SKIP_SELECTORS:
        for el in body.select(selector):
            el.decompose()
    for el in body.find_all(string=lambda s: isinstance(s, str) and "Log in or register to post comments" in s):
        el.extract()


def choose_extension(response: requests.Response, src: str) -> str:
    parsed = urlparse(src)
    suffix = Path(parsed.path).suffix
    if suffix and len(suffix) <= 8:
        return suffix
    ctype = response.headers.get("content-type", "").split(";", 1)[0].strip()
    guessed = mimetypes.guess_extension(ctype)
    return guessed or ".bin"


def localize_images(
    session: requests.Session,
    body: Tag,
    page_url: str,
    image_dir: Path,
    image_link_prefix: str,
    verify_tls: bool,
) -> list[dict]:
    image_dir.mkdir(parents=True, exist_ok=True)
    assets: list[dict] = []
    counter = 0
    for img in body.find_all("img"):
        src = img.get("src", "").strip()
        if not src:
            continue
        if src.startswith("data:"):
            assets.append({"source": "embedded-data-uri", "status": "skipped"})
            img.replace_with(NavigableString("[embedded image skipped: data URI was too large for staged Markdown]"))
            continue
        absolute = urljoin(page_url, src)
        counter += 1
        try:
            response = session.get(absolute, timeout=20, verify=verify_tls)
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            assets.append({"source": absolute, "status": f"download_failed: {exc}"})
            continue
        digest = hashlib.sha256(response.content).hexdigest()[:12]
        ext = choose_extension(response, absolute)
        filename = f"image-{counter:02d}-{digest}{ext}"
        target = image_dir / filename
        target.write_bytes(response.content)
        alt = img.get("alt") or f"Legacy image {counter}"
        local_href = f"{image_link_prefix.rstrip('/')}/{filename}"
        img.replace_with(NavigableString(f"![{alt}]({local_href})"))
        assets.append(
            {
                "source": absolute,
                "local": str(target),
                "markdown": local_href,
                "bytes": len(response.content),
                "status": "ok",
            }
        )
    return assets


def node_to_markdown(node: Tag) -> str:
    if isinstance(node, NavigableString):
        return html.unescape(str(node))
    if not isinstance(node, Tag):
        return ""

    name = node.name.lower()
    if name == "br":
        return "\n"
    if name == "hr":
        return "\n---\n"
    if name in {"script", "style", "noscript"}:
        return ""
    if name == "pre":
        code = node.get_text("\n", strip=False).strip("\n")
        return f"\n\n```\n{code}\n```\n\n"
    if name == "code":
        code = node.get_text("", strip=False)
        if "\n" in code:
            return f"\n\n```\n{code.strip()}\n```\n\n"
        return f"`{code.strip()}`"
    if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
        level = int(name[1])
        text = render_children(node).strip()
        return f"\n\n{'#' * level} {text}\n\n" if text else ""
    if name == "a":
        text = render_children(node).strip() or node.get("href", "").strip()
        href = node.get("href", "").strip()
        if href and text:
            return f"[{text}]({href})"
        return text
    if name in {"strong", "b"}:
        text = render_children(node).strip()
        return f"**{text}**" if text else ""
    if name in {"em", "i"}:
        text = render_children(node).strip()
        return f"*{text}*" if text else ""
    if name == "blockquote":
        text = collapse_blank_lines(render_children(node))
        return "\n\n" + "\n".join(f"> {line}" if line else ">" for line in text.splitlines()) + "\n\n"
    if name in {"ul", "ol"}:
        ordered = name == "ol"
        lines = []
        for idx, li in enumerate(node.find_all("li", recursive=False), start=1):
            item = collapse_blank_lines(render_children(li)).strip().replace("\n", "\n  ")
            prefix = f"{idx}. " if ordered else "- "
            lines.append(prefix + item)
        return "\n\n" + "\n".join(lines) + "\n\n"
    if name == "li":
        return render_children(node)
    if name == "table":
        rows = []
        for tr in node.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
        if not rows:
            return ""
        width = max(len(row) for row in rows)
        rows = [row + [""] * (width - len(row)) for row in rows]
        out = ["| " + " | ".join(rows[0]) + " |", "| " + " | ".join(["---"] * width) + " |"]
        for row in rows[1:]:
            out.append("| " + " | ".join(row) + " |")
        return "\n\n" + "\n".join(out) + "\n\n"

    text = render_children(node)
    if name in BLOCK_TAGS:
        return f"\n\n{text.strip()}\n\n" if text.strip() else ""
    return text


def render_children(node: Tag) -> str:
    return "".join(node_to_markdown(child) for child in node.children)


def capture_page(
    session: requests.Session,
    url: str,
    output_root: Path,
    image_root: Path,
    image_link_root: str,
    verify_tls: bool,
) -> Path:
    response = session.get(url, timeout=30, verify=verify_tls)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title_el = soup.find("h1") or soup.find("title")
    title = title_el.get_text(" ", strip=True) if title_el else url
    parsed = urlparse(url)
    node_match = re.search(r"/node/(\d+)", parsed.path)
    source_id = f"node-{node_match.group(1)}" if node_match else slugify(parsed.path, "page")
    slug = slugify(title, source_id)

    body = pick_body(soup)
    clean_body(body)

    image_dir = image_root / source_id
    image_link_prefix = f"{image_link_root.rstrip('/')}/{source_id}"
    assets = localize_images(session, body, url, image_dir, image_link_prefix, verify_tls)

    markdown_body = collapse_blank_lines(render_children(body))
    today = dt.date.today().isoformat()
    asset_lines = "\n".join(
        f"  - source: {front_matter(asset.get('source', ''))}\n"
        f"    status: {front_matter(asset.get('status', ''))}\n"
        f"    local: {front_matter(asset.get('local', ''))}"
        for asset in assets
    )
    if not asset_lines:
        asset_lines = "  []"

    doc = f"""---
title: {front_matter(title)}
slug: {front_matter(slug)}
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: {front_matter(url)}
source_id: {front_matter(source_id)}
captured_at: {front_matter(today)}
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
{asset_lines}
---

{markdown_body}
"""
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / f"{source_id}-{slug}.md"
    target.write_text(doc, encoding="utf-8")
    return target


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="https://auzietek.com", help="Legacy site base URL.")
    parser.add_argument("--node", action="append", default=[], help="Drupal node id to capture. May repeat.")
    parser.add_argument("--node-range", help="Inclusive range like 1-44.")
    parser.add_argument("--url", action="append", default=[], help="Absolute URL to capture. May repeat.")
    parser.add_argument("--output-root", default="docs/imported/auzietek-legacy", help="Markdown staging directory.")
    parser.add_argument("--image-root", default="docs/images/legacy-auzietek", help="Localized image directory.")
    parser.add_argument("--image-link-root", default="../images/legacy-auzietek", help="Markdown image path prefix.")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification for legacy captures.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    urls = list(args.url)
    if args.node_range:
        start, end = [int(part) for part in args.node_range.split("-", 1)]
        args.node.extend(str(node) for node in range(start, end + 1))
    for node in args.node:
        urls.append(urljoin(args.base_url.rstrip("/") + "/", f"node/{node}"))
    if not urls:
        print("No URLs selected. Use --node, --node-range, or --url.", file=sys.stderr)
        return 2

    session = requests.Session()
    session.headers.update({"User-Agent": "AuzietekLegacyMarkdownCapture/1.0"})
    output_root = Path(args.output_root)
    image_root = Path(args.image_root)
    verify_tls = not args.insecure
    for url in urls:
        try:
            target = capture_page(session, url, output_root, image_root, args.image_link_root, verify_tls)
            print(f"captured {url} -> {target}")
        except Exception as exc:  # noqa: BLE001
            print(f"failed {url}: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
