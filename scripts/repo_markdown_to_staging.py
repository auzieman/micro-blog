#!/usr/bin/env python3
"""Stage project/repository Markdown as public-site source material.

Unlike the Drupal importer, this keeps Markdown mostly intact. It adds metadata
and optionally localizes relative image references so project docs can become
curated public stories without losing their original repo context.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import re
import shutil
import sys
from pathlib import Path


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}


def slugify(value: str, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:100].strip("-") or fallback


def front_matter(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def first_heading(markdown: str, fallback: str) -> str:
    for line in markdown.splitlines():
        match = re.match(r"^#\s+(.+?)\s*$", line)
        if match:
            return match.group(1).strip()
    return fallback


def infer_lane(path: Path) -> str:
    lowered = str(path).lower()
    if any(token in lowered for token in ["amiga", "muirc", "amiwriter", "auzix", "retro"]):
        return "retro-users"
    if any(token in lowered for token in ["blackknight", "bkc", "pipeline", "ipmi", "openstack", "esxi"]):
        return "blackknightcontroller"
    if any(token in lowered for token in ["rx-demo", "docker", "kubernetes", "k3s", "linux", "ansible", "puppet"]):
        return "linux-users"
    return "auzietek"


def infer_tags(path: Path, text: str) -> list[str]:
    haystack = f"{path} {text[:4000]}".lower()
    tags = ["repo-md", "needs-review"]
    candidates = {
        "blackknight": ["blackknight", "bkc"],
        "pipelines": ["pipeline", "pipelines"],
        "openstack": ["openstack"],
        "esxi": ["esxi", "vmware"],
        "docker": ["docker", "swarm", "compose"],
        "kubernetes": ["kubernetes", "k3s", "kubectl"],
        "linux": ["linux", "debian", "fedora", "trixie"],
        "retro": ["amiga", "amigaos", "retro"],
        "auzix": ["auzix"],
        "observability": ["grafana", "prometheus", "otel", "opentelemetry", "loki", "tempo"],
        "aiops": ["codex", "ai", "llm", "prompt"],
    }
    for tag, needles in candidates.items():
        if any(needle in haystack for needle in needles):
            tags.append(tag)
    return sorted(set(tags))


def localize_relative_images(markdown: str, source: Path, image_dir: Path, image_link_prefix: str) -> tuple[str, list[dict]]:
    image_dir.mkdir(parents=True, exist_ok=True)
    assets: list[dict] = []

    def replace(match: re.Match) -> str:
        alt = match.group(1)
        href = match.group(2).strip()
        if re.match(r"^[a-z]+://", href) or href.startswith("/") or href.startswith("#"):
            assets.append({"source": href, "status": "external_or_absolute"})
            return match.group(0)
        raw_href = href.split("#", 1)[0].split("?", 1)[0]
        if Path(raw_href).suffix.lower() not in IMAGE_EXTENSIONS:
            return match.group(0)
        candidate = (source.parent / raw_href).resolve()
        if not candidate.exists() or not candidate.is_file():
            assets.append({"source": href, "status": "missing"})
            return match.group(0)
        digest = hashlib.sha256(candidate.read_bytes()).hexdigest()[:12]
        target_name = f"{candidate.stem}-{digest}{candidate.suffix.lower()}"
        target = image_dir / target_name
        if not target.exists():
            shutil.copy2(candidate, target)
        local_href = f"{image_link_prefix.rstrip('/')}/{target_name}"
        assets.append({"source": str(candidate), "local": str(target), "markdown": local_href, "status": "ok"})
        return f"![{alt}]({local_href})"

    converted = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace, markdown)
    return converted, assets


def stage_file(source: Path, workspace_root: Path, output_root: Path, image_root: Path, image_link_root: str) -> Path:
    text = source.read_text(encoding="utf-8", errors="replace")
    rel = source.resolve().relative_to(workspace_root.resolve()) if source.resolve().is_relative_to(workspace_root.resolve()) else source.resolve()
    repo = rel.parts[0] if isinstance(rel, Path) and len(rel.parts) > 1 else source.parent.name
    title = first_heading(text, source.stem.replace("-", " ").replace("_", " ").title())
    lane = infer_lane(source)
    tags = infer_tags(source, text)
    slug = slugify(f"{repo}-{title}", source.stem)
    source_id = slugify(str(rel), source.stem)
    image_dir = image_root / source_id
    image_prefix = f"{image_link_root.rstrip('/')}/{source_id}"
    converted, assets = localize_relative_images(text, source, image_dir, image_prefix)
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
summary: "Repository Markdown staged for public article/story cleanup."
status: draft
source_type: repo_markdown
source_repo: {front_matter(repo)}
source_path: {front_matter(str(rel))}
source_id: {front_matter(source_id)}
captured_at: {front_matter(today)}
candidate_lane: {front_matter(lane)}
tags: [{", ".join(tags)}]
assets:
{asset_lines}
---

{converted.strip()}
"""
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / f"{source_id}.md"
    target.write_text(doc, encoding="utf-8")
    return target


def discover(root: Path) -> list[Path]:
    ignored = {".git", ".pytest_cache", "node_modules", "vendor", "__pycache__"}
    found = []
    for path in root.rglob("*.md"):
        if any(part in ignored for part in path.parts):
            continue
        found.append(path)
    return sorted(found)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", default="/home/auzieman/Projects")
    parser.add_argument("--source", action="append", default=[], help="Markdown file or directory. May repeat.")
    parser.add_argument("--output-root", default="docs/imported/repos")
    parser.add_argument("--image-root", default="docs/images/imported-repos")
    parser.add_argument("--image-link-root", default="../images/imported-repos")
    parser.add_argument("--limit", type=int, help="Optional cap for discovery runs.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workspace_root = Path(args.workspace_root)
    sources: list[Path] = []
    for item in args.source:
        path = Path(item)
        if path.is_dir():
            sources.extend(discover(path))
        elif path.is_file():
            sources.append(path)
        else:
            print(f"missing source: {path}", file=sys.stderr)
    if not sources:
        print("No sources selected. Use --source FILE_OR_DIR.", file=sys.stderr)
        return 2
    if args.limit:
        sources = sources[: args.limit]
    for source in sources:
        target = stage_file(
            source.resolve(),
            workspace_root.resolve(),
            Path(args.output_root),
            Path(args.image_root),
            args.image_link_root,
        )
        print(f"staged {source} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
