#!/usr/bin/env python3
"""Smoke-check rendered public page images.

This is intentionally tiny: fetch pages, extract rendered `<img src="">`
references, and verify each image returns HTTP 200 with an image content type
and plausible image bytes.
Use `--insecure` for lab or transitional certificate checks.
"""

from __future__ import annotations

import argparse
import re
import sys
from urllib.parse import urljoin

import requests


def looks_like_image(content_type: str, body: bytes) -> bool:
    content_type = content_type.split(";", 1)[0].strip().lower()
    if content_type == "image/svg+xml":
        return body.lstrip().startswith(b"<svg") or b"<svg" in body[:512]
    if content_type == "image/png":
        return body.startswith(b"\x89PNG\r\n\x1a\n")
    if content_type in {"image/jpeg", "image/jpg"}:
        return body.startswith(b"\xff\xd8\xff")
    if content_type == "image/gif":
        return body.startswith((b"GIF87a", b"GIF89a"))
    if content_type == "image/webp":
        return body.startswith(b"RIFF") and body[8:12] == b"WEBP"
    return content_type.startswith("image/") and not body.lstrip().lower().startswith(b"<!doctype html")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("urls", nargs="+")
    args = parser.parse_args()

    ok = True
    session = requests.Session()
    for page_url in args.urls:
        response = session.get(page_url, timeout=15, verify=not args.insecure)
        print(f"PAGE {response.status_code} {page_url}")
        if response.status_code >= 400:
            ok = False
            continue
        image_sources = sorted(set(re.findall(r'<img[^>]+src="([^"]+)"', response.text)))
        for source in image_sources:
            image_url = urljoin(page_url, source)
            image_response = session.get(image_url, timeout=15, verify=not args.insecure)
            content_type = image_response.headers.get("content-type", "")
            status_ok = (
                image_response.status_code == 200
                and content_type.startswith("image/")
                and looks_like_image(content_type, image_response.content[:1024])
            )
            print(f"  {'OK' if status_ok else 'BAD'} {image_response.status_code} {content_type} {image_url}")
            ok = ok and status_ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
