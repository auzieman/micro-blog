#!/usr/bin/env python3
"""Smoke-check rendered public page images.

This is intentionally tiny: fetch pages, extract rendered `<img src="">`
references, and verify each image returns HTTP 200 with an image content type.
Use `--insecure` for lab or transitional certificate checks.
"""

from __future__ import annotations

import argparse
import re
import sys
from urllib.parse import urljoin

import requests


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
            image_response = session.get(image_url, timeout=15, verify=not args.insecure, stream=True)
            content_type = image_response.headers.get("content-type", "")
            status_ok = image_response.status_code == 200 and content_type.startswith("image/")
            print(f"  {'OK' if status_ok else 'BAD'} {image_response.status_code} {content_type} {image_url}")
            ok = ok and status_ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
