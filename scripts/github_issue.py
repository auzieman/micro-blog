#!/usr/bin/env python3
"""Create or dry-run GitHub issues using the local auzieman token.

Default behavior is dry-run. Add --create when the issue is reviewed and should
be posted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


DEFAULT_TOKEN_FILE = Path.home() / ".secrets" / "github-auzieman-token"


def read_body(args: argparse.Namespace) -> str:
    if args.body_file:
        return Path(args.body_file).read_text(encoding="utf-8")
    if args.body:
        return args.body
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def token(args: argparse.Namespace) -> str:
    value = os.getenv("GITHUB_TOKEN", "").strip()
    if value:
        return value
    path = Path(args.token_file)
    if not path.exists():
        raise SystemExit(f"missing token file: {path}")
    return path.read_text(encoding="utf-8").strip()


def github_request(method: str, path: str, token_value: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(
        f"https://api.github.com{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token_value}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "AuzietekIssueHelper/1.0",
        },
    )
    try:
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"GitHub API error {exc.code}: {detail}") from exc


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/repo, e.g. auzieman/micro-blog")
    parser.add_argument("--title", required=True)
    parser.add_argument("--body", help="Issue body text.")
    parser.add_argument("--body-file", help="Markdown file to use as body.")
    parser.add_argument("--label", action="append", default=[], help="Issue label. May repeat.")
    parser.add_argument("--create", action="store_true", help="Actually create the issue.")
    parser.add_argument("--token-file", default=str(DEFAULT_TOKEN_FILE))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    body = read_body(args).strip()
    payload = {"title": args.title, "body": body, "labels": args.label}
    if not args.create:
        print(json.dumps({"dry_run": True, "repo": args.repo, "payload": payload}, indent=2))
        return 0
    created = github_request("POST", f"/repos/{args.repo}/issues", token(args), payload)
    print(json.dumps({"created": True, "url": created.get("html_url"), "number": created.get("number")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
