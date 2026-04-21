import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_SRC = PROJECT_ROOT / "src" / "api"
sys.path.insert(0, str(API_SRC))

import import_utils  # noqa: E402


class ImportHelperTests(unittest.TestCase):
    def test_stable_import_article_id_is_deterministic(self):
        first = import_utils.stable_import_article_id("public-crawl", "40", "https://example.com/node/40")
        second = import_utils.stable_import_article_id("public-crawl", "40", "https://example.com/node/40")
        third = import_utils.stable_import_article_id("public-crawl", "41", "https://example.com/node/41")
        self.assertEqual(first, second)
        self.assertNotEqual(first, third)
        self.assertTrue(first.startswith("ART-"))

    def test_parse_front_matter(self):
        document = """---
title: Demo Post
tags: [linux, bash]
hero_image: images/demo.svg
---
body here
"""
        metadata, body = import_utils.parse_front_matter(document)
        self.assertEqual(metadata["title"], "Demo Post")
        self.assertEqual(metadata["tags"], ["linux", "bash"])
        self.assertEqual(metadata["hero_image"], "images/demo.svg")
        self.assertEqual(body.strip(), "body here")

    def test_rewrite_markdown_asset_paths(self):
        body = "![Demo](images/demo.svg)\n[Image link](images/demo.svg)\n[Doc](docs/readme.md)"
        rewritten = import_utils.rewrite_markdown_asset_paths(body, "posts/linux", "/content-files")
        self.assertIn("/content-files/posts/linux/images/demo.svg", rewritten)
        self.assertIn("[Doc](docs/readme.md)", rewritten)

    def test_rewrite_html_asset_urls(self):
        html = '<p><img src="https://example.com/demo.png"><a href="https://example.com/demo.png">full</a></p>'
        rewritten = import_utils.rewrite_html_asset_urls(
            html,
            {"https://example.com/demo.png": "/content-files/imports/assets/public-crawl/40/demo.png"},
        )
        self.assertIn('/content-files/imports/assets/public-crawl/40/demo.png', rewritten)
        self.assertNotIn('https://example.com/demo.png', rewritten)

    def test_parse_front_matter_multiline_list(self):
        document = """---
title: Queue Recovery Playbook
tags:
  - linux
  - rabbitmq
  - recovery
status: draft
---
body here
"""
        metadata, body = import_utils.parse_front_matter(document)
        self.assertEqual(metadata["tags"], ["linux", "rabbitmq", "recovery"])
        self.assertEqual(metadata["status"], "draft")
        self.assertEqual(body.strip(), "body here")

    def test_parse_public_article_page_strips_chrome(self):
        html = """
        <html>
          <body>
            <article class="node">
              <header><a href="/user/login">Log in</a></header>
              <div class="field--name-body">
                <div class="field__item">
                  <p>Real article text.</p>
                  <a href="/blogs">My Blog</a>
                  <img src="/sites/default/files/demo.png" />
                </div>
              </div>
              <footer>Footer noise</footer>
            </article>
          </body>
        </html>
        """
        parsed = import_utils.parse_public_article_page(html, "https://example.com/node/40")
        self.assertIn("Real article text.", parsed["body_html"])
        self.assertNotIn("Log in", parsed["body_html"])
        self.assertNotIn("My Blog", parsed["body_html"])
        self.assertEqual(parsed["hero_image_url"], "https://example.com/sites/default/files/demo.png")

    def test_filesystem_preview_items(self):
        payload = {
            "root_path": str(PROJECT_ROOT / "content"),
            "content_subdir": "posts/linux",
            "theme_variant": "midnight",
            "limit": 10,
        }
        preview = import_utils.filesystem_preview_items(payload, lambda value: value.lower().replace(" ", "-"), PROJECT_ROOT / "content", "/content-files")
        self.assertGreaterEqual(len(preview), 5)
        self.assertTrue(any(item["slug"] == "midnight-import-demo" for item in preview))

    def test_filesystem_preview_items_keyword_filter_and_defaults(self):
        payload = {
            "root_path": str(PROJECT_ROOT / "content"),
            "content_subdir": "posts/linux",
            "theme_variant": "midnight",
            "keyword_filter": "rabbitmq",
            "limit": 10,
        }
        preview = import_utils.filesystem_preview_items(payload, lambda value: value.lower().replace(" ", "-"), PROJECT_ROOT / "content", "/content-files")
        self.assertEqual(len(preview), 1)
        self.assertEqual(preview[0]["slug"], "queue-recovery-playbook")
        self.assertEqual(preview[0]["theme_variant"], "midnight")
        self.assertEqual(preview[0]["status"], "draft")
        self.assertIn("/content-files/posts/linux/images/terminal-rig.svg", preview[0]["markdown_body"])


if __name__ == "__main__":
    unittest.main()
