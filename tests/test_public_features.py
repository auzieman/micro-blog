import unittest
import importlib.util
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTENT_MODULE_PATH = PROJECT_ROOT / "src" / "shared" / "blog_shared" / "content.py"
SPEC = importlib.util.spec_from_file_location("microblog_content", CONTENT_MODULE_PATH)
content = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(content)


class PublicFeatureTests(unittest.TestCase):
    def test_unique_slug_adds_suffix_when_taken(self):
        slug = content.unique_slug("Mastering the Waves", {"mastering-the-waves", "mastering-the-waves-2"})
        self.assertEqual(slug, "mastering-the-waves-3")

    def test_article_public_metadata_prefers_seo_fields(self):
        article = {
            "slug": "demo-post",
            "title": "Demo Post",
            "summary": "Short summary",
            "seo_title": "SEO Demo Title",
            "seo_description": "SEO Description",
            "canonical_url": "https://blog.example/custom",
            "og_image_url": "https://blog.example/social.png",
        }
        metadata = content.article_public_metadata(article, "https://blog.example", "Micro Blog")
        self.assertEqual(metadata["title"], "SEO Demo Title")
        self.assertEqual(metadata["description"], "SEO Description")
        self.assertEqual(metadata["canonical_url"], "https://blog.example/custom")
        self.assertEqual(metadata["og_image_url"], "https://blog.example/social.png")

    def test_sitemap_generation(self):
        xml = content.build_sitemap_xml(
            [
                {"slug": "demo-post", "updated_at": "2026-04-24T12:00:00+00:00"},
                {"slug": "queue-recovery", "canonical_url": "https://blog.example/custom-queue"},
            ],
            "https://blog.example",
        )
        self.assertIn("<loc>https://blog.example/post/demo-post</loc>", xml)
        self.assertIn("<lastmod>2026-04-24T12:00:00+00:00</lastmod>", xml)
        self.assertIn("<loc>https://blog.example/custom-queue</loc>", xml)

    def test_rss_generation(self):
        xml = content.build_rss_xml(
            [
                {
                    "slug": "demo-post",
                    "title": "Demo Post",
                    "summary": "Summary",
                    "html_body": "<p>Hello</p>",
                    "published_at": "2026-04-24T12:00:00+00:00",
                }
            ],
            "https://blog.example",
            "Micro Blog",
            "A small site",
        )
        self.assertIn("<title>Micro Blog</title>", xml)
        self.assertIn("<link>https://blog.example/post/demo-post</link>", xml)
        self.assertIn("<content:encoded><![CDATA[<p>Hello</p>]]></content:encoded>", xml)

    def test_retro_content_assets_are_real_images_not_html(self):
        retro_root = PROJECT_ROOT / "content" / "assets" / "retro"
        self.assertTrue(retro_root.exists(), "retro asset root should exist")
        image_magic = {
            ".png": b"\x89PNG\r\n\x1a\n",
            ".jpg": b"\xff\xd8\xff",
            ".jpeg": b"\xff\xd8\xff",
            ".gif": b"GIF",
            ".svg": b"<",
        }
        for path in retro_root.rglob("*"):
            if path.suffix.lower() not in image_magic:
                continue
            head = path.read_bytes()[:256].lstrip()
            with self.subTest(path=str(path.relative_to(PROJECT_ROOT))):
                if path.suffix.lower() == ".svg":
                    self.assertIn(b"<svg", head[:128].lower())
                else:
                    self.assertTrue(
                        head.startswith(image_magic[path.suffix.lower()]),
                        f"{path} does not look like a {path.suffix} image",
                    )
                    self.assertNotIn(b"<html", head.lower())

    def test_public_lane_referenced_content_files_exist(self):
        post_root = PROJECT_ROOT / "content" / "posts" / "public-lanes"
        refs = []
        for post in post_root.glob("*.md"):
            text = post.read_text()
            refs.extend((post, ref) for ref in re.findall(r"/content-files/(assets/[^)'\"\s>]+)", text))
        self.assertGreater(len(refs), 0)
        for post, ref in refs:
            target = PROJECT_ROOT / "content" / ref
            with self.subTest(post=post.name, ref=ref):
                self.assertTrue(target.exists(), f"{post.name} references missing {ref}")


if __name__ == "__main__":
    unittest.main()
