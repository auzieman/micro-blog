import unittest
import importlib.util
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


if __name__ == "__main__":
    unittest.main()
