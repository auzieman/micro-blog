import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
UI_SRC = PROJECT_ROOT / "src" / "ui"
SHARED_SRC = PROJECT_ROOT / "src" / "shared"
sys.path.insert(0, str(UI_SRC))
sys.path.insert(0, str(SHARED_SRC))

SPEC = importlib.util.spec_from_file_location("microblog_ui_app", UI_SRC / "app.py")
try:
    ui_app = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(ui_app)
    UI_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    ui_app = None
    UI_IMPORT_ERROR = exc


class FakeResponse:
    def __init__(self, status_code=200, text="OK", json_payload=None):
        self.status_code = status_code
        self.text = text
        self._json_payload = json_payload or {}

    def json(self):
        return self._json_payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.text)


@unittest.skipIf(ui_app is None, f"UI test dependencies unavailable: {UI_IMPORT_ERROR}")
class UIRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = ui_app.app.test_client()

    def authenticate(self):
        with self.client.session_transaction() as session:
            session["admin_email"] = ui_app.ADMIN_EMAIL

    def test_edit_flow_uses_api_put(self):
        self.authenticate()
        with mock.patch.object(ui_app, "api_put", return_value=FakeResponse(202, "queued")) as mocked_put:
            response = self.client.post(
                "/admin/posts/ART-1234/update",
                data={
                    "title": "Edited Title",
                    "slug": "edited-title",
                    "summary": "Edited summary",
                    "markdown_body": "# Edited",
                    "body_format": "markdown",
                    "hero_image_url": "",
                    "theme_variant": "midnight",
                    "tags": "linux, seo",
                    "status": "draft",
                    "seo_title": "SEO Edited",
                    "seo_description": "SEO body",
                    "canonical_url": "https://example.com/custom",
                    "og_image_url": "https://example.com/og.png",
                },
            )
        self.assertEqual(response.status_code, 302)
        mocked_put.assert_called_once()
        called_payload = mocked_put.call_args[0][1]
        self.assertEqual(called_payload["slug"], "edited-title")
        self.assertEqual(called_payload["seo_title"], "SEO Edited")
        self.assertEqual(called_payload["tags"], ["linux", "seo"])

    def test_soft_delete_and_restore_routes_call_api(self):
        self.authenticate()
        with mock.patch.object(ui_app, "api_post", return_value=FakeResponse(202, "queued")) as mocked_post:
            delete_response = self.client.post("/admin/posts/ART-1234/delete", data={"return_to": "/admin"})
            restore_response = self.client.post("/admin/posts/ART-1234/restore", data={"return_to": "/admin", "restore_status": "published"})
        self.assertEqual(delete_response.status_code, 302)
        self.assertEqual(restore_response.status_code, 302)
        self.assertEqual(mocked_post.call_args_list[0][0][0], "/admin/posts/ART-1234/delete")
        self.assertEqual(mocked_post.call_args_list[1][0][0], "/admin/posts/ART-1234/restore")
        self.assertEqual(mocked_post.call_args_list[1][0][1]["restore_status"], "published")

    def test_bootstrap_sync_route_calls_api(self):
        self.authenticate()
        with mock.patch.object(
            ui_app,
            "api_post",
            return_value=FakeResponse(202, "queued", {"count": 4, "skipped": 1, "reset_deleted": 0}),
        ) as mocked_post:
            response = self.client.post(
                "/admin/bootstrap/filesystem-sync",
                data={
                    "content_subdir": "posts/linux",
                    "status": "published",
                    "theme_variant": "midnight",
                    "sync_mode": "update",
                    "keyword_filter": "",
                    "page_limit": "",
                },
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(mocked_post.call_args[0][0], "/admin/bootstrap/filesystem-sync")
        self.assertEqual(mocked_post.call_args[0][1]["sync_mode"], "update")

    def test_unpublish_route_calls_api(self):
        self.authenticate()
        with mock.patch.object(ui_app, "api_post", return_value=FakeResponse(202, "queued")) as mocked_post:
            response = self.client.post("/admin/posts/ART-1234/unpublish", data={"return_to": "/admin"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(mocked_post.call_args[0][0], "/admin/posts/ART-1234/unpublish")

    def test_hard_delete_route_passes_confirmation(self):
        self.authenticate()
        with mock.patch.object(ui_app, "api_post", return_value=FakeResponse(202, "queued")) as mocked_post:
            response = self.client.post(
                "/admin/posts/ART-1234/hard-delete",
                data={"return_to": "/admin", "confirm_article_id": "ART-1234"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(mocked_post.call_args[0][0], "/admin/posts/ART-1234/hard-delete")
        self.assertEqual(mocked_post.call_args[0][1]["confirm_article_id"], "ART-1234")

    def test_public_post_redirects_when_slug_is_alias(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        posts = []
        selected = {"slug": "mastering-the-waves", "title": "Mastering the Waves", "summary": "Summary", "theme_variant": "midnight"}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, posts, selected, "mastering-the-waves")):
            response = self.client.get("/post/old-mastering-waves")
        self.assertEqual(response.status_code, 301)
        self.assertIn("/post/mastering-the-waves", response.location)

    def test_public_post_renders_seo_meta_tags(self):
        payload = {"items": [], "total": 1, "page": 1, "page_size": 10}
        selected = {
            "slug": "mastering-the-waves",
            "title": "Mastering the Waves",
            "summary": "Wave summary",
            "seo_title": "SEO Waves",
            "seo_description": "SEO Waves Description",
            "canonical_url": "https://blog.example/waves",
            "og_image_url": "https://blog.example/waves.png",
            "theme_variant": "midnight",
            "html_body": "<p>Body</p>",
            "markdown_body": "# Body",
            "body_format": "markdown",
            "author_email": "author@example.com",
            "updated_at": "2026-04-24T12:00:00+00:00",
            "published_at": "2026-04-24T12:00:00+00:00",
            "tags": ["linux"],
        }
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [selected], selected, None)):
            response = self.client.get("/post/mastering-the-waves")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<title>SEO Waves</title>", body)
        self.assertIn('meta name="description" content="SEO Waves Description"', body)
        self.assertIn('link rel="canonical" href="https://blog.example/waves"', body)
        self.assertIn('property="og:image" content="https://blog.example/waves.png"', body)

    def test_sitemap_and_rss_routes_render(self):
        posts = [
            {
                "slug": "mastering-the-waves",
                "title": "Mastering the Waves",
                "summary": "Wave summary",
                "html_body": "<p>Body</p>",
                "published_at": "2026-04-24T12:00:00+00:00",
                "updated_at": "2026-04-24T12:00:00+00:00",
            }
        ]
        with mock.patch.object(ui_app, "fetch_all_public_posts", return_value=posts):
            sitemap_response = self.client.get("/sitemap.xml")
            rss_response = self.client.get("/rss.xml")
        self.assertEqual(sitemap_response.status_code, 200)
        self.assertIn("<urlset", sitemap_response.get_data(as_text=True))
        self.assertEqual(rss_response.status_code, 200)
        self.assertIn("<rss", rss_response.get_data(as_text=True))

    def test_google_oauth_guardrail_redirects_when_not_configured(self):
        with mock.patch.object(ui_app, "GOOGLE_CLIENT_ID", ""), mock.patch.object(ui_app, "GOOGLE_CLIENT_SECRET", ""):
            response = self.client.get("/admin/login/google")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.location)


if __name__ == "__main__":
    unittest.main()
