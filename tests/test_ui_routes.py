import importlib.util
import sys
import unittest
import tempfile
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

    def test_lane_public_index_applies_lane_posture_and_tag(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/blog?lane=blackknight")
        self.assertEqual(response.status_code, 200)
        mocked_fetch.assert_called_once()
        self.assertEqual(mocked_fetch.call_args[0][3], "blackknightcontroller")
        self.assertEqual(mocked_fetch.call_args[0][4], "blackknightcontroller-recovery-weekend-repeatable-lab")
        body = response.get_data(as_text=True)
        self.assertIn("<title>BlackKnightController | Infrastructure automation and practical systems support</title>", body)
        self.assertNotIn("No article is available yet", body)
        self.assertNotIn("BlackKnight Articles", body)
        self.assertIn('class="theme-midnight"', body)

    def test_auzietek_lane_keeps_business_front_door(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/blog?lane=auzietek")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "services")
        self.assertEqual(mocked_fetch.call_args[0][4], "infrastructure-automation-that-stays-repeatable")
        body = response.get_data(as_text=True)
        self.assertIn("<title>Auzietek | Infrastructure automation and practical systems support</title>", body)
        self.assertNotIn("No article is available yet", body)
        self.assertNotIn("Auzietek Articles", body)

    def test_auzietek_thinktank_page_renders_static_page_and_related_tag(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/thinktank", headers={"Host": "auzietek.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "think-tank")
        body = response.get_data(as_text=True)
        self.assertIn("<title>Auzietek ThinkTank | Human-Centered Systems and Future Infrastructure</title>", body)
        self.assertIn('property="og:type" content="article"', body)
        self.assertIn('"@type":"BreadcrumbList"', body)
        self.assertNotIn("No article is available yet", body)
        self.assertNotIn("Total 0", body)
        self.assertIn("Ideas with a path toward useful systems.", body)
        self.assertIn('href="/thinktank" class="active"', body)
        self.assertIn("RACS remains alive", body)

    def test_auzietek_root_emits_homepage_metadata_and_website_schema(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/", headers={"Host": "beta.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<title>Auzietek | Practical Infrastructure Automation and AIOps</title>", body)
        self.assertIn('link rel="canonical" href="https://beta.auzietek.com/"', body)
        self.assertIn('property="og:type" content="website"', body)
        self.assertIn('"@type":"WebSite"', body)
        self.assertIn('"@type":"Organization"', body)
        self.assertNotIn("No article is available yet", body)

    def test_services_page_emits_service_schema(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/services", headers={"Host": "beta.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<title>Infrastructure Automation Services for Small Teams | Auzietek</title>", body)
        self.assertIn('"@type":"Service"', body)
        self.assertIn('link rel="canonical" href="https://beta.auzietek.com/services"', body)

    def test_root_defaults_to_auzietek_when_host_has_no_lane(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/", headers={"Host": "localhost"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "services")
        body = response.get_data(as_text=True)
        self.assertIn('class="theme-auzietek"', body)
        self.assertIn("What can Auzietek do for you?", body)
        self.assertIn('href="/" class="active"', body)

    def test_root_respects_non_auzietek_host_lane(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/", headers={"Host": "blackknight.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "blackknightcontroller")
        body = response.get_data(as_text=True)
        self.assertIn('class="theme-midnight"', body)
        self.assertIn("What can BlackKnightController do for you?", body)

    def test_lane_homepage_with_featured_article_keeps_site_identity_metadata(self):
        payload = {"items": [], "total": 1, "page": 1, "page_size": 10}
        selected = {
            "slug": "blackknightcontroller-lab-proof",
            "title": "BlackKnightController lab proof",
            "summary": "Featured article summary",
            "theme_variant": "midnight",
            "tags": ["blackknightcontroller"],
        }
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [selected], selected, None)):
            response = self.client.get("/", headers={"Host": "blackknight.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<title>BlackKnightController | Rebuild real infrastructure from power button to running service.</title>", body)
        self.assertIn('link rel="canonical" href="https://blackknight.auzietek.com/"', body)
        self.assertIn('property="og:type" content="website"', body)
        self.assertIn('"@type":"WebSite"', body)
        self.assertIn("What can BlackKnightController do for you?", body)

    def test_authoritative_host_ignores_cross_lane_query(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get(
                "/blog?lane=retro&theme=retro",
                headers={"Host": "linux-users.auzietek.com"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "linux")
        body = response.get_data(as_text=True)
        self.assertIn('class="theme-linux-pro"', body)
        self.assertIn("Linux Users", body)

    def test_fqdn_lane_nav_links_use_sibling_hosts(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/blog", headers={"Host": "linux-users.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('href="https://blackknight.auzietek.com/"', body)
        self.assertIn('href="https://retro-users.auzietek.com/blog"', body)
        self.assertNotIn('href="/blog?lane=retro"', body)

    def test_static_auzietek_page_redirects_from_other_lane_host(self):
        response = self.client.get("/thinktank", headers={"Host": "linux-users.auzietek.com"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "https://beta.auzietek.com/thinktank")

    def test_auzietek_articles_page_can_show_all_recent_articles(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/articles", headers={"Host": "auzietek.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mocked_fetch.call_args[0][3])
        body = response.get_data(as_text=True)
        self.assertIn("Field notes, walkthroughs, and proof-backed teaching material.", body)
        self.assertNotIn("<h2>Recent Articles</h2>", body)

    def test_resource_card_links_have_light_theme_contrast_rules(self):
        template = (ui_app.UI_SRC / "templates" / "public_index.html").read_text()
        self.assertIn("body.theme-auzietek .resource-card a", template)
        self.assertIn("body.theme-linux-pro .resource-card a", template)
        self.assertIn("body.theme-retro .resource-card a", template)
        self.assertIn("color: #ffffff", template)

    def test_static_page_overrides_load_from_content_mount(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            page_dir = Path(temp_dir) / "site" / "pages"
            page_dir.mkdir(parents=True)
            (page_dir / "principles.md").write_text(
                """---
page: principles
title: Mounted Principles
body: Mounted summary
points:
  - Mounted point one
  - Mounted point two
---

## Mounted heading

Mounted body from private content payload.
""",
                encoding="utf-8",
            )
            overrides = ui_app.load_static_page_overrides(temp_dir)
        self.assertIn("principles", overrides)
        self.assertEqual(overrides["principles"]["title"], "Mounted Principles")
        self.assertEqual(overrides["principles"]["points"], ["Mounted point one", "Mounted point two"])
        self.assertIn("<h2>Mounted heading</h2>", overrides["principles"]["html_content"])

    def test_principles_page_contains_evidence_and_boundaries(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/principles", headers={"Host": "auzietek.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Evidence before action", body)
        self.assertIn("Progressive trust and explicit boundaries", body)
        self.assertIn("BlackKnightController is the practical edge", body)

    def test_aiops_page_explains_bounded_operating_loop(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/aiops", headers={"Host": "auzietek.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("signal", body)
        self.assertIn("Bootstrap CLI versus controller", body)
        self.assertIn("Receipts as handoff", body)

    def test_business_case_page_labels_benchmarks_and_assumptions(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)):
            response = self.client.get("/business-case", headers={"Host": "auzietek.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Public benchmarks, carefully used", body)
        self.assertIn("Assumption, not a guarantee", body)
        self.assertIn("Uptime Institute Annual Outage Analysis 2025", body)

    def test_blackknight_lab_host_selects_blackknight_lane(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], None, None)) as mocked_fetch:
            response = self.client.get("/blog", headers={"Host": "blackknight.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "blackknightcontroller")
        self.assertEqual(mocked_fetch.call_args[0][4], "blackknightcontroller-recovery-weekend-repeatable-lab")
        body = response.get_data(as_text=True)
        self.assertIn("<title>BlackKnightController | Infrastructure automation and practical systems support</title>", body)
        self.assertIn('link rel="canonical" href="http://blackknight.lab.auzietek.com/blog"', body)
        self.assertIn('class="theme-midnight"', body)

    def test_linux_users_lab_host_selects_linux_lane(self):
        payload = {"items": [], "total": 1, "page": 1, "page_size": 10}
        selected = {
            "slug": "linux-find-regex",
            "title": "Linux find regex",
            "summary": "Linux summary",
            "html_body": "<p>Linux body</p>",
            "theme_variant": "linux-pro",
            "tags": ["linux"],
        }
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [selected], selected, None)) as mocked_fetch:
            response = self.client.get("/blog", headers={"Host": "linux-users.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "linux")
        body = response.get_data(as_text=True)
        self.assertIn("<title>Linux find regex | Linux Users</title>", body)
        self.assertIn('class="theme-linux-pro"', body)
        self.assertIn("linux-mag-nav", body)
        self.assertIn("practical systems", body)
        self.assertIn("Current Lab Build", body)
        self.assertIn("Companion guide repo", body)

    def test_lane_public_post_filters_post_list_by_lane_tag(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        selected = {
            "slug": "rx-demo-part-1-cloud-native-observability",
            "title": "RX-Demo Part 1",
            "summary": "Summary",
            "theme_variant": "linux-pro",
            "tags": ["linux"],
        }
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [selected], selected, None)) as mocked_fetch:
            response = self.client.get("/post/rx-demo-part-1-cloud-native-observability", headers={"Host": "linux-users.lab.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mocked_fetch.call_args[0][3], "linux")

    def test_lane_public_post_redirects_misplaced_slug_to_article_lane(self):
        payload = {"items": [], "total": 0, "page": 1, "page_size": 10}
        selected = {
            "slug": "muirc-amigaos41-irc-client-codex",
            "title": "MuIRC",
            "summary": "Summary",
            "theme_variant": "retro",
            "tags": ["retro"],
        }
        with mock.patch.object(ui_app, "fetch_public_payload", return_value=(payload, [], selected, None)):
            response = self.client.get("/post/muirc-amigaos41-irc-client-codex", headers={"Host": "linux-users.lab.auzietek.com"})
        self.assertEqual(response.status_code, 302)
        self.assertIn("http://retro-users.lab.auzietek.com/post/muirc-amigaos41-irc-client-codex?lane=retro", response.location)
        self.assertTrue(response.location.endswith("#article-start"))

    def test_article_lane_prefers_theme_over_generic_services_tag(self):
        selected = {
            "slug": "blackknightcontroller-hardware-as-code",
            "theme_variant": "midnight",
            "tags": ["blackknightcontroller", "services", "lab"],
        }
        self.assertEqual(ui_app.article_lane_key(selected), "blackknight")

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

    def test_public_host_robots_and_sitemap_use_canonical_host_urls(self):
        posts = [
            {
                "slug": "linux-find-regex",
                "title": "Linux find regex",
                "summary": "Summary",
                "theme_variant": "linux-pro",
                "tags": ["linux"],
            },
            {
                "slug": "retro-muirc",
                "title": "Retro MuIRC",
                "summary": "Summary",
                "theme_variant": "retro",
                "tags": ["retro"],
            },
        ]
        with mock.patch.object(ui_app, "fetch_all_public_posts", return_value=posts):
            robots_response = self.client.get("/robots.txt", headers={"Host": "linux-users.auzietek.com"})
            sitemap_response = self.client.get("/sitemap.xml", headers={"Host": "linux-users.auzietek.com"})
        self.assertIn("Sitemap: https://linux-users.auzietek.com/sitemap.xml", robots_response.get_data(as_text=True))
        sitemap = sitemap_response.get_data(as_text=True)
        self.assertIn("<loc>https://linux-users.auzietek.com/</loc>", sitemap)
        self.assertIn("<loc>https://linux-users.auzietek.com/blog</loc>", sitemap)
        self.assertIn("https://linux-users.auzietek.com/post/linux-find-regex", sitemap)
        self.assertNotIn("retro-muirc", sitemap)

    def test_public_host_rss_is_limited_to_authoritative_lane(self):
        posts = [
            {
                "slug": "linux-find-regex",
                "title": "Linux find regex",
                "summary": "Linux summary",
                "html_body": "<p>Linux body</p>",
                "theme_variant": "linux-pro",
                "tags": ["linux"],
                "published_at": "2026-04-24T12:00:00+00:00",
                "updated_at": "2026-04-24T12:00:00+00:00",
            },
            {
                "slug": "retro-muirc",
                "title": "Retro MuIRC",
                "summary": "Retro summary",
                "html_body": "<p>Retro body</p>",
                "theme_variant": "retro",
                "tags": ["retro"],
                "published_at": "2026-04-24T12:00:00+00:00",
                "updated_at": "2026-04-24T12:00:00+00:00",
            },
        ]
        with mock.patch.object(ui_app, "fetch_all_public_posts", return_value=posts):
            response = self.client.get("/rss.xml", headers={"Host": "linux-users.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        rss = response.get_data(as_text=True)
        self.assertIn("<title>Linux Users</title>", rss)
        self.assertIn("https://linux-users.auzietek.com/post/linux-find-regex", rss)
        self.assertIn("Linux body", rss)
        self.assertNotIn("retro-muirc", rss)
        self.assertNotIn("Retro body", rss)

    def test_auzietek_sitemap_includes_static_pages(self):
        with mock.patch.object(ui_app, "fetch_all_public_posts", return_value=[]):
            response = self.client.get("/sitemap.xml", headers={"Host": "beta.auzietek.com"})
        self.assertEqual(response.status_code, 200)
        sitemap = response.get_data(as_text=True)
        self.assertIn("<loc>https://beta.auzietek.com/</loc>", sitemap)
        self.assertIn("<loc>https://beta.auzietek.com/thinktank</loc>", sitemap)
        self.assertIn("<loc>https://beta.auzietek.com/business-case</loc>", sitemap)

    def test_google_oauth_guardrail_redirects_when_not_configured(self):
        with mock.patch.object(ui_app, "GOOGLE_CLIENT_ID", ""), mock.patch.object(ui_app, "GOOGLE_CLIENT_SECRET", ""):
            response = self.client.get("/admin/login/google")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.location)


if __name__ == "__main__":
    unittest.main()
