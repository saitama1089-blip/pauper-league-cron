import os
import unittest
from unittest.mock import Mock, patch
import requests

with patch.dict(os.environ, {"SUPABASE_URL": "https://example.invalid", "SUPABASE_SERVICE_ROLE": "test-only"}):
    import PauperBatchLeagues_API as scraper

TABLE = '<table class="table-tournament"><tr><td>5-0</td><td><a href="/deck/8032965">Terror</a></td><td>Pilot</td></tr></table>'


def response(status, text=""):
    result = Mock(status_code=status, text=text)
    return result


class ScraperTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch("builtins.print"))
        self.enterContext(patch.object(scraper.time, "sleep"))
        self.enterContext(patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": ""}))
        self.driver = Mock()
        self.enterContext(patch.object(scraper, "setup_driver", return_value=self.driver))
        self.enterContext(patch.object(scraper, "CHALLENGE_LOOKBACK_DAYS", 1))
        self.get = self.enterContext(patch.object(scraper.SESSION, "get", side_effect=AssertionError("Unexpected network")))
        self.post = self.enterContext(patch.object(scraper.requests, "post", return_value=response(204)))

    def test_ordinary_missing_events_do_not_retry_or_open_chrome(self):
        self.get.side_effect = None
        self.get.return_value = response(404)
        self.assertEqual(scraper.fetch_page_html(self.driver, "https://example.invalid/event"), "")
        self.get.assert_called_once()
        self.driver.get.assert_not_called()

    def test_successful_http_page_does_not_need_chrome(self):
        self.get.side_effect = None
        self.get.return_value = response(200, TABLE)
        self.assertEqual(scraper.fetch_page_html(self.driver, "https://example.invalid/event"), TABLE)
        self.driver.get.assert_not_called()

    def test_browser_challenge_is_not_treated_as_results(self):
        self.get.side_effect = requests.Timeout("offline")
        self.driver.page_source = "<html>Just a moment</html>"
        with patch.object(scraper, "load_page_with_retries", return_value=True):
            self.assertEqual(scraper.fetch_page_html(self.driver, "https://example.invalid/event"), "")

    def test_normal_scrape_submits_results_and_closes_driver(self):
        with patch.object(scraper, "fetch_page_html", return_value=TABLE):
            scraper.main()
        self.assertEqual(self.post.call_count, 8)
        self.driver.quit.assert_called_once()

    def test_partial_insert_failure_is_visible_after_other_dates_continue(self):
        self.post.side_effect = [response(500)] + [response(204)] * 7
        with patch.object(scraper, "fetch_page_html", return_value=TABLE), self.assertRaises(RuntimeError):
            scraper.main()
        self.assertEqual(self.post.call_count, 8)
        self.driver.quit.assert_called_once()

    def test_no_results_cannot_silently_pass(self):
        with patch.object(scraper, "fetch_page_html", return_value=""), self.assertRaises(RuntimeError):
            scraper.main()
        self.post.assert_not_called()
        self.driver.quit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
