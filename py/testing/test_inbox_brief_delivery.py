"""Verify assembled brief text survives the shipped HTML/mail boundary."""
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

import bs4
import markdown


DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/inbox-daily-brief.automation.json"


class InboxBriefDeliveryTests(unittest.TestCase):
    def setUp(self):
        sources = json.loads(DEFINITION.read_text())["nodeSources"]
        self.pixel = Mock(return_value=[{"pixelReturn": [{"output": {}, "operationType": ["MAP"]}]}])
        semoss = types.ModuleType("semoss")
        semoss.Insight = Mock(return_value=types.SimpleNamespace(run_pixel=self.pixel))
        self.prepare, self.brief, self.deliver = {}, {}, {}
        with patch.dict(sys.modules, {"semoss": semoss}):
            for name in ("prepare", "brief", "deliver"):
                exec(compile(sources[name], str(DEFINITION), "exec"), getattr(self, name))

    def test_literal_markup_ampersands_and_encoded_source_links_survive_email(self):
        title = 'R&D <review> **literal** [false link](https://untrusted.example)'
        url = 'https://outlook.example/mail/item%2Fpart(2)?query=a+b&progress=50%25'
        context = {"today": "2026-10-23", "timezone": "America/New_York", "replies": [], "meetings": [],
                   "actionItems": [{"title": title, "webLink": url, "due": "2026-10-23"}]}
        prepared = self.prepare["run"]({"context": context, "saved": {"replies": []}})
        brief = self.brief["run"]({"prepared": prepared})
        result = self.deliver["run"]({"brief": brief, "context": context, "owner_email": "owner@example.com"})
        soup = bs4.BeautifulSoup(result["html"], "html.parser")

        self.assertEqual(2, soup.get_text().count(title))  # Priority mention and the full action entry.
        self.assertEqual([url, url], [a["href"] for a in soup.find_all("a")])
        self.assertEqual(4, len(soup.find_all("h2")))
        self.assertIsNone(soup.find("review"))
        self.assertIsNone(soup.find("strong"))
        pixel = self.pixel.call_args.args[0]
        self.assertTrue(pixel.startswith('MicrosoftOutlookSendMail(to=[["owner@example.com"]]'))
        self.assertNotIn("<review>", pixel)
        self.assertEqual("owner@example.com", result["to"])
        self.assertTrue(result["sent"])

    def test_delivery_drops_images_and_links_absent_from_source_context(self):
        brief = '# <script>alert(1)</script>\n\n[Unknown](https://untrusted.example) ![pixel](https://tracker.example/a)'
        result = self.deliver["run"]({"brief": brief, "context": {"today": "2026-10-23"},
                                      "owner_email": "owner@example.com"})
        soup = bs4.BeautifulSoup(result["html"], "html.parser")
        self.assertFalse(soup.find_all(["img", "a", "script"]))
        self.assertIn("<script>alert(1)</script>", soup.get_text())


if __name__ == "__main__":
    unittest.main()
