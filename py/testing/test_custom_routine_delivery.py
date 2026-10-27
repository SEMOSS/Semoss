"""Verify the custom routine's deliver step mails only a filled HTML template, and Markdown answers as before."""
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

import bs4


DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/custom-routine.automation.json"

TEMPLATE = ('<!DOCTYPE html>\n<html lang="en">\n<head><title>Meeting priorities</title></head>\n'
            '<body style="margin:0;">\n<table role="presentation" width="100%"><tr><td>'
            '<h1 style="margin:0;">Your meeting priorities</h1><p>2 attend &middot; 1 needs review</p>'
            '</td></tr></table>\n</body>\n</html>')
CHATTY = ("I have the calendar events for today. Let me review what I've found:\n\n"
          "## Meeting Priorities\n\n- **Standup** - Attend\n\n"
          "HTML email body (to be sent via Outlook):\n\n```html\n" + TEMPLATE + "\n```\n\n"
          "**Subject:** Meeting priorities\n\nThe HTML email has been placed in your Outlook drafts.")


class CustomRoutineDeliveryTests(unittest.TestCase):
    def setUp(self):
        source = json.loads(DEFINITION.read_text())["nodeSources"]["deliver"]
        self.pixel = Mock(return_value=[{"pixelReturn": [{"output": {}, "operationType": ["MAP"]}]}])
        semoss = types.ModuleType("semoss")
        semoss.Insight = Mock(return_value=types.SimpleNamespace(run_pixel=self.pixel))
        self.deliver = {}
        with patch.dict(sys.modules, {"semoss": semoss}):
            exec(compile(source, str(DEFINITION), "exec"), self.deliver)

    def _deliver(self, answer):
        return self.deliver["run"]({"answer": answer, "owner_email": "owner@example.com",
                                    "routine_name": "Event Reviewer", "delivery": "email"})

    def test_only_the_html_document_is_mailed(self):
        result = self._deliver(CHATTY)
        soup = bs4.BeautifulSoup(result["html"], "html.parser")
        self.assertEqual("Your meeting priorities", soup.h1.get_text())
        for chatter in ("Let me", "Meeting Priorities", "```", "Subject", "Outlook drafts"):
            self.assertNotIn(chatter, soup.get_text())
        self.assertNotIn("DOCTYPE", result["html"])
        self.assertFalse(soup.find_all(["html", "head", "title", "body"]))
        pixel = self.pixel.call_args.args[0]
        self.assertTrue(pixel.startswith('MicrosoftOutlookSendMail(to=[["owner@example.com"]], '
                                         'subject=["Event Reviewer"]'))
        self.assertIn("Your meeting priorities", pixel)
        self.assertNotIn("Outlook drafts", pixel)
        self.assertTrue(result["sent"])

    def test_markdown_answer_still_renders(self):
        result = self._deliver("Nothing needs your attention today.\n\n- **Standup** - attend\n")
        soup = bs4.BeautifulSoup(result["html"], "html.parser")
        self.assertEqual("Standup", soup.strong.get_text())
        self.assertIn("Nothing needs your attention today.", soup.get_text())


if __name__ == "__main__":
    unittest.main()
