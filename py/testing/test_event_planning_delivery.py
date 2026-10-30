"""Verify event planning keeps and attaches only the sheets of meetings the plan gave rows, and emails the digest as the
Deloitte-styled HTML that Routines also shows."""
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

# imported before patch.dict(sys.modules) so they are kept: a re-import mid-run makes second copies of their classes
import bs4  # noqa: F401
import markdown.extensions.tables  # noqa: F401
import openpyxl

DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/event-planning.automation.json"
ROW = {"key": "", "sitting": "2026-11-03", "start": "13:00", "topic": "Migration cutover", "presenter": "Priya Shah",
       "presenterEmail": "priya@example.com", "minutes": 20, "status": "Proposed", "materials": "", "notes": "",
       "source": "Email: cutover dates"}


def _returns(output, error=False):
    return [{"pixelReturn": [{"output": output, "operationType": ["ERROR" if error else "MAP"]}]}]


def _node(name, run_pixel, root=""):
    source = json.loads(DEFINITION.read_text())["nodeSources"][name]
    semoss = types.ModuleType("semoss")
    semoss.Insight = Mock(return_value=types.SimpleNamespace(run_pixel=run_pixel))
    smssutil = types.ModuleType("smssutil")
    smssutil.smss_get_runtime_var = Mock(return_value=root)
    scope = {}
    with patch.dict(sys.modules, {"semoss": semoss, "smssutil": smssutil}):
        exec(compile(source, str(DEFINITION), "exec"), scope)
    return scope["run"]


def _meeting(pin, title, rows=()):
    return {"pin": pin, "title": title,
            "sheet": {"path": "collaboration/routines/event-planning/" + title + ".xlsx", "name": title + ".xlsx",
                      "exists": bool(rows), "readable": True, "rows": list(rows), "deleted": []}}


class EventPlanningDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.pixels = []
        self.root = tempfile.mkdtemp()

    def _run_pixel(self, pixel, raw=True):
        self.pixels.append(pixel)
        return _returns({})

    def _run(self, plan_rows):
        # the second meeting has last month's row in its sheet, but nothing planned this run
        old = {**ROW, "key": "k1", "sitting": "2026-10-01", "topic": "Budget review", "presenterEmail": "",
               "presenter": "", "ownerEdited": []}
        context = {"today": "2026-10-29", "meetings": [_meeting("p1", "Northwind eng weekly"),
                                                       _meeting("p2", "Board prep", [old])]}
        plan = {"digest": "## Northwind eng weekly\n\n- Cutover planned for the next sitting.",
                "meetings": [{"pin": "p1", "rows": plan_rows}, {"pin": "p2", "rows": []}]}
        scope = {"owner_email": "owner@example.com", "context": context, "plan": plan}
        scope["sheets"] = _node("sheets", self._run_pixel, self.root)(scope)
        scope["digest"] = _node("digest", self._run_pixel)(scope)
        delivery = _node("deliver", self._run_pixel)(scope)
        return scope, delivery

    def test_only_a_meeting_with_planned_rows_gets_its_sheet_kept_and_attached(self):
        scope, delivery = self._run([ROW])
        draft, save, send = self.pixels
        self.assertTrue(draft.startswith('MicrosoftOutlookSaveDraft(to=[["priya@example.com"]]'))
        self.assertTrue(save.startswith('SaveUserAssetsBase64(filePath=[["collaboration/routines/event-planning/'
                                        'Northwind eng weekly.xlsx"]]'))
        self.assertIn('attachments=[["Northwind eng weekly.xlsx"]]', send)
        self.assertEqual(["Northwind eng weekly.xlsx"], delivery["attachments"])
        lineup = openpyxl.load_workbook(Path(self.root) / "Northwind eng weekly.xlsx")["Lineup"]
        self.assertEqual("Migration cutover", lineup["C2"].value)
        self.assertEqual("Asked", lineup["G2"].value)
        self.assertFalse((Path(self.root) / "Board prep.xlsx").exists())
        self.assertIn("- Northwind eng weekly: 1 row, attached", scope["digest"])
        self.assertIn("- Board prep: no rows this run, so no sheet attached", scope["digest"])
        self.assertIn("- Priya Shah (Migration cutover)", scope["digest"])

    def test_the_digest_is_emailed_as_the_branded_html_routines_shows(self):
        scope, delivery = self._run([ROW])
        send = self.pixels[-1]
        self.assertTrue(send.startswith('MicrosoftOutlookSendMail(to=[["owner@example.com"]], '
                                        'subject=["Event Planning - 2026-10-29"]'))
        self.assertIn("html=[true]", send)
        html = delivery["html"]
        self.assertIn("Event Planning &middot; 2026-10-29", html)
        self.assertIn('<h2 style="margin:24px 0 8px; padding:0 0 6px; border-bottom:2px solid #86BC25;', html)
        self.assertIn("Cutover planned for the next sitting.", html)
        # the email body is the same HTML, with its <encode>-safe escaping in the Pixel
        self.assertIn(json.dumps(html).replace("<", "\\u003c"), send)

    def test_a_run_with_no_planned_rows_sends_no_attachments(self):
        scope, delivery = self._run([])
        self.assertEqual(1, len(self.pixels))
        self.assertNotIn("attachments=", self.pixels[0])
        self.assertEqual([], delivery["attachments"])
        self.assertIn("- Northwind eng weekly: no rows this run, so no sheet attached", scope["digest"])
        self.assertIn("- None this run.", scope["digest"])


if __name__ == "__main__":
    unittest.main()
