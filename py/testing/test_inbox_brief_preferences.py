"""Check the daily brief's preference plan, the sections an owner adds from their own routines, and how the brief
renders both. Run with the server's Python, which has bs4 and markdown."""
import datetime
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

import bs4
# imported before patch.dict(sys.modules) so it is kept: a re-import mid-run makes a second Extension class
import markdown.extensions.tables  # noqa: F401


DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/inbox-daily-brief.automation.json"
SOURCES = json.loads(DEFINITION.read_text(encoding="utf-8"))["nodeSources"]


def node(name, pixel):
    """One step's code, with the pixel calls it makes answered by the given mock."""
    semoss = types.ModuleType("semoss")
    semoss.Insight = Mock(return_value=types.SimpleNamespace(run_pixel=pixel))
    namespace = {}
    with patch.dict(sys.modules, {"semoss": semoss}):
        exec(compile(SOURCES[name], name, "exec"), namespace)
    return namespace


def answered(output):
    return [{"pixelReturn": [{"output": output, "operationType": ["MAP"]}]}]


def model(text):
    return answered({"messageType": "CHAT", "response": text, "parts": []})


def prepared():
    reply = {"n": 1, "subject": "Budget sign-off", "from": "Lee, Dana", "priority": "P1", "dueLabel": "",
             "dueDate": None, "urgency": 1, "draft": "saved", "location": "Drafts", "webLink": "https://mail.example/b"}
    waiting = {"subject": "Vendor contract", "from": "Sam Roe", "priority": "P2", "waited": "waiting 3 days",
               "dueLabel": "", "dueDate": None, "urgency": 2}
    expenses = {"stepId": "step-1", "title": "File expenses", "thread": "Finance", "dueLabel": "due today",
                "dueDate": "2026-11-10", "urgency": 0}
    travel = {"stepId": "step-2", "title": "Book travel", "thread": "Offsite", "dueLabel": "", "dueDate": None,
              "urgency": 3}
    teams = {"title": "Deck review", "channel": "teams", "from": "Max Quinn", "priority": "P2", "dueLabel": "",
             "dueDate": None, "urgency": 2}
    return {"today": "2026-11-10", "timezone": "America/New_York", "replies": [reply], "waiting": [waiting],
            "waitingMore": 0, "actions": [expenses, travel], "actionsMore": 0, "staleActions": 0,
            "priorities": [expenses, reply, waiting, teams],
            "meetings": [{"n": 1, "subject": "Standup", "start": "09:00", "end": "09:15", "recurring": True,
                          "organizer": "Hickey, Caroline", "preReads": []},
                         {"n": 2, "subject": "All-Hands", "start": "11:00", "end": "12:00", "organizer": None,
                          "preReads": []}],
            "allDay": [{"subject": "Kunal - OOO - DU", "webLink": "https://cal.example/ooo"},
                       {"subject": "Veterans Day"}],
            "meetingInputs": []}


class PreferencePlanTests(unittest.TestCase):
    def plan(self, answers, preferences="Start with a countdown to Christmas. Hide other people's time off.",
             **scope):
        self.pixel = Mock(side_effect=[model(answer) for answer in answers])
        arrange = node("arrange", self.pixel)
        return arrange["run"]({"prepared": prepared(), "preferences": preferences, "brief_model": "text-model",
                               "priority_count": 3, **scope})

    def test_no_preferences_asks_no_model_and_changes_nothing(self):
        self.assertEqual({}, self.plan([], preferences="  "))
        self.pixel.assert_not_called()

    def test_the_model_names_records_and_dates_and_code_does_the_rest(self):
        answer = json.dumps({
            "top": ["{days_until:12-25} days until Christmas", "{days_until:11-11} days to go", "Feb {days_until:02-30}",
                    "{days_until:2025-12-25} days", "{broken"],
            "bottom": [], "leave_out": ["d1", "actions", "zz9", "d1"], "first": ["p3", "m2", "d1"],
            "order": ["meetings", "s1", "nowhere"],
            "cannot": ["Include the weather", "", "two", "three", "four"]})
        added = {"sections": [{"name": "Client news", "text": "- Acme signed"}]}
        plan = self.plan(["Here is the plan:\n```json\n" + answer + "\n```"], added=added)
        # 45 days from November 10, and one day reads as one day; a date that cannot be counted drops its line
        self.assertEqual(["45 days until Christmas", "1 day to go"], plan["top"])
        self.assertEqual({"allDay": [0]}, plan["hide"])
        self.assertEqual(["actions"], plan["hideSections"])
        # meetings keep their time order, and what is left out is not also put first
        self.assertEqual({"priorities": [2]}, plan["first"])
        self.assertEqual(["meetings", "s1"], plan["order"])
        self.assertEqual(["All-day event: Kunal - OOO - DU", "Open action items (the whole section)"], plan["leftOut"])
        self.assertEqual(["Include the weather", "two", "three"], plan["cannot"])
        pixel = self.pixel.call_args.args[0]
        command = json.JSONDecoder().raw_decode(pixel, pixel.index("command=") + len("command="))[0][0]
        self.assertIn("Today is Tuesday, 2026-11-10.", command)
        outline = json.loads(command.split("Outline: ", 1)[1])["sections"]
        self.assertEqual(["priorities", "replies", "meetings", "actions", "s1"], [part["id"] for part in outline])
        items = {item["id"]: item["text"] for part in outline for item in part["items"]}
        # only the priorities the brief will list are offered
        self.assertEqual(["p1", "p2", "p3", "r1", "w1", "d1", "d2", "m1", "m2", "x1", "x2"], list(items))
        self.assertEqual("New email to answer: Budget sign-off, from Dana Lee, P1", items["r1"])
        self.assertEqual("09:00-09:15 Standup, organized by Caroline Hickey, recurring", items["m1"])
        self.assertEqual("11:00-12:00 All-Hands, you organize it", items["m2"])

    def test_an_unreadable_answer_is_asked_again_then_reported_never_guessed(self):
        self.assertEqual(["Hello"], self.plan(["", '{"top": ["Hello"]}'])["top"])
        self.assertEqual(2, self.pixel.call_count)
        self.assertEqual({"failed": True}, self.plan(["I could not decide.", "[]"]))


class BriefRenderTests(unittest.TestCase):
    def render(self, plan=None, added=None):
        brief = node("brief", Mock())
        return brief["run"]({"prepared": prepared(), "meeting_notes": {"results": []}, "priority_count": 3,
                             "plan": plan, "added": added})

    def headings(self, output):
        return [line for line in output.splitlines() if line.startswith("## ")]

    def test_without_a_plan_the_brief_is_the_usual_four_sections(self):
        output = self.render()
        self.assertEqual(["## Suggested priorities", "## Needs your reply", "## Today's meetings",
                          "## Open action items"], self.headings(output))
        self.assertNotIn("---", output)
        self.assertIn("Veterans Day", output)

    def test_a_plan_adds_lines_leaves_out_reorders_and_says_what_it_did(self):
        output = self.render(plan={
            "top": ["45 days until Christmas", "<b>[x](https://evil.example)</b>"], "bottom": ["Have a good day"],
            "hide": {"allDay": [0], "priorities": [0], "meetings": [1]}, "hideSections": ["replies"],
            "first": {"priorities": [2], "actions": [1]}, "order": ["meetings"],
            "leftOut": ["All-day event: Kunal - OOO - DU"], "cannot": ["Include the weather"]})
        self.assertTrue(output.startswith("45 days until Christmas\n\n"))
        # the plan's own lines are text: no markup or link of its making reaches the brief
        self.assertNotIn("<b>", output)
        self.assertNotIn("](https://evil.example)", output)
        self.assertEqual(["## Today's meetings", "## Suggested priorities", "## Open action items"],
                         self.headings(output))
        self.assertNotIn("Kunal", output.split("---")[0])
        self.assertIn("All day: Veterans Day", output)
        self.assertNotIn("All\\-Hands", output)
        self.assertIn("Standup", output)
        # the one put first leads, and the next in rank fills the place of the one left out
        priorities = output.split("## Suggested priorities\n", 1)[1].split("\n## ", 1)[0]
        self.assertEqual(["1. Reply to Sam Roe", "2. Reply to Dana Lee", "3. Reply in Teams"],
                         [line[:line.index(":")] for line in priorities.strip().splitlines()])
        actions = output.split("## Open action items\n", 1)[1]
        self.assertLess(actions.index("Book travel"), actions.index("File expenses"))
        tail = output.split("---\n", 1)[1]
        self.assertIn("Have a good day", output.split("---")[0])
        self.assertIn("Left out by your preferences: All\\-day event: Kunal \\- OOO \\- DU.", tail)
        self.assertIn("Your preferences also ask for something this brief cannot do: Include the weather.", tail)

    def test_preferences_that_could_not_be_read_are_said_not_dropped_silently(self):
        output = self.render(plan={"failed": True})
        self.assertIn("Your preferences could not be applied to this brief.", output)
        self.assertEqual(4, len(self.headings(output)))

    def test_an_added_section_is_the_routines_answer_under_its_name(self):
        added = {"sections": [
            {"name": "Client news", "text": "# Headlines\n- [Acme](https://outlook.office.com/x) signed\n"},
            {"name": "Jira", "text": ""}, {"name": "Long one", "text": "word " * 2000}]}
        output = self.render(added=added, plan={"order": ["s1"], "hideSections": ["s3"]})
        self.assertEqual(["## Client news", "## Suggested priorities", "## Needs your reply", "## Today's meetings",
                          "## Open action items", "## Jira"], self.headings(output))
        # the routine's own heading sits under the section's, and its Markdown is kept
        self.assertIn("## Client news\n\n### Headlines\n- [Acme](https://outlook.office.com/x) signed", output)
        self.assertIn("## Jira\n\nThis section could not be prepared today.", output)
        long_one = self.render(added=added).split("## Long one\n", 1)[1]
        self.assertLess(len(long_one), 6200)
        self.assertIn("(Shortened. Open Routines for the full result.)", long_one)


class AddedSectionsTests(unittest.TestCase):
    BEGAN = datetime.datetime.now(datetime.timezone.utc)

    def routine(self, key, delivery="brief", enabled=True, status=None, minutes=0, summary=None):
        at = (self.BEGAN + datetime.timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
        last = None if status is None else {"status": status, "startedAt": at, "summary": summary,
                                            "finishedAt": None if status == "running" else at}
        return {"id": key, "name": "Routine " + key, "delivery": delivery, "enabled": enabled, "lastRun": last}

    def run_sections(self, lists, refuse=None, wait=600):
        """Each list is what the owner's routines look like at one look; the last one repeats."""
        looks = iter(lists)
        state = {"last": lists[0]}
        self.started = []

        def pixel(text, raw=True):
            if text.startswith("WorkListCustomRoutines("):
                state["last"] = next(looks, state["last"])
                return answered({"routines": state["last"]})
            key = json.loads(text.split("routineId=", 1)[1].split(", action=", 1)[0])[0]
            self.assertIn('action=["run"]', text)
            self.started.append(key)
            if refuse and key in refuse:
                return [{"pixelReturn": [{"output": refuse[key], "operationType": ["ERROR"]}]}]
            return answered({"status": "started"})

        sections = node("sections", Mock(side_effect=pixel))
        # a clock that only moves while the step sleeps
        clock = {"now": 0.0}
        self.sleep = Mock(side_effect=lambda seconds: clock.update(now=clock["now"] + seconds))
        with patch.object(sections["time"], "sleep", self.sleep), \
                patch.object(sections["time"], "monotonic", lambda: clock["now"]):
            return sections["run"]({"section_wait_seconds": wait})["sections"]

    def test_only_routines_delivered_to_the_brief_and_turned_on_are_run(self):
        listed = [self.routine("mail", delivery="email"), self.routine("off", enabled=False)]
        self.assertEqual([], self.run_sections([listed]))
        self.assertEqual([], self.started)

    def test_each_section_is_started_waited_for_and_read_once_it_has_answered(self):
        old = "yesterday's answer"
        before = [self.routine("news", status="completed", minutes=-600, summary=old), self.routine("jira"),
                  self.routine("slow", status="running", minutes=-1), self.routine("blocked")]
        going = [self.routine("news", status="running", minutes=1), self.routine("jira", status="running", minutes=1),
                 self.routine("slow", status="running", minutes=-1), self.routine("blocked")]
        done = [self.routine("news", status="completed", minutes=2, summary="- Acme signed"),
                self.routine("jira", status="failed", minutes=2),
                self.routine("slow", status="completed", minutes=2, summary="Already going when the brief asked"),
                self.routine("blocked")]
        sections = self.run_sections([before, going, going, done], refuse={
            "slow": "The routine is already running", "blocked": "The SEMOSS scheduler is unavailable"})
        # by name, so a section keeps its place from one brief to the next
        self.assertEqual(["blocked", "jira", "news", "slow"], self.started)
        self.assertEqual([("Routine blocked", ""), ("Routine jira", ""), ("Routine news", "- Acme signed"),
                          ("Routine slow", "Already going when the brief asked")],
                         [(section["name"], section["text"]) for section in sections])
        # it looked again while runs were going, and stopped as soon as each had ended
        self.assertEqual(2, self.sleep.call_count)

    def test_a_section_still_running_at_the_limit_is_left_out_and_never_yesterdays_answer(self):
        old = [self.routine("news", status="completed", minutes=-600, summary="old")]
        stuck = [self.routine("news", status="running", minutes=1)]
        sections = self.run_sections([old, stuck], wait=600)
        self.assertEqual([("Routine news", "")], [(section["name"], section["text"]) for section in sections])
        # it waited the whole limit, five seconds at a time
        self.assertEqual(120, self.sleep.call_count)

    def test_a_run_that_never_starts_is_given_up_on_within_a_minute(self):
        old = [self.routine("news", status="completed", minutes=-600, summary="old")]
        sections = self.run_sections([old], wait=600)
        self.assertEqual([("Routine news", "")], [(section["name"], section["text"]) for section in sections])
        self.assertEqual(12, self.sleep.call_count)


class BriefEmailTests(unittest.TestCase):
    def test_a_sections_microsoft_link_is_kept_and_any_other_new_link_is_not(self):
        pixel = Mock(return_value=answered({}))
        deliver = node("deliver", pixel)
        brief = ("## Client news\n\n[Thread](https://outlook.office.com/mail/1) and [news](https://evil.example/x) "
                 "and [gathered](https://mail.example/b)\n")
        result = deliver["run"]({"owner_email": "owner@example.com", "brief": brief, "context": {
            "today": "2026-11-10", "replies": [{"webLink": "https://mail.example/b"}]}})
        links = [link.get("href") for link in bs4.BeautifulSoup(result["html"], "html.parser").find_all("a")]
        self.assertEqual(["https://outlook.office.com/mail/1", "https://mail.example/b"], links)
        self.assertIn("news", result["html"])


if __name__ == "__main__":
    unittest.main()
