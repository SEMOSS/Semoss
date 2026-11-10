"""Check that daily-brief rendering preserves records independently of model prose."""
import copy
import json
from pathlib import Path
import unittest


DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/inbox-daily-brief.automation.json"


def action(number, **changes):
    return {"key": "a" + str(number), "title": "Action " + str(number), "thread": "Source " + str(number),
            "dueDate": None, "dueLabel": "No recorded deadline", "group": "flexible", **changes}


def reply(number, **changes):
    return {"key": "r" + str(number), "title": "Reply " + str(number), "from": "Sender " + str(number),
            "dueDate": None, "dueLabel": "No recorded deadline", "group": "flexible", "draft": "not written",
            **changes}


def meeting(number, **changes):
    return {"key": "m" + str(number), "subject": "Meeting " + str(number), "start": "09:00", "end": "09:30",
            "agenda": "Do not print this raw agenda.", "preReads": [], **changes}


def note(index, **changes):
    return {"index": index, "outputs": {"note": {"available": True, "summary": "A useful meeting summary.",
                                                "questions": [], "preReads": [], **changes}}}


class InboxBriefRenderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.renderer = {}
        definition = json.loads(DEFINITION.read_text())
        exec(compile(definition["nodeSources"]["brief"], str(DEFINITION), "exec"), cls.renderer)

    def render(self, replies=None, actions=None, meetings=None, notes=None):
        prepared = {"today": "2026-10-23", "timezone": "America/New_York", "replies": replies or [],
                    "actions": actions or [], "meetings": meetings}
        before = copy.deepcopy(prepared)
        result = self.renderer["run"]({"prepared": prepared, "meeting_notes": {"results": notes or []}})
        self.assertEqual(before, prepared)
        return result

    def section(self, output, name):
        return output.split("## " + name + "\n", 1)[1].split("\n## ", 1)[0]

    def test_fixed_headings_and_distinct_calendar_empty_states(self):
        output = self.render(meetings=[])
        self.assertEqual(["## Suggested priorities", "## Needs your reply", "## Today's meetings", "## Open action items"],
                         [line for line in output.splitlines() if line.startswith("## ")])
        self.assertIn("### 🔴 Due today, 10/23", output)
        self.assertIn("### 🟡 Due soon", output)
        self.assertIn("### 🟢 Flexible", output)
        self.assertEqual(3, output.count("None selected."))
        self.assertNotIn("None identified.", output)
        self.assertIn("The calendar is clear.", output)
        self.assertNotIn("could not be read", output)
        self.assertIn("The calendar could not be read.", self.render(meetings=None))

    def test_only_replies_have_draft_status_and_only_their_own_status(self):
        replies = [reply(1, draft="saved", location="Drafts > Teamwork drafts", latestMessage="SECRET BODY"),
                   reply(2), reply(3, draft="could not be saved"), reply(4, draft="unexpected")]
        output = self.render(replies=replies, actions=[action(1, draft="saved", location="Wrong folder")], meetings=[])
        section = self.section(output, "Needs your reply")
        self.assertEqual(4, section.count("- Draft:"))
        self.assertEqual(1, section.count("Saved in Drafts &gt; Teamwork drafts"))
        self.assertIn("No draft was written.", section)
        self.assertIn("Draft could not be saved.", section)
        self.assertIn("Draft status unavailable.", section)
        self.assertNotIn("SECRET BODY", output)
        self.assertNotIn("Wrong folder", output)
        self.assertNotIn("Action 1", section)

    def test_empty_replies_do_not_promote_action_items_or_historical_replies(self):
        output = self.render(actions=[action(1, title="Reply to the historical request", draft="saved")],
                             meetings=[meeting(1, related=[{"summary": "An old reply was saved."}])])
        self.assertEqual("No new emails need a reply.", self.section(output, "Needs your reply").strip())
        self.assertNotIn("Draft:", output)

    def test_every_meeting_and_overlap_keeps_its_source_identity(self):
        meetings = [meeting(1, webLink="https://calendar.example/one"), meeting(2),
                    meeting(3, allDay=True, start="2026-10-23", end="2026-10-24")]
        output = self.render(meetings=meetings, notes=[note(1, summary="Only the second meeting has notes.")])
        section = self.section(output, "Today's meetings")
        self.assertEqual(3, sum(line.startswith("### ") for line in section.splitlines()))
        self.assertIn("### 09:00–09:30 [Meeting 1](https://calendar.example/one)", section)
        self.assertIn("### 09:00–09:30 Meeting 2", section)
        self.assertIn("### All day Meeting 3", section)
        self.assertEqual(1, section.count("Only the second meeting has notes"))
        self.assertEqual(2, section.count("Brief unavailable."))
        self.assertNotIn("Do not print this raw agenda", section)

    def test_missing_and_invalid_notes_keep_every_pre_read_with_source_read_status(self):
        sources = [{"n": 1, "name": "Read.pdf", "text": "Actual file text", "webLink": "https://files.example/read"},
                   {"n": 2, "name": "Unread.zip", "text": None},
                   {"n": 3, "name": "Also read.docx", "text": "More text"}]
        output = self.render(meetings=[meeting(1, preReads=sources)], notes=[note(0, preReads=[
            {"n": 1, "takeaway": "Useful distinct takeaway"},
            {"n": 2, "takeaway": "Invented content from unread file"},
            {"n": 999, "takeaway": "Unknown attachment"},
        ])])
        self.assertIn("[Read\\.pdf](https://files.example/read) — Useful distinct takeaway", output)
        self.assertIn("Unread\\.zip — Contents unavailable.", output)
        self.assertIn("Also read\\.docx", output)
        self.assertNotIn("Invented content", output)
        self.assertNotIn("Unknown attachment", output)
        fallback = self.render(meetings=[meeting(1, preReads=sources)], notes=[note(0, summary="x" * 1201)])
        self.assertIn("Brief unavailable.", fallback)
        for name in ("Read\\.pdf", "Unread\\.zip", "Also read\\.docx"):
            self.assertIn(name, fallback)
        self.assertIn("Unread\\.zip — Contents unavailable.", fallback)

    def test_invalid_duplicate_and_unknown_note_indices_cannot_replace_meeting_notes(self):
        output = self.render(meetings=[meeting(1), meeting(2)], notes=[
            note(0, summary="First valid note"), note(0, summary="Duplicate must be ignored"),
            note(999, summary="Unknown index"), note(-1, summary="Negative index"),
            note(True, summary="Boolean index"), note("1", summary="String index"),
            {"index": 1, "outputs": {"note": {"available": True, "summary": {"secret": "Not text"}}}},
        ])
        self.assertIn("First valid note", output)
        self.assertEqual(1, output.count("Brief unavailable."))
        for forbidden in ("Duplicate must", "Unknown index", "Negative index", "Boolean index", "String index", "secret"):
            self.assertNotIn(forbidden, output)

    def test_java_json_numeric_round_trip_preserves_notes_and_attachment_identity(self):
        # Gson's untyped maps become Python floats when loop results cross back to Python.
        notes = json.loads(json.dumps([note(0, summary="Valid meeting notes", preReads=[
            {"n": 1, "takeaway": "A valid finding"}])]), parse_int=float)
        output = self.render(meetings=[meeting(1, preReads=[{"n": 1.0, "name": "Read.txt", "text": "Source text"}])],
                             notes=notes)
        self.assertIn("Valid meeting notes", output)
        self.assertIn("Read\\.txt — A valid finding", output)
        self.assertNotIn("Brief unavailable.", output)

    def test_boolean_nonfinite_and_fractional_indices_and_attachment_numbers_are_ignored(self):
        invalid = [True, False, float("nan"), float("inf"), float("-inf"), 0.5, "0"]
        notes = [note(index, summary="Invalid meeting note") for index in invalid]
        notes.append(note(0.0, summary="The valid meeting note", preReads=[
            {"n": number, "takeaway": "Invalid attachment finding"} for number in invalid
        ] + [{"n": 1.0, "takeaway": "The valid attachment finding"}]))
        output = self.render(meetings=[meeting(1, preReads=[{"n": 1, "name": "Read.txt", "text": "Source text"}])],
                             notes=notes)
        self.assertIn("The valid meeting note", output)
        self.assertIn("The valid attachment finding", output)
        self.assertNotIn("Invalid meeting note", output)
        self.assertNotIn("Invalid attachment finding", output)

    def test_question_and_takeaway_types_are_checked_and_questions_capped(self):
        questions = ["Question one?", "Question one?", {"bad": "object"}, "x" * 351,
                     "Question two?", "Question three?", "Question four?"]
        output = self.render(meetings=[meeting(1, preReads=[{"n": 1, "name": "Read.txt", "text": "Read"}])],
                             notes=[note(0, questions=questions, preReads=[None, {"n": 1, "takeaway": []},
                                                                          {"n": True, "takeaway": "Bad boolean id"}])])
        self.assertEqual(1, output.count("Question one?"))
        self.assertIn("Question three?", output)
        self.assertNotIn("Question four?", output)
        self.assertNotIn("Bad boolean id", output)
        self.assertNotIn("object", output)
        self.assertNotIn("x" * 351, output)

    def test_empty_summary_retains_valid_takeaways_and_questions(self):
        output = self.render(meetings=[meeting(1, preReads=[{"n": 1, "name": "Read.txt", "text": "Source text"}])],
                             notes=[note(0, summary="", questions=["Who owns the next step?"],
                                         preReads=[{"n": 1, "takeaway": "A distinct finding from the file."}])])
        self.assertIn("No additional context was identified.", output)
        self.assertNotIn("Brief unavailable.", output)
        self.assertIn("Who owns the next step?", output)
        self.assertIn("A distinct finding from the file\\.", output)

    def test_overlong_model_fields_are_rejected_without_truncating_other_fields(self):
        source = meeting(1, preReads=[{"n": 1, "name": "Read.txt", "text": "Source text"},
                                     {"n": 2, "name": "Other.txt", "text": "Other text"}])
        output = self.render(meetings=[source], notes=[note(0, summary="word " * 61,
                             questions=["ask " * 41, "A valid question?"], preReads=[
                                 {"n": 1, "takeaway": "takeaway " * 36},
                                 {"n": 2, "takeaway": "A valid finding."},
                             ])])
        self.assertIn("Brief unavailable.", output)
        self.assertNotIn("word ", output)
        self.assertNotIn("ask ", output)
        self.assertNotIn("takeaway ", output)
        self.assertIn("Read\\.txt", output)
        self.assertIn("A valid question?", output)
        self.assertIn("A valid finding\\.", output)

    def test_priority_cap_does_not_drop_actions_or_create_new_ones(self):
        actions = [action(n) for n in range(1, 11)]
        actions[-1].update(group="today", dueDate="2026-10-20", dueLabel="Overdue — due 10/20")
        output = self.render(replies=[reply(1, group="soon", dueDate="2026-10-24", dueLabel="Due 10/24")],
                             actions=actions, meetings=[meeting(1)])
        priorities = self.section(output, "Suggested priorities")
        self.assertEqual(5, sum(line.startswith("- ") for line in priorities.splitlines()))
        self.assertIn("Action 10 — Overdue — due 10/20", priorities)
        self.assertIn("Reply 1 — Due 10/24", priorities)
        self.assertNotIn("Meeting 1", priorities)
        details = self.section(output, "Open action items")
        self.assertEqual(10, sum(line.startswith("- ") for line in details.splitlines()))
        for number in range(1, 11):
            self.assertEqual(1, details.count("- Action " + str(number) + " —"))

    def test_source_and_model_markup_is_text_and_urls_keep_their_exact_encoding(self):
        url = "https://outlook.example/a(b)%2Fpart?query=a+b&progress=50%25"
        title = "[Click](javascript:alert(1))\n## forged <img src=x onerror=alert(1)> &copy;"
        output = self.render(replies=[reply(1, title=title, webLink=url)],
                             actions=[action(1, title="**Fake action**", webLink="javascript:alert(1)"),
                                      action(2, webLink="https://safe.example/\n#bad"),
                                      action(3, webLink='https://safe.example/\" onclick=bad'),
                                      action(4, webLink="https://safe.example:bad/path")],
                             meetings=[meeting(1)], notes=[note(0, summary="<script>bad()</script>\n## forged",
                                                              questions=["[unsafe](data:text/html,bad)"])])
        self.assertIn("](" + url.replace("(", "\\(").replace(")", "\\)") + ")", output)
        self.assertIn("&lt;img", output)
        self.assertIn("&amp;copy;", output)
        self.assertIn("&lt;script&gt;", output)
        self.assertIn("\\[Click\\]\\(javascript:alert\\(1\\)\\)", output)
        self.assertNotIn("<img", output)
        self.assertNotIn("<script", output)
        self.assertNotIn("\n## forged", output)
        self.assertNotIn("](javascript:", output)
        self.assertNotIn("https://safe.example", output)
        self.assertNotIn("[unsafe](data:", output)


if __name__ == "__main__":
    unittest.main()
