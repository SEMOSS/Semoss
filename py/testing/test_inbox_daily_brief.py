"""Execute shipped inbox gather and draft-saving sources with mailbox calls stubbed out."""

import datetime
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, call, patch
from urllib.parse import unquote_plus


DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/inbox-daily-brief.automation.json"
UTC = datetime.timezone.utc


def item(thread_id, received, ask_type="reply", priority="P2"):
    return {"threadId": thread_id, "received": received, "askType": ask_type, "priority": priority}


class InboxDailyBriefTests(unittest.TestCase):
    def setUp(self):
        definition = json.loads(DEFINITION.read_text())
        self.definition = definition
        semoss = types.ModuleType("semoss")
        semoss.Insight = Mock()
        smssutil = types.ModuleType("smssutil")
        smssutil.smss_get_runtime_var = Mock()
        self.gather = {}
        self.save_drafts = {}
        with patch.dict(sys.modules, {"semoss": semoss, "smssutil": smssutil}):
            exec(compile(definition["nodeSources"]["gather"], str(DEFINITION), "exec"), self.gather)
            exec(compile(definition["nodeSources"]["save_drafts"], str(DEFINITION), "exec"), self.save_drafts)
        self.gather.update({
            "_optional": Mock(return_value={}),
            "_meetings": Mock(return_value=[]),
            "_action_items": Mock(return_value=[]),
        })
        self.save_drafts["_pixel"] = Mock()

    def queue(self, items, messages=None):
        def read(reactor, **args):
            self.assertEqual("WorkListItems", reactor)
            self.assertEqual("needs_me", args["view"])
            self.assertEqual("email", args["channel"])
            if args.get("sort") == "received":
                ordered = sorted(items, key=lambda row: self.gather["_at"](row["received"]), reverse=True)
            else:
                ordered = sorted(items, key=lambda row: row["priority"])
            offset = args.get("offset", 0)
            return {"items": ordered[offset:offset + args["limit"]], "total": len(ordered)}

        def read_messages(thread_id, limit):
            self.assertEqual(6, limit)
            if messages is not None and thread_id in messages:
                return messages[thread_id]
            return [{"id": "message-" + thread_id, "fromAddress": "sender@example.com", "text": "Please reply."}]

        self.gather["_pixel"] = Mock(side_effect=read)
        self.gather["_messages"] = Mock(side_effect=read_messages)

    def run_at(self, now, **scope):
        class FrozenDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        self.gather.update({
            "datetime": types.SimpleNamespace(datetime=FrozenDateTime, timedelta=datetime.timedelta, timezone=datetime.timezone),
        })
        return self.gather["run"]({"owner_email": "owner@example.com", **scope})

    def test_cutoff_is_inclusive_and_compares_instants(self):
        self.queue([
            item("new", "2026-10-20T12:01:00Z"),
            item("boundary", "2026-10-20T08:00:00-04:00"),
            item("old", "2026-10-20T11:59:59.999999Z"),
        ])
        replies = self.gather["_replies"]({"owner@example.com"}, datetime.datetime(2026, 10, 20, 12, tzinfo=UTC), {})

        self.assertEqual(["new", "boundary"], [reply["threadId"] for reply in replies])
        self.assertEqual(2, self.gather["_messages"].call_count)

    def test_paginates_all_recent_mail_without_old_priority_or_count_limits(self):
        self.queue(
            [item("old-" + str(n), "2026-10-01T10:00:00Z", priority="P0") for n in range(110)]
            + [item("recent-" + str(n), "2026-10-20T10:00:00Z") for n in range(125)]
        )
        replies = self.gather["_replies"]({"owner@example.com"}, datetime.datetime(2026, 10, 19, tzinfo=UTC), {})

        self.assertEqual(125, len(replies))
        self.assertEqual(list(range(1, 126)), [reply["n"] for reply in replies])
        self.assertTrue(all(reply["threadId"].startswith("recent-") for reply in replies))
        self.assertEqual([0, 100], [call.kwargs["offset"] for call in self.gather["_pixel"].call_args_list])
        self.assertEqual(125, self.gather["_messages"].call_count)

    def test_preserves_ask_filters_and_skips_threads_the_owner_answered(self):
        self.queue(
            [item("reply", "2026-10-20T10:00:00Z"),
             item("approve", "2026-10-20T10:00:00Z", "approve"),
             item("answered", "2026-10-20T10:00:00Z", "review"),
             item("attend", "2026-10-20T10:00:00Z", "attend"),
             item("errand", "2026-10-20T10:00:00Z", "errand"),
             item("empty", "2026-10-20T10:00:00Z")],
            {"answered": [{"fromAddress": "sender@example.com"}, {"fromAddress": "OWNER@example.com"}], "empty": []},
        )
        replies = self.gather["_replies"]({"owner@example.com"}, datetime.datetime(2026, 10, 19, tzinfo=UTC), {})

        self.assertEqual(["reply", "approve"], [reply["threadId"] for reply in replies])
        self.assertEqual(4, self.gather["_messages"].call_count)

    def test_longer_window_is_not_shortened_by_previous_run_or_four_day_limit(self):
        self.queue([item("five-days-old", "2026-10-15T12:00:00Z"), item("too-old", "2026-10-13T11:59:59Z")])
        context = self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC),
                              lookback_hours=168, since="2026-10-20T11:00:00Z")

        self.assertEqual(["five-days-old"], [reply["threadId"] for reply in context["replies"]])

    def test_defaults_to_24_hours_and_ignores_legacy_email_count(self):
        self.queue([item("one", "2026-10-20T11:00:00Z"), item("two", "2026-10-19T12:00:00Z"),
                    item("old", "2026-10-19T11:59:59Z")])
        context = self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC), email_count=1)

        self.assertEqual(["one", "two"], [reply["threadId"] for reply in context["replies"]])
        prompt = json.loads(context["draftsInput"])
        self.assertEqual(2, len(prompt["replies"]))
        self.assertTrue(all("threadId" not in reply and "messageId" not in reply for reply in prompt["replies"]))

    def test_draft_and_brief_inputs_preserve_text_and_links_after_url_decoding(self):
        link = "https://outlook.example/mail/id/item%2Fpart?query=a+b&progress=50%25"
        latest = "Please approve a 10% increase + delivery on Friday."
        self.queue(
            [{**item("estimate", "2026-10-20T10:00:00Z", "approve", "P1"),
              "title": "Approve the estimate", "reasons": ["Due tomorrow"], "due": "2026-10-21"}],
            {"estimate": [
                {"id": "earlier-id", "fromAddress": "dana@example.com", "text": "Earlier discussion."},
                {"id": "latest-id", "fromAddress": "dana@example.com", "fromName": "Dana", "subject": "Estimate",
                 "text": latest, "webLink": link},
            ]},
        )
        self.gather["_optional"].side_effect = lambda reactor, **args: (
            {"items": [{"id": "estimate", "summary": "Estimate is ready for approval."}]}
            if reactor == "BrainListThreads" else {}
        )
        meetings = [{"subject": "Planning", "agenda": "Discuss next steps.", "webLink": link}]
        actions = [{"title": "Confirm the delivery date", "due": "2026-10-21"}]
        self.gather["_meetings"].return_value = meetings
        self.gather["_action_items"].return_value = actions

        context = self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC), timezone="America/New_York")
        drafting = json.loads(unquote_plus(context["draftsInput"]))
        self.assertEqual({"owner", "replies"}, set(drafting))
        self.assertEqual(context["owner"], drafting["owner"])
        self.assertEqual(["Earlier discussion.", latest], [message["text"] for message in drafting["replies"][0]["messages"]])
        for field in ("threadId", "messageId", "webLink"):
            self.assertNotIn(field, drafting["replies"][0])
        self.assertNotIn("briefInput", context)

        saved = self.save_drafts["run"]({"context": context, "drafts": '{"drafts": []}'})
        prompt = json.loads(unquote_plus(saved["briefInput"]))

        self.assertEqual({"today", "timezone", "replies", "meetings", "actionItems"}, set(prompt))
        self.assertEqual("2026-10-20", prompt["today"])
        self.assertEqual("America/New_York", prompt["timezone"])
        self.assertEqual([{
            "n": 1, "title": "Approve the estimate", "priority": "P1", "askType": "approve",
            "reasons": ["Due tomorrow"], "due": "2026-10-21", "from": "Dana", "received": "2026-10-20T10:00:00Z",
            "subject": "Estimate", "summary": "Estimate is ready for approval.", "webLink": link,
            "latestMessage": latest, "draft": "not written",
        }], prompt["replies"])
        self.assertEqual(meetings, prompt["meetings"])
        self.assertEqual(actions, prompt["actionItems"])
        self.assertEqual(saved["replies"], prompt["replies"])
        self.save_drafts["_pixel"].assert_not_called()
        self.assertEqual("estimate", context["replies"][0]["threadId"])
        self.assertEqual("latest-id", context["replies"][0]["messageId"])
        self.assertEqual(link, context["replies"][0]["webLink"])
        self.assertEqual(["Earlier discussion.", latest], [message["text"] for message in context["replies"][0]["messages"]])

    def test_uses_both_owner_addresses_and_supplies_confirmed_style_and_sent_mail_for_drafting(self):
        self.queue(
            [item("scope-owner", "2026-10-20T10:00:00Z"), item("profile-owner", "2026-10-20T10:00:00Z"),
             item("needs-reply", "2026-10-20T10:00:00Z")],
            {"scope-owner": [{"fromAddress": "OWNER@example.com"}],
             "profile-owner": [{"fromAddress": "PROFILE@example.com"}]},
        )

        def optional(reactor, **args):
            if reactor == "BrainGetProfile":
                return {"name": "Owner", "email": "profile@example.com",
                        "role": {"source": "you", "value": "Manager"},
                        "style": {"confirmed": True, "summary": "Concise and friendly", "examples": ["Thanks, Dana."]}}
            if reactor == "MicrosoftOutlookListMail":
                return {"messages": [{"subject": "Prior reply", "body": "Thanks + agreed 100%."}]}
            self.assertEqual("BrainListThreads", reactor)
            return {}

        self.gather["_optional"].side_effect = optional
        context = self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC))

        self.assertEqual(["needs-reply"], [reply["threadId"] for reply in context["replies"]])
        self.assertEqual([
            call("BrainGetProfile"),
            call("MicrosoftOutlookListMail", folder="sentitems", limit=5, includeBody=True, maxBodyChars=1500),
            call("BrainListThreads", limit=100, offset=0, detail=True),
        ], self.gather["_optional"].call_args_list)
        self.assertEqual([("WorkListItems",)], [call.args for call in self.gather["_pixel"].call_args_list])
        self.assertEqual({
            "name": "Owner", "email": "profile@example.com", "role": "Manager",
            "writingStyle": "Concise and friendly", "styleExamples": ["Thanks, Dana."],
            "recentSentEmails": [{"subject": "Prior reply", "text": "Thanks + agreed 100%."}],
        }, json.loads(unquote_plus(context["draftsInput"]))["owner"])

    def test_shipped_workflow_drafts_and_saves_before_writing_the_brief(self):
        agents = [node for node in self.definition["graph"]["nodes"] if node["type"] == "agent.run"]
        self.assertEqual(["drafts", "brief"], [node["id"] for node in agents])
        self.assertIn("${context.draftsInput}", agents[0]["config"]["command"])
        self.assertIn("Needs your reply", agents[1]["config"]["command"])
        self.assertIn("${saved.briefInput}", agents[1]["config"]["command"])
        edges = {(edge["source"], edge["target"]) for edge in self.definition["graph"]["edges"]}
        self.assertTrue({("gather", "drafts"), ("drafts", "save_drafts"), ("save_drafts", "brief")}.issubset(edges))

    def test_saves_model_bodies_by_reply_number_as_reply_all_drafts_in_the_teamwork_folder(self):
        self.queue([item("first", "2026-10-20T10:00:00Z"), item("second", "2026-10-20T10:00:00Z"),
                    item("unwritten", "2026-10-20T10:00:00Z")])
        context = self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC))
        self.save_drafts["_pixel"].side_effect = [
            {"id": "teamwork-folder"}, {"id": "draft-first"}, {}, {"id": "draft-second"}, {},
        ]
        answer = json.dumps({"drafts": [
            {"n": 2, "body": "Second <ready> & approved\nThank you."},
            {"n": 1, "body": "First answer"},
            {"n": 999, "body": "Unmatched answer"},
        ]})

        saved = self.save_drafts["run"]({"context": context, "drafts": answer})

        self.assertEqual([
            call("MicrosoftOutlookCreateMailFolder", name="Teamwork drafts", parent="drafts"),
            call("MicrosoftOutlookReplyMail", id="message-first", body="First answer", html=True, replyAll=True, asDraft=True),
            call("MicrosoftOutlookMoveMail", id="draft-first", folder="teamwork-folder"),
            call("MicrosoftOutlookReplyMail", id="message-second", body="Second &lt;ready&gt; &amp; approved<br>Thank you.",
                 html=True, replyAll=True, asDraft=True),
            call("MicrosoftOutlookMoveMail", id="draft-second", folder="teamwork-folder"),
        ], self.save_drafts["_pixel"].call_args_list)
        self.assertEqual(["saved", "saved", "not written"], [reply["draft"] for reply in saved["replies"]])
        self.assertEqual(["Drafts > Teamwork drafts", "Drafts > Teamwork drafts"],
                         [reply["location"] for reply in saved["replies"][:2]])
        self.assertEqual(saved["replies"], json.loads(unquote_plus(saved["briefInput"]))["replies"])
        self.assertTrue(all("draft" not in reply for reply in context["replies"]))

    def test_window_uses_elapsed_hours_across_daylight_saving_change(self):
        self.queue([item("boundary", "2026-10-31T07:30:00Z"), item("old", "2026-10-31T07:29:59Z")])
        context = self.run_at(datetime.datetime(2026, 11, 1, 7, 30, tzinfo=UTC),
                              timezone="America/New_York", lookback_hours=24)

        self.assertEqual(["boundary"], [reply["threadId"] for reply in context["replies"]])
        self.assertEqual("2026-11-01", context["today"])

    def test_calendar_keeps_owners_local_date(self):
        self.queue([])
        context = self.run_at(datetime.datetime(2026, 10, 20, 2, tzinfo=UTC), timezone="America/New_York")

        self.assertEqual("2026-10-19", context["today"])
        self.assertEqual(datetime.date(2026, 10, 19), self.gather["_meetings"].call_args.args[0])

    def test_rejects_invalid_lookback_before_reading_mail(self):
        self.queue([])
        for value in (0, -1, 721, 1.5, True, "24", None, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "whole number of hours"):
                self.run_at(datetime.datetime(2026, 10, 20, 12, tzinfo=UTC), lookback_hours=value)
        self.gather["_pixel"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
