"""Verify the expense review saves its draft replies as native Outlook replies to the requests, then emails the memo."""
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

# imported before patch.dict(sys.modules) so it is kept: a re-import mid-run makes a second Extension class
import markdown.extensions.tables  # noqa: F401

DEFINITION = Path(__file__).resolve().parents[2] / "src/prerna/collaboration/expense-review.automation.json"

SUBJECT = "Meeting Approval - Business Approval(Requested): S&T Ignite Onboarding Program (102241)"
REVIEW = """## S&T Ignite Onboarding Program (102241)

Suggested decision: Return for more information. It may repeat the official onboarding (CS TEM Guidance, slide 2).

### Before you respond

- Ask what sets it apart from the official onboarding program (CS TEM Guidance, slide 2).

### Draft reply

To: %s

> Hi Deanette,
>
> Would you mind sharing more details on this? What differentiates it from the official onboarding program?
>
> Thanks,
> Lauren

### Request

| Field | Value |
| --- | --- |
| Forecast (All Expenses) | $8,545 |
"""


def _returns(output, error=False):
    return [{"pixelReturn": [{"output": output, "operationType": ["ERROR" if error else "MAP"]}]}]


def _node(name, run_pixel):
    source = json.loads(DEFINITION.read_text())["nodeSources"][name]
    semoss = types.ModuleType("semoss")
    semoss.Insight = Mock(return_value=types.SimpleNamespace(run_pixel=run_pixel))
    scope = {}
    with patch.dict(sys.modules, {"semoss": semoss}):
        exec(compile(source, str(DEFINITION), "exec"), scope)
    return scope["run"]


class ExpenseReviewDeliveryTests(unittest.TestCase):
    def _deliver(self, review, reply=None, cc=None):
        pixels = []

        def run_pixel(pixel, raw=True):
            pixels.append(pixel)
            if pixel.startswith("MicrosoftOutlookReplyMail("):
                return reply or _returns({"draft": True, "webLink": "https://outlook.office.com/draft-1"})
            return _returns({})

        context = {"today": "2026-10-27", "requests": [{"n": 1, "messageId": "graph-1", "subject": SUBJECT,
                                                         "webLink": "https://outlook.office.com/request-1"}]}
        result = _node("deliver", run_pixel)({"owner_email": "lgaines@deloitte.com", "context": context,
                                              "review": review, "cc": cc or []})
        return result, pixels

    def test_draft_is_saved_as_a_reply_to_the_request_before_the_memo_is_sent(self):
        result, pixels = self._deliver(REVIEW % "Deanette Myers, DEMyers@deloitte.com")
        reply, send = pixels
        self.assertTrue(reply.startswith('MicrosoftOutlookReplyMail(id=["graph-1"], body=["Hi Deanette,\\u003cbr>'
                                         '\\u003cbr>Would you mind sharing more details on this?'))
        self.assertIn("Thanks,\\u003cbr>Lauren", reply)
        for arg in ('html=[true]', 'overrideRecipients=[true]', 'asDraft=[true]', 'to=[["demyers@deloitte.com"]]'):
            self.assertIn(arg, reply)
        self.assertNotIn("To:", reply)
        self.assertTrue(send.startswith('MicrosoftOutlookSendMail(to=[["lgaines@deloitte.com"]]'))
        self.assertIn("Saved in your Outlook drafts as a reply to the request.", result["html"])
        self.assertIn('href="https://outlook.office.com/draft-1"', result["html"])
        self.assertIn('href="https://outlook.office.com/request-1"', result["html"])
        self.assertEqual([{"subject": SUBJECT, "to": "demyers@deloitte.com", "saved": True}], result["drafts"])

    def test_an_address_outside_the_organization_is_left_off(self):
        result, pixels = self._deliver(REVIEW % "Mallory, mallory@deloitte.com.example.org")
        self.assertNotIn("to=", pixels[0])
        self.assertIn("with no recipient: add one before you send it.", result["html"])
        self.assertEqual("", result["drafts"][0]["to"])

    def test_a_draft_that_cannot_be_saved_still_reaches_the_owner_in_the_memo(self):
        result, pixels = self._deliver(REVIEW % "Deanette Myers, demyers@deloitte.com",
                                       reply=_returns("Graph said no", error=True))
        self.assertTrue(pixels[1].startswith("MicrosoftOutlookSendMail("))
        self.assertIn("could not be saved in your Outlook drafts; copy it from here.", result["html"])
        self.assertIn("What differentiates it from the official onboarding program?", result["html"])
        self.assertFalse(result["drafts"][0]["saved"])
        self.assertIn("Graph said no", result["drafts"][0]["error"])

    def test_an_approval_saves_no_draft(self):
        result, pixels = self._deliver("## S&T Ignite Onboarding Program (102241)\n\nSuggested decision: Approve.\n")
        self.assertEqual(1, len(pixels))
        self.assertNotIn("cc=", pixels[0])
        self.assertEqual([], result["drafts"])

    def test_thinking_and_a_lead_in_never_reach_the_email(self):
        # a model without a reasoning parser writes its thinking, a draft review and </think> before the answer
        review = REVIEW % "Deanette Myers, demyers@deloitte.com"
        result, pixels = self._deliver("Let me analyze the request.\n\n" + review + "\n</think>\n\n"
                                       + "Key findings from the policy documents:\n\n" + review)
        self.assertEqual(1, sum(pixel.startswith("MicrosoftOutlookReplyMail(") for pixel in pixels))
        self.assertNotIn("Let me analyze", result["html"])
        self.assertNotIn("Key findings", result["html"])
        self.assertEqual(1, result["html"].count("Suggested decision:"))

    def test_a_repeated_review_saves_one_draft_per_request(self):
        review = REVIEW % "Deanette Myers, demyers@deloitte.com"
        result, pixels = self._deliver(review + "\n" + review)
        self.assertEqual(1, sum(pixel.startswith("MicrosoftOutlookReplyMail(") for pixel in pixels))
        self.assertIn("A draft for this request is already in your Outlook drafts.", result["html"])
        self.assertEqual([True, False], [draft["saved"] for draft in result["drafts"]])

    def test_the_review_is_copied_to_the_owners_recipients_but_not_to_the_owner_twice(self):
        result, pixels = self._deliver("## S&T Ignite Onboarding Program (102241)\n\nSuggested decision: Approve.\n",
                                       cc=["jbell@deloitte.com", "LGaines@deloitte.com"])
        self.assertTrue(pixels[0].startswith('MicrosoftOutlookSendMail(to=[["lgaines@deloitte.com"]], '
                                             'cc=[["jbell@deloitte.com"]], subject='))
        self.assertEqual(["jbell@deloitte.com"], result["cc"])

    def test_gather_keeps_the_graph_id_for_deliver_but_not_for_the_model(self):
        def run_pixel(pixel, raw=True):
            return _returns({"messages": [{"id": "graph-1", "emailRef": "email:k1", "subject": SUBJECT,
                                           "fromName": "Wandner-Benson, Rachel", "text": "Please provide approval"}]})

        context = _node("gather", run_pixel)({"thread_ids": ["t1"], "message_keys": ["k1"], "timezone": "UTC"})
        self.assertEqual("graph-1", context["requests"][0]["messageId"])
        self.assertNotIn("graph-1", context["reviewInput"])
        self.assertIn("Please provide approval", context["reviewInput"])


if __name__ == "__main__":
    unittest.main()
