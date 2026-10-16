#!/usr/bin/env python3
"""Create an isolated Review mailbox copy and optional Pixel setup; never changes the input mailbox."""
import argparse
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso(value):
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def shifted(value, delta):
    if isinstance(value, list):
        return [shifted(item, delta) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if isinstance(item, str) and key.endswith("DateTime"):
                try:
                    item = iso(timestamp(item) + delta)
                except ValueError:
                    pass
            else:
                item = shifted(item, delta)
            result[key] = item
        return result
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-mail", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date())
    args = parser.parse_args()
    source = args.base_mail.resolve()
    output = args.output.resolve()
    if output / "mail.json" == source:
        raise SystemExit("Choose a separate output directory; the original mailbox is preserved.")
    original = source.read_bytes()
    mail = json.loads(original)
    if not isinstance(mail, list) or not mail:
        raise SystemExit("The base mailbox must be a nonempty JSON list.")
    now = dt.datetime.combine(args.as_of, dt.time(12), dt.timezone.utc)
    latest = max(timestamp(row["receivedDateTime"]) for row in mail)
    delta = now - latest
    mail = shifted(mail, delta)
    owner = {"emailAddress": {"name": "Alex Morgan", "address": "alex.morgan@search.example"}}
    added = []
    cases = []

    def add(case, subject, sender, minutes, days=0, conversation=None, sent=False):
        row = copy.deepcopy(mail[0])
        key = f"review-{case}-{minutes}"
        person = {"emailAddress": {"name": sender[0], "address": sender[1]}}
        at = now - dt.timedelta(days=days, minutes=minutes)
        row.update({"id": key, "internetMessageId": f"<{key}@review.example>", "conversationId": conversation or key, "parentFolderId": "review-fixture-sentitems" if sent else "review-fixture-inbox", "from": owner if sent else person, "toRecipients": [person] if sent else [owner], "ccRecipients": [], "subject": subject, "receivedDateTime": iso(at), "sentDateTime": iso(at), "isRead": False, "importance": "normal", "phase": "review-fixture", "hasAttachments": False})
        body = {"contentType": "text", "content": f"{subject}. Please review the plan for this work and send your comments. This is a local Review test conversation."}
        row["body"] = body
        row["uniqueBody"] = body
        for field in ["attachments", "internetMessageHeaders", "isDraft"]:
            row.pop(field, None)
        added.append(row)
        cases.append({"case": case, "subject": subject, "conversationId": row["conversationId"], "at": iso(at)})

    groups = [ ("Cedar Relay", ("Avery Brooks", "avery@cedar.review.example")), ("Maple Observatory", ("Jordan Reed", "jordan@maple.review.example")), ("Saffron Logistics", ("Taylor Shaw", "taylor@saffron.review.example")) ]
    minute = 20
    for group, person in groups:
        for number in range(1, 4):
            add("unfiled", f"ReviewCase Unfiled: {group} planning {number:02d}", person, minute)
            minute += 1
    for label, person in [("Unknown", ("Riley Park", "riley@northwind.review.example")), ("Membership", ("Casey Ng", "casey@northwind.review.example"))]:
        for number in range(1, 4):
            add("people", f"ReviewCase {label}: Northwind migration delivery {number:02d}", person, minute)
            minute += 1
    for number in range(2):
        add("quiet", f"ReviewCase Legacy Audit: historical handover {number+1}", ("Morgan Vale", "morgan@audit.review.example"), minute+number, days=45, conversation="review-quiet-audit")
    add("identity", "ReviewCase: fixture owner identity", ("Riley Park", "riley@northwind.review.example"), -1, sent=True)
    output.mkdir(parents=True, exist_ok=True)
    (output / "mail.json").write_text(json.dumps(mail + added, indent=2, ensure_ascii=False) + "\n")
    teams = source.with_name("teams.json")
    if teams.exists():
        (output / "teams.json").write_text(json.dumps(shifted(json.loads(teams.read_text()), delta), indent=2, ensure_ascii=False) + "\n")
    topics = [
        {"name": "ReviewCase Northwind Migration", "description": "Northwind migration delivery, handover and deployment planning.", "kind": "client", "status": "active"},
        {"name": "ReviewCase Northwind Delivery", "description": "The same Northwind migration delivery and handover work. Review whether these topics should be combined.", "kind": "client", "status": "active"},
        {"name": "ReviewCase Legacy Audit", "description": "Historical audit handover; link the old ReviewCase Legacy Audit conversation here for the quiet-topic scenario.", "kind": "internal", "status": "active"},
    ]
    statements = ["// Local test account only. Run once after import; these commands create saved topics and two synthetic memories."]
    statements += ["BrainSaveTopic(topic=[" + json.dumps(topic) + "]);" for topic in topics]
    for name, state, origin, text in [ ("suggested", "suggested", "brain", "ReviewCase: keep project status updates short."), ("learned", "active", "assistant", "ReviewCase: Alex prefers the weekly summary on Friday.") ]:
        # Existing restore semantics can insert a known local fixture identity with its saved review state.
        memory = {"id": f"review-fixture-{name}-{args.as_of:%Y%m%d}", "kind": "preference", "text": text, "state": state, "origin": origin, "confirmed": False, "about": []}
        statements.append("BrainSaveMemory(memory=[" + json.dumps(memory) + "]);")
    (output / "setup.pixel").write_text("\n".join(statements) + "\n")
    (output / "import-90-days.pixel").write_text("// Optional: include the 45-day-old conversation needed for the quiet-topic scenario.\nBrainImportMail(days=[90]);\n")
    manifest = {"asOf": str(args.as_of), "baseMail": str(source), "baseSha256": hashlib.sha256(original).hexdigest(), "originalMessages": len(mail), "addedMessages": len(added), "totalMessages": len(mail) + len(added), "addedThreads": len({row["conversationId"] for row in added}), "unfiledTargetThreads": 9, "cases": cases, "notes": ["Mail headers alone do not force model classifications or create saved topic/memory review state.", "Exact nine-unfiled counts are covered deterministically in BrainReviewWorkflowTest; UI totals include the existing mailbox and depend on its saved classifications.", "setup.pixel is optional and uses existing owner-scoped APIs. Run once in a local test account.", "Quiet-topic testing requires importing 90 days and linking the old conversation to the Legacy Audit topic."]}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    assert source.read_bytes() == original, "The base mailbox was changed"
    print(json.dumps({"output": str(output), "totalMessages": len(mail) + len(added), "addedMessages": len(added), "addedThreads": manifest["addedThreads"]}))


if __name__ == "__main__":
    main()
