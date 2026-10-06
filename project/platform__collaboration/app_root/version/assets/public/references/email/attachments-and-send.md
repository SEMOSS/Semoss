# Attach files and handle sending

## Attach a verified room file

Create or locate the requested file, check its content, and use its room-relative
path. A file merely named in chat is not attached. To add files to the open email,
call `ComposeEmail` with `openEmailId` and an `attachments` array:

```json
{"openEmailId":"<openEmail.id from context>","attachments":["summary.docx","analysis.xlsx"]}
```

Omit `message`, recipients, and subject when they do not change. Existing attachments
stay. For a new email, include the usual draft fields and the same `attachments`
array. Never pass absolute paths, raw bytes, base64, or a file on another machine.

The current call limit is 10 files and 2.5 MB total. Check sizes before attempting
the handoff. When over the limit, explain the constraint and offer a smaller output
or an authorized sharing route. Do not silently omit files or report a failed
handoff as success.

The result contains attachment descriptors with `path`, `name`, `size`, and `sha256`
for internal snapshots. The editor loads those snapshots. Do not edit their internal
paths or hashes, read their bytes into model history, or substitute them for the
owner's source files. Changes to an original file after handoff do not update the
already attached snapshot. Reattach the intended revised file when requested and
check for duplicates in the supplied open-email state.

After a successful result, state that the files were added to the email editor.
If the editor reports an attachment loading failure, address it before requesting
send. Do not claim that the mailbox draft was saved or delivered.

## Request sending through the owner flow

Use `SendEmail` only when the owner asks to send. For an open email, pass only
`openEmailId`. If no email is open, compose it first, then use the current editor ID
when available. Never guess an ID or manufacture a mailbox `draftId`.

```json
{"openEmailId":"<openEmail.id from context>"}
```

`SendEmail` waits for the owner. Pressing Send in the editor saves the current draft
and approves its actual content. Until a result proves success, report the pending
state. A request to send, an approval card, or `ComposeEmail.shown` is not proof of
delivery. Only report sent after a successful send result.

## Rejection, cancellation, and failure

If the owner rejects or cancels Send, stop. Do not issue another SendEmail, call a
provider's send tool, or use Python/Pixel to bypass the decision. Continue with a
requested revision, or ask briefly what they want changed when that is missing.
A new send attempt needs a new explicit request from the owner.

On a timeout or ambiguous provider response, report that delivery is unconfirmed.
Use an available read/status tool if it can resolve the outcome; do not blindly
resend and risk duplicate mail. On a clear failure, report the cause and preserve
the editable draft for correction. Keep saving, attaching, approval, and sending
as distinct states in the final answer.
