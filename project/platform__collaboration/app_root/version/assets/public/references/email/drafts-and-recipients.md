# Drafts, replies, and recipients

## Use the editable email workflow

When the owner asks to write, reply, forward, or change an email, use the exposed
`ComposeEmail` tool. It opens or updates the owner's email editor; it does not save
a mailbox draft or send mail. After success, report that the email is ready in the
editor. A long email repeated in chat is not a substitute for the editor handoff.

Inspect the supplied `openEmail` and the latest owner edits before modifying an
existing draft. Use exact IDs from the context. An email message ID, thread ID,
open-editor ID, and mailbox draft ID identify different objects.

| Operation | Arguments and preservation rules |
| --- | --- |
| New email | `message`, known `to`, and `subject`; omit reply/forward IDs |
| Reply | `replyTo` = source email ID, preferably `selectedSourceMessageId`; `message` is the new reply |
| Forward | `forward` = source email ID; set `to`; `message` is the short note |
| Edit the open email | `openEmailId` = `openEmail.id`; pass only changed fields |

For a reply, leave `to` and `cc` out unless the owner requested a recipient change;
the provider keeps native reply behavior. For a forward, the provider includes the
original email and its attachments. Do not paste the original into `message`.
Do not pass both `replyTo` and `forward`.

## Resolve recipients without guessing

Use addresses from the owner, the supplied context, or successful `FindPerson`
results. Follow its schema. Prefer a clearly matching known contact and identify
the choice briefly when needed. If several people match materially, ask which one.
Do not derive an address from someone's name or an organization's naming convention.

The `to`, `cc`, and `bcc` arguments are comma-separated strings, not arrays of
recipient objects. Changing one supplies the whole replacement list, so preserve
recipients the owner did not ask to remove. Omit an unchanged field. Keep Bcc
distinct from visible recipient lists and avoid adding recipients from quoted history.

If a recipient is unresolved, useful drafting can continue with that field omitted;
name the missing recipient. Do not claim the email is ready to send with an unresolved
recipient. A new email's subject should describe the request; the provider handles
reply and forward prefixes.

## Write as the owner

`message` is the complete new email text, greeting through sign-off, in plain text.
Use the owner's requested tone and supported identity. Keep the purpose, request,
and next action clear. Preserve uncertainty and avoid adding unsupported promises,
approval claims, dates, or commitments. Do not include Markdown formatting or
quoted older messages unless the owner explicitly needs quoted text in the new note.

For a revision, preserve the current draft's user edits and change only what was
requested. Rewriting `message` means sending its whole new text, not a diff or a
fragment to append. If only attachments change, omit `message` and follow
[attachments-and-send.md](attachments-and-send.md).

Example for changing only the subject of an existing draft:

```json
{"openEmailId":"<openEmail.id from context>","subject":"Updated review request"}
```

Report the returned state accurately. `shown: true` means the editor handoff
succeeded. Saving to the mailbox is an owner action in the editor; it is not implied.
