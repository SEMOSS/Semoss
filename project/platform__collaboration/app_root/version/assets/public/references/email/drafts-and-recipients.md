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
| Reply | `replyTo: "selected"` for the selected email, or its `emailRef` from `ReadThread`; `message` is the new reply |
| Forward | `forward: "selected"` for the selected email, or its `emailRef` from `ReadThread`; set `to`; `message` is the short note |
| Edit the open email | `openEmailId` = `openEmail.id`; pass only changed fields |

For a reply, leave `to` and `cc` out unless the owner requested a recipient change;
the provider keeps native reply behavior. For a forward, the provider includes the
original email and its attachments. Do not paste the original into `message`.
Do not pass both `replyTo` and `forward`.

Topic and home chats can reply to or forward accessible Brain emails without
adding the thread to the chat. Use `SearchMail` to find the thread and `ReadThread`
to get its messages, then pass the chosen email's `emailRef` (`email:...`) to
`ComposeEmail`. SEMOSS resolves that stable reference to the exact Outlook ID.
Native email IDs from context remain supported, but prefer `emailRef` over copying
a long Outlook ID. The editor keeps the original email thread; no selected email
is required. This reference lookup is for the owner's assistant chats; delegation
rooms keep using `"selected"` or the exact native ID of an included email.

`"selected"` resolves to the current context's `selectedSourceMessageId`; do not copy
that long ID yourself. If the source is rejected, check the latest context or
`ReadThread` and retry once with its `emailRef` and the same draft text (in a
delegation room, use the exact included native ID instead). Never
drop `replyTo` or `forward` to bypass an error. Report a persistent failure; do not
retry authentication or access errors without the owner's help. An unresolved
email ID does not establish a permissions problem.

Completing a topic goal does not fulfill an explicit request to draft or reply.
If the latest email says the action already happened, briefly surface that fact
without silently abandoning the requested email. Clarify only when that fact
materially changes what the owner wants to send.

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

Report the returned `mode` accurately: `reply`, `forward`, `new`, or `edit`.
`replyTo` or `forward` in the result is the resolved source ID; `shown: true` means
the editor handoff succeeded. Saving to the mailbox is an owner action in the
editor; it is not implied.
