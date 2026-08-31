# Sending mail (SMTP)

A relay that is not Microsoft 365 only speaks SMTP, so this engine defaults to
`MAIL_TRANSPORT jakarta`. For a Microsoft 365 mailbox use `EXCHANGE_SMTP`, which
is the same engine defaulting to Graph.

```
FUNCTION_TYPE              SMTP
SMTP_HOST                  semoss-mail    (SEMOSS in Docker; use localhost if SEMOSS runs on host)
SMTP_PORT                  3025
SMTP_SECURITY              none           (starttls / ssl / none - see the TLS note below)
SMTP_USERNAME              semoss@semoss.local
SMTP_PASSWORD              semoss
SMTP_SENDER                semoss@semoss.local
SMTP_SENDER_NAME           <optional; display name on the from header>
ALLOW_SENDER_OVERRIDE      <optional; true to let a call send as another address, defaults to false>
ALLOWED_RECIPIENT_DOMAINS  <optional; e.g. semoss.local - blank allows any recipient>
DEFAULT_TO                 <optional; recipients used when a call passes none>
DEFAULT_CC                 <optional>
DEFAULT_BCC                <optional>
SUBJECT_PREFIX             <optional; prepended to every subject>
HTML                       <optional; default for the html parameter, defaults to false>
MAX_RECIPIENTS             <optional; cap per email, defaults to 25>
ALLOW_ATTACHMENTS          <optional; true to allow attachments, defaults to false>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

Sending cannot be undone, so the guardrails sit in the SMSS rather than with the
caller: the sender is pinned to `SMTP_SENDER` unless `ALLOW_SENDER_OVERRIDE` is
on, recipients outside `ALLOWED_RECIPIENT_DOMAINS` are rejected, the recipient
count is capped, and attachments are refused until `ALLOW_ATTACHMENTS` is set.
Attachments also have to already be files of the insight making the call.

Read what arrived at http://localhost:8085 (the GreenMail UI), or with
`curl "http://localhost:8085/api/user/reports@semoss.local/messages/"`.

---

Part of the [function engines](README.md) of the
[supporting engines](../README.md).
