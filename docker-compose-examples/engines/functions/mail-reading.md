# Reading a mailbox (POP3 and IMAP)

Two engines read mail over the protocols. They share every guardrail below and
differ in what the protocol itself can do: POP3 has one inbox and no idea what
has been read, IMAP has folders, search and read state, and can be allowed to
change the mailbox.

## POP3

```
FUNCTION_TYPE              POP3
POP3_HOST                  semoss-mail    (host: localhost)
POP3_PORT                  3110
POP3_SECURITY              none
POP3_USERNAME              reports@semoss.local
POP3_PASSWORD              reports
MAX_MESSAGES               <optional; cap per call, defaults to 25>
DEFAULT_MESSAGES           <optional; returned when a call passes no limit, defaults to 10>
MAX_BODY_CHARS             <optional; longer bodies come back truncated, defaults to 10000>
ALLOWED_SENDER_DOMAINS     <optional; only mail from these domains is surfaced at all>
ALLOW_ATTACHMENT_DOWNLOAD  <optional; true to save attachments into the calling insight, defaults to false>
MAX_ATTACHMENT_SIZE        <optional; bytes, defaults to 5242880>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

POP3 has one inbox, no folders, no record of what has been read, and no search, so
the engine filters the mailbox itself: it walks back from the newest message and
stops once it has enough matches. A search that matches nothing therefore reads
the whole mailbox, so prefer a `sinceDays` on a big one. The engine never deletes
anything - a POP3 delete happens the moment the connection closes and cannot be
walked back.

## IMAP

```
FUNCTION_TYPE              IMAP
IMAP_HOST                  semoss-mail    (host: localhost)
IMAP_PORT                  3143
IMAP_SECURITY              none
IMAP_USERNAME              reports@semoss.local
IMAP_PASSWORD              reports
DEFAULT_FOLDER             <optional; folder a call reads when it names none, defaults to INBOX>
ALLOWED_FOLDERS            <optional; e.g. INBOX,Archive - blank allows any folder>
MARK_AS_READ               <optional; true to mark what the engine returns as read, defaults to false>
ALLOW_FLAG_CHANGES         <optional; true to allow markRead / markUnread, defaults to false>
ALLOW_MOVE                 <optional; true to allow move, defaults to false>
ALLOW_DELETE               <optional; true to allow delete, defaults to false>
MAX_MESSAGES               <optional; cap per call, also caps how many uids one change can touch, defaults to 25>
DEFAULT_MESSAGES           <optional; defaults to 10>
MAX_BODY_CHARS             <optional; defaults to 10000>
ALLOWED_SENDER_DOMAINS     <optional>
ALLOW_ATTACHMENT_DOWNLOAD  <optional; defaults to false>
MAX_ATTACHMENT_SIZE        <optional; bytes, defaults to 5242880>
CONNECTION_TIMEOUT         <optional; ms, defaults to 10000>
READ_TIMEOUT               <optional; ms, defaults to 30000>
```

A search opens the folder read only, so looking at a mailbox does not mark
anything seen unless `MARK_AS_READ` is on. Changing the mailbox goes through the
`action` parameter - `markRead`, `markUnread`, `move`, `delete` - and each is
missing from what the engine publishes until the matching `ALLOW_` key is set, so
a model never sees an action it cannot use. Every change takes the `uid` a search
returned. `move` is a copy into the target folder followed by deleting the
original, since IMAP has no move, and the engine reads folders but never creates
them: make `Archive` from a mail client (or `docker exec` an IMAP `CREATE`) before
moving into it.

To try the whole set against GreenMail, create the IMAP engine with
`ALLOW_FLAG_CHANGES`, `ALLOW_MOVE` and `ALLOW_DELETE` set to true, send yourself a
few messages through the SMTP engine, then search with `unreadOnly true` and pass
a returned `uid` back with `action markRead`.

---

Part of the [function engines](README.md) of the
[supporting engines](../README.md).
