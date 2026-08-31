# Microsoft 365 mailboxes

A Microsoft 365 mailbox is reached one of two ways, and every Exchange engine
takes the same key to say which:

```
MAIL_TRANSPORT             graph | jakarta        (defaults to graph)
```

`graph` goes through the Microsoft Graph API. `jakarta` goes through the mail
protocol - SMTP, POP3 or IMAP - with the token presented over XOAUTH2. Nothing
else about an engine changes with it: the same guardrails apply, the same
parameters are published, and a search answers in the same shape either way.

Graph is the default because it needs far less to be true:

| Type | Graph needs | The protocol needs |
|------|-------------|--------------------|
| `EXCHANGE_SMTP` | `Mail.Send` | `SMTP.SendAsApp`, a service principal, a mailbox grant, and SMTP AUTH enabled for the tenant **and** the mailbox |
| `EXCHANGE_IMAP` | `Mail.Read`, or `Mail.ReadWrite` when a change is enabled | `IMAP.AccessAsApp`, a service principal, and a mailbox grant |
| `EXCHANGE_POP3` | `Mail.Read` | `POP.AccessAsApp`, a service principal, and a mailbox grant |

The Graph permissions are granted under **Microsoft Graph**; the protocol ones
under **Office 365 Exchange Online** (the "APIs my organization uses" tab). They
are separate grants, so an app registration set up for one does not serve the
other. Both need admin consent.

The app registration itself is the same either way, which is the point:

```
EXCHANGE_TENANT            <directory (tenant) id, or the tenant domain>
EXCHANGE_CLIENT_ID         <application (client) id>
EXCHANGE_CLIENT_SECRET     <client secret on the app registration>
EXCHANGE_SCOPE             <optional; overrides the scope asked for>
GRAPH_BASE_URL             <optional; defaults to https://graph.microsoft.com/v1.0, for a sovereign cloud>
```

One thing happens behind that: a token is issued for one resource at a time, so
the protocols get a token for `outlook.office365.com` and Graph gets one for
`graph.microsoft.com`. Same credentials, different scope, handled for you.

The mailbox is named the way the protocol engine names it - `SMTP_USERNAME`,
`POP3_USERNAME`, `IMAP_USERNAME` - and there is no password key. On `graph` the
host, port and security settings are ignored, since there is no server to reach.

Two differences worth knowing when moving an engine from `jakarta` to `graph`:

- **`uid` becomes a string.** Graph names a message with an opaque id where the
  protocols use a number. It round trips the same way, so `action` calls work
  unchanged, but anything that stored a uid from the other transport will not
  find it.
- **`delete` is gentler.** Graph moves a message to Deleted Items where IMAP
  expunges it.

When Graph refuses a call, the engine says which permission it wanted and what
the token actually carried, which separates a missing consent from a mailbox the
application was never granted:

```
Microsoft Graph would not read the mailbox. Detailed error: ErrorAccessDenied: Access
is denied. Reading through Graph needs the Mail.Read application permission on the app
registration, with admin consent. It is granted under Microsoft Graph rather than
Office 365 Exchange Online ...
```

> GreenMail cannot stand in for either transport here. The credentials come from
> Microsoft, so testing needs a real tenant, app registration and mailbox. Use the
> plain SMTP, POP3 and IMAP engines against GreenMail for everything that is not
> the sign in.

> On a Microsoft 365 mailbox, `EXCHANGE_IMAP` is almost always the better read
> engine than `EXCHANGE_POP3`. Over Graph the two are the same API, and the POP3
> engine only keeps its promise of a single inbox with no folders and no read
> state.

---

Part of the [function engines](README.md) of the
[supporting engines](../README.md).
