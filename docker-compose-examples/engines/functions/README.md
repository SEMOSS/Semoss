# Function engines

Function engines are the tools a model can call. The ones documented here all
send or read mail; each page below covers one way of doing that.

A function engine takes three keys on top of its own settings, and all three have
to be present - `AbstractFunctionEngine` refuses to open an SMSS that is missing
`FUNCTION_NAME` or `FUNCTION_DESCRIPTION`. The description may be left blank
though, and each engine then publishes its own:

```
FUNCTION_TYPE           SMTP | POP3 | IMAP | EXCHANGE_SMTP | EXCHANGE_POP3 | EXCHANGE_IMAP
FUNCTION_NAME           <what a model calls this, e.g. send_email>
FUNCTION_DESCRIPTION    <what it does; blank falls back to the engine's own wording>
```

Leave `FUNCTION_PARAMETERS` and `FUNCTION_REQUIRED_PARAMETERS` unset unless you
want to override what the engine publishes. Each engine fills them in from its own
settings, so the parameter descriptions already carry the limits it was given.

## The mail engines

Every mail engine below is one of these types, and they share the send or read
settings of the engine they extend.

| Page | Types | What it does |
|------|-------|--------------|
| [Sending](mail-sending.md) | `SMTP` | send through a mail server |
| [Reading](mail-reading.md) | `POP3`, `IMAP` | read a mailbox over the protocols |
| [Microsoft 365](mail-microsoft-365.md) | `EXCHANGE_SMTP`, `EXCHANGE_POP3`, `EXCHANGE_IMAP` | send and read a Microsoft 365 mailbox, through Graph or the protocols |
| [Testing locally](mail-testing.md) | - | GreenMail, the TLS ports, and the `SendEmail` pixel |

The local mail server that backs all of the non-Microsoft ones is
[semoss-mail.yml](../semoss-mail.yml); see [testing locally](mail-testing.md).
