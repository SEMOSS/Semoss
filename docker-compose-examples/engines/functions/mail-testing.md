# Testing the mail engines locally

[semoss-mail.yml](../semoss-mail.yml) runs GreenMail, which speaks SMTP, POP3 and
IMAP at once, so one container backs all three protocol engines. Mail lives in
memory: restarting the container is the fastest way to get an empty mailbox
again.

> Not for the Microsoft 365 engines. Their credentials come from Microsoft, so
> testing those needs a real tenant - see
> [Microsoft 365 mailboxes](mail-microsoft-365.md).

## GreenMail specifics worth knowing

- **The login is the whole address.** `-Dgreenmail.users=semoss@semoss.local:semoss`
  creates a mailbox whose username is `semoss@semoss.local`, not `semoss`. Add
  `-Dgreenmail.auth.disabled` to `GREENMAIL_OPTS` to accept any login instead,
  which also creates the mailbox on the fly.
- **POP3 and IMAP share one store.** Reading a message over POP3 marks it seen for
  IMAP, so an `unreadOnly` IMAP search right after a POP3 read comes back empty.
- **Mail is only in memory.** `docker compose -f semoss-mail.yml restart` is the
  reset button.
- **Any recipient is accepted.** GreenMail delivers to addresses that were never
  configured, so you can send to `whoever@semoss.local` and read it back from the
  API even though no such mailbox was declared.

## Using the TLS ports

GreenMail also serves SMTPS / POP3S / IMAPS on `3465` / `3995` / `3993`, but with a
self-signed certificate that does not match the host. The engines require TLS that
is both trusted and hostname-matched, so they refuse it, correctly:

```
Error occurred connecting to the mail server defined. Detailed error: PKIX path
building failed ... unable to find valid certification path to requested target
```

Any key starting with `mail.` is passed straight through to jakarta.mail, applied
after the engine's own defaults so it wins, and matched whichever case it is
written in - the reactor that creates an engine upper cases every key, and
jakarta.mail only answers to its own lower case names. Two of those keys relax
exactly this:

```
IMAP_PORT                            3993
IMAP_SECURITY                        ssl
MAIL.IMAPS.SSL.TRUST                 localhost   (or semoss-mail from Docker)
MAIL.IMAPS.SSL.CHECKSERVERIDENTITY   false
```

Use `MAIL.POP3S.*` or `MAIL.SMTP.*` for the other two protocols. Both keys are
needed: trusting the certificate still leaves the hostname check to fail.

> Those two keys are the point of the passthrough, and they are also the two you
> must never carry to a real mail server - together they accept any certificate
> from anyone. For local testing the plaintext ports are the simpler choice.

## Testing the SendEmail pixel

`SendEmail` can be pointed at a mail server per call, without creating an engine at
all, which is the quickest way to check a template:

```
SendEmail(smtpHost=["localhost"], smtpPort=["3025"], smtpSecurity=["none"],
          from=["semoss@semoss.local"], to=["reports@semoss.local"],
          subject=["Test"], message=["Hello"]);
```

`smtpSecurity` matters here: with no credentials the call defaults to `none`, but
with `username` / `password` it defaults to `starttls`, which GreenMail's plaintext
port does not offer. Left to itself, `SendEmail` uses the instance wide mail server
from `social.properties` instead - the `smtp_*` keys, which the
[docker configuration guide](../../../docs/deployment/docker_configuration.md)
covers.

These transient sends use the same `SMTPFunctionEngine` and `EmailUtility`
delivery path as a cataloged engine, including email-attempt tracking.

---

Part of the [function engines](README.md) of the
[supporting engines](../README.md).
