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

## Setting it up in Exchange Online

The app registration is only half of the setup. The rest is done in Exchange
Online PowerShell (`Install-Module ExchangeOnlineManagement`), signed in as an
administrator:

```powershell
Connect-ExchangeOnline -UserPrincipalName <admin>@yourdomain.com
```

A browser opens for the sign in and MFA.

### Register the application in Exchange

Exchange keeps its own record of the application, separate from the Entra one.
Nothing below this works without it: role assignments have nothing to assign to,
and the protocols have nothing to grant a mailbox to.

```powershell
New-ServicePrincipal -AppId <application (client) id> `
  -ObjectId <enterprise application object id> `
  -DisplayName "<a name for it>"
```

`<enterprise application object id>` is the Object ID under **Entra ID >
Enterprise applications**, which is a different object with a different id than
the app registration. Newer versions of the module name that parameter
`-ServiceId` rather than `-ObjectId`.

### Limiting which mailboxes Graph can reach

There is one way the Graph column of that table is not simply the easier option.
`Mail.Send` and `Mail.Read` are **application** permissions, which are tenant
wide: consenting to them lets the app registration send and read as every mailbox
in the tenant. The protocol permissions are not, because they do nothing until a
particular mailbox is granted to the service principal.

An engine only ever touches the one mailbox it was configured with, but that is a
convention of this code rather than a boundary. Anyone who can edit the engine, or
who holds the client secret, can point it at any mailbox the token allows.

Narrow it with RBAC for Applications, one assignment per role:

```powershell
New-ManagementScope -Name "Scope-SharedMailboxOnly" `
  -RecipientRestrictionFilter "PrimarySmtpAddress -eq 'reports@yourdomain.com'"

New-ManagementRoleAssignment -Role "Application Mail.Send" -App <application (client) id> `
  -CustomResourceScope "Scope-SharedMailboxOnly"
New-ManagementRoleAssignment -Role "Application Mail.ReadWrite" -App <application (client) id> `
  -CustomResourceScope "Scope-SharedMailboxOnly"
```

Use `Application Mail.Read` rather than `Mail.ReadWrite` for an engine that only
reads - which is the same permission the engine names when Graph refuses it, so
the error tells you which role to assign.

> A scope that filters on `PrimarySmtpAddress` stops matching if that address is
> ever changed, and follows the address if it is reassigned to another mailbox.
> `ExternalDirectoryObjectId` or a custom attribute is steadier.

### Granting the mailbox to the protocols

Skip this if every engine stays on `graph`. RBAC for Applications does not govern
`SMTP.SendAsApp`, `IMAP.AccessAsApp` or `POP.AccessAsApp`. Those are scoped by the
mailbox grant instead, and carry no access at all without one - which is what the
table above is asking for, and why they cannot reach a mailbox nobody named:

```powershell
Add-MailboxPermission -Identity reports@yourdomain.com `
  -User <enterprise application object id> -AccessRights FullAccess
```

The protocol also has to be turned on for the mailbox, separately from the
permission:

```powershell
Set-CASMailbox -Identity reports@yourdomain.com -ImapEnabled $true
```

`-PopEnabled $true` for POP3. SMTP needs `SmtpClientAuthenticationDisabled` to be
`False` on both the tenant (`Set-TransportConfig`) and the mailbox
(`Set-CASMailbox`).

> `AuthenticationFailedException: User is authenticated but not connected` means
> the token was accepted and carried the right role, so what is missing is on the
> Exchange side. The engine logs the audience, the roles and the service
> principal object id the token carried next to that error. Two things produce it.
> Either the mailbox grant above has not been made, or has not taken effect yet -
> allow half an hour before reading a failure as a real one, since retrying before
> it lands fails identically - or the object id in the log does not match the
> `ObjectId` that `Get-ServicePrincipal` reports, which means `New-ServicePrincipal`
> was registered against the app registration's object id rather than the
> enterprise application's. Exchange matches the grant on that id, so a mismatch
> refuses a mailbox that looks granted.

### Checking what is in force

For Graph, ask against a mailbox that should be out of reach as well as the one
that should not:

```powershell
Test-ServicePrincipalAuthorization -Identity <application (client) id> `
  -Resource reports@yourdomain.com
Get-ManagementRoleAssignment -RoleAssignee "<name given to the service principal>" |
  Format-List Role, CustomResourceScope
```

`InScope True` on each role is the answer. A blank `ScopeName` in the plain
`Test-ServicePrincipalAuthorization` output is not a problem: that column reports
the management scope, which stays empty when the assignment uses a custom
resource scope, and `CustomResourceScope` above is where the real answer is.

For the protocols, the service principal has to appear in both lists:

```powershell
Get-ServicePrincipal | Format-List DisplayName, AppId, ObjectId
Get-MailboxPermission -Identity reports@yourdomain.com |
  Where-Object { $_.User -notlike 'NT AUTHORITY*' } | Format-Table User, AccessRights
Get-CASMailbox reports@yourdomain.com | Format-List ImapEnabled, PopEnabled
```

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
