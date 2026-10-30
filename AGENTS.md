# AGENTS.md - SEMOSS

Rules for anyone, person or coding agent, changing this repository. Guides inside the source
tree add rules for their own packages, such as
[src/prerna/auth/access/AGENTS.md](src/prerna/auth/access/AGENTS.md) for permissions and
[src/prerna/reactor/automation/AGENTS.md](src/prerna/reactor/automation/AGENTS.md) for
Automation.

## A user is an id and a login type

A user id is unique only within its login type: `jdoe` with `NATIVE` and `jdoe` with
`MICROSOFT` are two different people, each with their own `SMSS_USER` row (`ID` and `TYPE`).
So a user is always the pair, stored and passed together, never the id alone:

- **Tables.** A table that records a user stores the login type beside the id, such as
  `USER_ID` and `USER_TYPE`, or `USERID` and `TYPE`. Every insert writes both.
- **Code.** Methods, records and maps that carry a user carry both, as two values or a
  `Pair<String, String>` of id and type. Take both from the same login: a login's
  `AccessToken` gives the id (`getId()`) and the type (`getProvider().getLabel()`), and
  `User.getUserIdAndType` gives the pair for each of a user's logins.
- **Reads.** A query for a user's rows matches the id and the type. For the signed in user,
  match any of their logins, as `AbstractSecurityUtils.getUserIdAndTypeFilter` does. A row with
  no type matches the id alone, since its login type is not known.
- **Pixel and REST.** A reactor key, request or response that names a user takes or returns
  both, such as `userId` with `userType`.
- **The type's value** is the provider's label, such as `NATIVE` or `MICROSOFT`
  (`AuthProvider.getLabel()`), the value `SMSS_USER.TYPE` holds.

New code always keeps the pair. When a change works with a table, method or API that has only
the user id, it adds the type beside it.
