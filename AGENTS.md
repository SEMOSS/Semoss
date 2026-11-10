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

## A reactor describes itself in its definition

A reactor tells its MCP tool, its help text and the SDKs what it does by overriding
`getReactorDefinition()` (`prerna.reactor.ReactorDefinition`). It calls
`super.getReactorDefinition()`, which starts from `keysToGet` and `keyRequired`, and adds
its description, its keys' descriptions and types, and its output:

```java
@Override
public ReactorDefinition getReactorDefinition() {
	return super.getReactorDefinition()
			.description("Stop the current execution of a pixel job")
			.key(ReactorKeysEnum.ID, "The id for the job")
			.key(PATHS_KEY, key -> key.list(String.class).description("The file paths"))
			.output(String.class, "Whether the job was stopped");
}
```

- **Metadata only.** `keysToGet` and `keyRequired` still decide how a call's inputs are read.
  The definition can say more, such as `key.required(true)`, without changing that.
- **No keys on purpose.** A reactor that takes no input says `.noKeys()`.
- **Admin reactors say so first.** A reactor only admins can run starts its description with
  `Admin Only: `, such as `"Admin Only: Returns every engine on the server, ..."`. A reactor that
  admins can run with extra access, but other users can run too, does not.
- **Parents describe only shared keys.** A parent's definition describes only the keys every
  reactor below it takes. Text several reactors share lives in the parent as a constant, such as
  `TEAM_ID_DESCRIPTION`, and each reactor describes the keys it takes with it. A reactor that does
  not take its parent's keys starts from `startDefinition()` instead of
  `super.getReactorDefinition()`. Describing a key a reactor does not take, or listing a key twice
  in `keysToGet`, is reported, with the class to fix, and fails the OpenAPI export.
- **Reading it.** Code that needs a reactor's description or a key's type calls
  `getReactorDefinition()`, not `getReactorDescription`, `getDescriptionForKey` or
  `getKeyTypeForMCP`.
- **Migrating.** Those three methods are deprecated. New code does not override them, and a
  change that touches a reactor still overriding them moves the values into
  `getReactorDefinition()`, as each method's javadoc shows, and deletes the old methods.
