# Permission Batch Model Package

This package holds the permission batch model: the classes that describe
changes to who can use a project, an engine or an insight, or to who is in or
manages a custom team, the problems found when they cannot be made, and the
rules that turn those problems into a permission method's own exceptions. It
has no database code and no permission checks.

The validating and saving live in `prerna.auth.utils.PermissionBatchUtils`,
with the classes only it uses: `PermissionBatchOptions`,
`PermissionAllowedChanges`, `PermissionBatchValidation` and
`PermissionChangeValidation`. All five are package-private, so nothing outside
`prerna.auth.utils` can turn on admin mode, build a batch validation, or save
one.

Every change to these permissions, from Bulk Access Management or from any
single or bulk permission method such as `SecurityProjectUtils.addProjectUser`,
is saved as a permission batch through `PermissionBatchUtils`.

## The classes

```
In prerna.auth.access (public)

What to change (the batches, also sent as JSON)
  PermissionBatch                    changes to one target, validated and saved together
    target: PermissionTargetResource what changes: a project, engine, insight, or a team's members or managers
    changes: PermissionChange[]
      action                         ADD, CHANGE or REMOVE
      subject: PermissionSubject     who: a USER or a TEAM, with an id and a type
      to: PermissionGrant            the access after an ADD or a CHANGE; unused for a REMOVE

What went wrong, and how a permission method reports it
  PermissionProblem                  why one change, or a whole batch, cannot be made: a code and a message
  PermissionBatchException           thrown when batches cannot be saved, with every PermissionProblem found
  PermissionErrorRules               the exception and message a permission method throws for each kind of problem

In prerna.auth.utils (package-private, used only by PermissionBatchUtils)

How to save it
  PermissionBatchOptions             admin mode or not, who is notified, and each method's other choices

What validating a batch finds (built while saving; saving works from these)
  PermissionAllowedChanges           what the user may change on one target
  PermissionBatchValidation          a batch being validated: why it is blocked, if it is
    changes: PermissionChangeValidation[]
                                     each change being validated: what was read from it, and why it is blocked, if it is
```

- `PermissionTargetResource` says what changes. Use its factories: `project(id)`,
  `engine(id)`, `insight(projectId, insightId)`, `team(groupId)` for a custom
  team's members, and `teamManagers(groupId)` for its managers.
- `PermissionSubject` says whose access changes:
  `PermissionSubject.ofUser(id, type)` or `PermissionSubject.ofTeam(id, type)`.
  `type` is the person's login type (such as
  `NATIVE` or `MICROSOFT`, or null on a project, engine or insight, which keep
  only the id) or the team's type.
- `PermissionGrant` is the whole access the subject ends up with: a level, an end
  date, and on a model engine the usage limits. Leaving the end date or a limit
  out clears it; it is never merged with what the subject has now.
- `PermissionChange` ties an action, a subject and a grant together:
  `PermissionChange.add(subject, grant)`, `change(subject, grant)`,
  `remove(subject)`, or `forPeople(action, userIds, grant)` for the same change
  to many people. On a model engine, `new PermissionGrant(level, endDate)
  .withUsageLimits(...)` adds the usage limits.
- `PermissionBatch` groups the changes to one target. A save can take many
  batches, and saves all of them in one transaction or none of them.
- `PermissionBatchOptions` (package-private, in `prerna.auth.utils`) says how a
  save behaves. See [Options](#options). A save takes any number of changes.
- `PermissionErrorRules` keeps a permission method's exceptions. See
  [Errors](#errors).
- `PermissionAllowedChanges` (package-private, in `prerna.auth.utils`) is what
  the user may change on one target: people, Author access, which teams have
  access, and whether only admins can give people access. It is read once for
  each batch, and `getPermissions` returns it as `allowedPermissionChanges`.
- `PermissionBatchValidation` and `PermissionChangeValidation` (package-private,
  in `prerna.auth.utils`) are a batch and its changes while they are validated,
  and what saving works from: the batch as sent is only read once. Each
  `PermissionChangeValidation` starts with the change as sent, gets the
  subject's id and type, the level and end date when the change is read, the
  subject's current level when it is validated, and a code and reason when it is
  blocked. A change that is not blocked is written from those values.
  `PermissionBatchValidation.problems()` lists every `PermissionProblem`, and
  `toResult()` is what `validateBatches` returns.
- `PermissionProblem` carries a `Code` to act on, the message to show, the
  batch's target, the change's index in its batch (null for the whole batch),
  and the subject's id, so a bulk method can list everyone with the same
  problem.
- `PermissionBatchException` is an `IllegalArgumentException`, which the
  endpoints answer with a 400. `getProblems()` returns every problem, the first
  one first.

## What each target takes

| Target kind | Id | Subjects | Grant | Table |
|---|---|---|---|---|
| `PROJECT` | project id | people, teams | level and end date | `PROJECTPERMISSION`, `GROUPPROJECTPERMISSION` |
| `ENGINE` | engine id | people, teams | level, end date, and model limits for people | `ENGINEPERMISSION`, `GROUPENGINEPERMISSION` |
| `INSIGHT` | insight id, with `projectId` | people, teams | level and end date | `USERINSIGHTPERMISSION`, `GROUPINSIGHTPERMISSION` |
| `TEAM` | custom group id | people only | end date only | `CUSTOMGROUPASSIGNMENT` |
| `TEAM_MANAGERS` | custom group id | people only | none | `GROUPMANAGERS` |

- Levels are `OWNER`, `EDIT` and `READ_ONLY`. The UI calls `OWNER` Author.
- End dates are ISO dates and times with a zone, or null for never.
- A person needs a login type only on `TEAM` and `TEAM_MANAGERS`, since the
  other tables keep only the id. A type that is given must match an account.
- A team subject always needs its type.
- `TEAM` and `TEAM_MANAGERS` take only `ADD` and `REMOVE`; they have no level
  to change.

## Who can make a change

Outside admin mode, a user has only their own permissions, even when they are
an admin:

- editors of a project, engine or insight change people's access;
- only owners give or change Author access, and only owners change which teams
  have access;
- only a custom team's managers change its members and its managers;
- anyone can leave a project or an engine when the method allows it.

Where those permissions come from, as
`PermissionBatchUtils.readPermissionAllowedChanges` reads them:

- On a project, an engine or an insight, every permission comes from the user's
  highest current level there (`CurrentPermissionLevel.of`): given to them
  directly, or to a login provider group or a custom team they are in. An
  Author through a team is an Author. A grant, or a team membership, that has
  ended does not count, and neither does a favorite or visibility row. On an
  insight, an Author of its project is an Author of the insight.
- `CurrentPermissionLevel` is the one definition of that level.
  `SecurityProjectUtils.userCanEditProject` and `userIsOwner`, and the engine
  and insight versions, read it too, so every check in Semoss agrees with Bulk
  Access Management.
- A user who is not an owner cannot give themselves access or change their
  own, such as to move or drop its end date; they can only leave.
- The permissions are read from the rows as they are before anything is
  saved. The changes in the batches being saved never give the user
  permissions, even one that adds them to a team that has access.
- A user who can change nothing on the target is told only that, on each
  change, before the changes are read
  (`PermissionBatchUtils.blockForNoPermission`), so a malformed change gets the
  same answer. A team change says it needs an
  owner; anything else says it needs an editor.

Admin mode allows every change the batch's shape and the current rows allow.

## Who can call it

`PermissionBatchUtils` is package-private, so nothing outside
`prerna.auth.utils` can save a permission change except through these public
entry points:

- `SecurityAccessPermissionBatchUtils`, Bulk Access Management's methods:
  `readBatches`, `validateBatches`, `saveBatches`, `getPermissions` and
  `getPermissionsToCopy`. Monolith's `PermissionBatchResource`
  (`/auth/permissionBatch`) and `AdminPermissionBatchResource`
  (`/auth/admin/permissionBatch`) call them, and the semoss-ui SDK mirrors these
  classes in `libs/sdk/src/api/access.ts`.
- `EffectivePermissionUtils.getEffectivePermissions`, which shows everyone who
  can use a project or engine and where their access comes from.
- The single and bulk permission methods of `SecurityProjectUtils`,
  `SecurityEngineUtils`, `SecurityInsightUtils`, `SecurityAdminUtils`,
  `AdminSecurityGroupUtils`, the `SecurityGroup*Utils` and
  `SecurityGroupManagerUtils`.

Copying who can use one project, engine or insight to another is done in Bulk
Access Management: the copied people are staged in the batch, where the user
sees and settles every difference, and saved with the rest of it. There is no
other way to copy access.

Denying access requests changes no one's access, so it is not a batch, but it
goes through `PermissionBatchUtils.denyRequests` all the same: the same
permissions (`readPermissionAllowedChanges`, so only owners, directly or through
a team, can deny), the same `PermissionErrorRules`, and the request rows
written in one transaction.

Admin mode is proven only by holding `SecurityAdminUtils` or
`AdminSecurityGroupUtils` from their `getInstance(user)`, which returns null
for anyone who is not an admin. The admin entry points take those utilities,
or are their instance methods. No public method takes `PermissionBatchOptions`,
or a flag that turns on admin mode, from its callers.

## How a save works

Take
`SecurityProjectUtils.removeProjectUsers(dana, ["alice", "bob"], "sales-app")`,
where Dana edits the project, Alice has Read-Only access and Bob has none.

1. **The method describes the save.** It calls
   `PermissionBatchUtils.saveChanges(user, target, changes, options, errors)`
   with `PermissionTargetResource.project("sales-app")`,
   `PermissionChange.forPeople(REMOVE, ["alice", "bob"], null)` (one REMOVE for
   each person), `PermissionBatchOptions.userMode()`, and
   `new PermissionErrorRules().illegalArgumentWithIds(NO_ACCESS, "...")` and its
   other rules.
2. **`saveChanges` wraps the changes in a `PermissionBatch`** and calls
   `PermissionBatchUtils.save`. `save` gets the options but not the error rules:
   it reports problems as codes, and only `saveChanges` turns them into the
   method's own exception.
3. **`save` validates the batch.** `validateBatch` makes a
   `PermissionBatchValidation`, reads Dana's `PermissionAllowedChanges` on the
   project (`readPermissionAllowedChanges`), reads each change
   (`readChangeFields`), reads the rows Alice and Bob have now
   (`readExistingLevels`: each person's highest level among their rows), and
   validates each `PermissionChangeValidation` (`validatePersonChange`).
   Removing Alice is fine. Removing Bob is blocked with `NO_ACCESS`, since he
   has no access to remove.
4. **`save` stops before writing anything.** It collects every
   `PermissionProblem` and throws a `PermissionBatchException`, so Alice is not
   removed either.
5. **`saveChanges` turns the problems into the method's exception.**
   `PermissionErrorRules.toException` finds the `NO_ACCESS` rule and throws
   "Attempting to modify user permission for the following users who do not
   currently have access to the project: bob".

When no change is blocked, `save` adds Microsoft directory accounts for anyone
who needs one, validates again, writes every change in one transaction, sends the
notifications, and the error rules are never used. A database failure rolls the
transaction back, and `saveChanges` reports it as a `WRITE_FAILED` problem.

Bulk Access Management takes the same path from step 3, through
`SecurityAccessPermissionBatchUtils.saveBatches`, which calls `save` with
`PermissionBatchOptions.bulkAccessManagement(...)`. With no error rules, its
callers get the `PermissionBatchException` with every problem, and
`validateBatches` returns, for each change, whether it is allowed and why not.

Batches sent as JSON use these classes' field names:

```json
[{"target": {"kind": "PROJECT", "id": "abc"},
  "changes": [{"action": "ADD",
               "subject": {"kind": "USER", "id": "jdoe", "type": "NATIVE"},
               "to": {"level": "EDIT", "endDate": null}}]}]
```

## Options

`PermissionBatchOptions` lives in `prerna.auth.utils` and is package-private,
so only that package's permission methods and
`SecurityAccessPermissionBatchUtils` build it.
It starts from one of three factories, then turns on what a caller needs:

| Factory | Used by | What it does |
|---|---|---|
| `bulkAccessManagement(adminMode)` | Bulk Access Management | Notifies every add and change, takes only a level's name as a level, checks the admin-only add settings, and in admin mode checks that the target exists |
| `userMode()` | The single and bulk permission methods | Notifies nobody, reads levels as those methods read them, and leaves the admin-only add settings to the endpoints |
| `adminMode()` | The methods of `SecurityAdminUtils` and `AdminSecurityGroupUtils` | `userMode()` in admin mode |

The builders each keep one method's behavior:

| Builder | What it turns on |
|---|---|
| `notifying(adds, changes)` | Notifications for adds, changes, or both |
| `keepingModelLimits()` | A change on an engine keeps the person's model limits |
| `zeroLimitsAsNone()` | A model limit of 0 is stored as no limit |
| `allowingSelfRemoval()` | Anyone can take away their own access to a project or an engine |
| `removingInsightPermissions()` | Taking someone off a project takes their insight permissions too |
| `withoutTeamExistsCheck()` | A team need not be registered to be given access |
| `keepingTeamType()` | A team's type is stored as given |
| `rejectingSameLevel()` | Giving a team the level it has is refused |
| `grantedBy(id, type)` | Who the rows say gave the access, when it is not the user |
| `approving(target, requests)` | The access requests are marked approved in the same transaction |

`PermissionBatchUtils` reads them through the accessors, such as `isAdminMode()`
and `notifiesAdds()`.

## Errors

A permission method builds its `PermissionErrorRules` from the messages it has
always thrown:

```java
new PermissionErrorRules(endDate)
        .illegalAccess(PermissionProblem.Code.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
        .illegalArgument(PermissionProblem.Code.ALREADY_HAS_ACCESS, "This user already has access ...")
        .illegalArgumentWithIds(PermissionProblem.Code.NO_ACCESS, "... the following users ...: ")
```

- `illegalAccess(code, text)` and `illegalArgument(code, text)` give one text.
- `illegalArgument(code, () -> text)` builds a text that reads the current
  rows only when the problem is found.
- `illegalArgumentWithIds(code, text)` lists every id with the problem after
  the text, for a bulk method.
- `PermissionErrorRules.forApprovals(endDate)` holds the texts every access
  request approval shares.

The rules are checked in the order they are added, and the first one that any
problem matches is used. Problems no rule matches get the messages every
method shares: only admins can give access, someone does not exist, someone is
listed twice, or an end date cannot be read (naming the end date given to the
constructor). Anything else gets the batch's own `PermissionBatchException`.

## Invariants

- Keep the batch classes plain data: a no-argument constructor and fields Gson
  can read. Add fields; do not rename or remove them, since semoss-ui and the
  Monolith endpoints send and read them as they are. `PermissionErrorRules` is
  built in Java only and never read from JSON.
- Keep `PermissionBatchOptions`, `PermissionAllowedChanges`,
  `PermissionBatchValidation` and `PermissionChangeValidation` package-private
  in `prerna.auth.utils`, next to `PermissionBatchUtils`. Anything that decides
  admin mode, or holds what validating found, must not be reachable from
  outside that package.
- Put no database access, permission checks or Semoss utilities here, and do not
  depend on `prerna.auth.utils`. Rules belong in `PermissionBatchUtils`.
- Keep `PermissionBatchUtils` package-private, and its save methods with it. A
  new public way to change access is a method on one of the public classes above
  that calls it.
- Admin mode comes only from holding `SecurityAdminUtils` or
  `AdminSecurityGroupUtils` from `getInstance(user)`. Never take
  `PermissionBatchOptions`, or an admin flag, from a public method's callers.
- Never write the permission tables above directly. A new way to change access
  builds a `PermissionBatch` and saves it through `PermissionBatchUtils`.
- Someone without permission on a target learns nothing from asking. Check
  who may see or change a target before anything that reads it or the people in
  the batch, and give them the same answer whether the target is missing, is not
  theirs, or is the wrong kind (such as a login provider group asked about as a
  custom team). Only admin mode, and those who may see the target, are told
  why it cannot be changed. A refused change never says anything about its
  person: whether they exist, have access, or are a member.
- A new option is a private field with its doc comment, a builder that turns it
  on, an accessor, and the check or write in `PermissionBatchUtils` that reads it.
- A new target kind needs, together: the `Kind` value, a
  `PermissionTargetResource` factory, `describe()`, the permissions, checks,
  reads and writes in `PermissionBatchUtils`, and the type in
  `libs/sdk/src/api/access.ts`.
- A new problem code needs a doc comment on the `Code` value and a check in
  `PermissionBatchUtils` that sets it. Methods that need their own message for
  it add a rule to their `PermissionErrorRules`.
- Name things for what they do, and write docs and comments in ASCII that
  describe the current behavior.

## Verification

Tests live in `test/prerna/auth/utils`:

- `PermissionBatchUtilsUnitTests`: Bulk Access Management's validation, saves,
  reads and admin mode, through `SecurityAccessPermissionBatchUtils`.
- `PermissionBatchUtilsSaveChangesUnitTests`: the options and rules the single
  and bulk permission methods share.
- `EffectivePermissionUtilsUnitTests`: the effective access view.
