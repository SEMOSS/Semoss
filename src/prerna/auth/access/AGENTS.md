# Permission Batch Model Package

This package holds the permission batch model: the classes that describe
changes to who can use a project, an engine or an insight, or to who is in or
manages a custom team, the problems found when they cannot be made, and the
rules that turn those problems into a permission method's own exceptions. On a
model, the same changes carry usage limits (see
[Usage limits on a model](#usage-limits-on-a-model)). It has no database code
and no permission checks.

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
      subject: PermissionSubject     who: a USER or a TEAM, with an id and a type; or a model's PUBLIC use or its MODEL total
      to: PermissionGrant            the access after an ADD or a CHANGE, with a model's usageLimit; unused for a REMOVE

What went wrong, and how a permission method reports it
  PermissionProblem                  why one change, or a whole batch, cannot be made: a code and a message
  PermissionBatchException           thrown when batches cannot be saved, with every PermissionProblem found
  PermissionErrorRules               the exception and message a permission method throws for each type of problem

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
  only the id) or the team's type. On a model, a subject of category `PUBLIC`
  or `MODEL`, with no id or type, is its public use or its total, which have
  only a usage limit.
- `PermissionGrant` is the whole access the subject ends up with: a level, an end
  date, and on a model a usage limit (`usageLimit`, a `ModelUsageLimit`, or
  null for no limit defined). Leaving the end date or the limit out clears it;
  it is never merged with what the subject has now, unless the options keep the
  limit.
- `PermissionChange` ties an action, a subject and a grant together:
  `PermissionChange.add(subject, grant)`, `change(subject, grant)`,
  `remove(subject)`, or `forPeople(action, userIds, grant)` for the same change
  to many people. On a model, `new PermissionGrant(level, endDate)
  .withUsageLimit(limit)` gives the usage limit.
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

| Target resource type | Id | Subjects | Grant | Table |
|---|---|---|---|---|
| `PROJECT` | project id | people, teams | level and end date | `PROJECTPERMISSION`, `GROUPPROJECTPERMISSION` |
| `ENGINE` | engine id | people, teams; on a model also its public use and its total | level, end date, and on a model a usage limit for people and teams; only a usage limit for public use and the total | `ENGINEPERMISSION`, `GROUPENGINEPERMISSION`, and `MODELUSAGELIMIT` for the limits |
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
- A model's `PUBLIC` use and `MODEL` total take only `CHANGE`, on an `ENGINE`
  target that is a model, with `to: {"usageLimit": ...}` and no level or end
  date. Making a model public is still `setEngineGlobal`.
- A team's grant on an engine keeps its `DATEADDED` when its level changes,
  since the order the teams are billed in depends on it.

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
  ended does not count, and neither does a row with no level. On an
  insight, an Author of its project is an Author of the insight.
- `CurrentPermissionLevel` is the one definition of that level.
  `SecurityProjectUtils.userCanEditProject` and `userIsOwner`, and the engine
  and insight versions, read it too, so every check in Semoss agrees with Bulk
  Access Management.
- A user who is not an owner cannot give themselves access or change their
  own, such as to move or drop its end date; they can only leave.
- On a model, only owners set usage limits: anyone's, their own included, team
  limits, the public use limit and the total. Editors still add people and
  change Read-Only and Editor access; their adds give no limit and their changes
  keep the limit as it is. See [Who may set a limit](#who-may-set-a-limit).
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
  `getPermissionsToCopy`, and on a model `getUsageLimitPath` and
  `getMyUsageLimitPath`. Monolith's `PermissionBatchResource`
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
written in one transaction. That transaction also reads back the requesters of
the requests it denied, which `denyRequests` returns; the project and engine
methods notify them from those rows once it has committed.

Approving and denying touch only the target's own access requests: a request
id of another project, engine or insight is left as it is, and is not
returned or recorded as denied.

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
who needs one, validates again, writes every change in one transaction, and
once it has committed records the audit trail (`PermissionBatchAudit`) and
sends the notifications, and the error rules are never used. Each notification
is written on its own, so one that fails is logged and does not undo the save,
and a save that fails sends none. The audit trail gets one event per change of
access, or a summary the caller records when the options say
`withSummaryAudit()`, and one `USAGE_LIMIT_UPDATE` event per usage limit a
change sets, changes or removes, summary or not. A database failure rolls the
transaction back, and `saveChanges` reports it as a `WRITE_FAILED` problem.

Bulk Access Management takes the same path from step 3, through
`SecurityAccessPermissionBatchUtils.saveBatches`, which calls `save` with
`PermissionBatchOptions.bulkAccessManagement(...)`. With no error rules, its
callers get the `PermissionBatchException` with every problem, and
`validateBatches` returns, for each change, whether it is allowed and why not.

Batches sent as JSON use these classes' field names:

```json
[{"target": {"resourceType": "PROJECT", "id": "abc"},
  "changes": [{"action": "ADD",
               "subject": {"category": "USER", "id": "jdoe", "type": "NATIVE"},
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
| `keepingModelLimits()` | A change on an engine whose grant does not give its usage limit keeps the limit of the person's or the team's grant |
| `allowingSelfRemoval()` | Anyone can take away their own access to a project or an engine |
| `removingInsightPermissions()` | Taking someone off a project takes their insight permissions too |
| `withoutTeamExistsCheck()` | A team need not be registered to be given access |
| `keepingTeamType()` | A team's type is stored as given |
| `rejectingSameLevel()` | Giving a team the level it has is refused |
| `grantedBy(id, type)` | Who the rows say gave the access, when it is not the user |
| `approving(target, requests)` | The target's own access requests are marked approved in the same transaction, and people who have access already keep their usage limit |

`PermissionBatchUtils` reads them through the accessors, such as `isAdminMode()`
and `notifiesAdds()`.

## Usage limits on a model

A model's usage limits are kept in `MODELUSAGELIMIT` and `MODELUSAGELIMITRULE`
(`prerna.auth.utils.SecurityModelUsageLimitUtils`), apart from the permission
tables. The limits on grants, and a model's public use limit and total, are
part of permission batches: they are validated with the change and written in
the batch's own transaction through `SecurityModelUsageLimitUtils.setLimit`.

- A person's grant on a model is `ModelUsageLimitKey.direct(engineId,
  userId, userType)`, a team's is `ModelUsageLimitKey.team(engineId, teamId,
  teamType)`, and the subjects `PUBLIC` and `MODEL` are
  `ModelUsageLimitKey.publicUse(engineId)` and `modelTotal(engineId)`.
- A person's grant keeps its limit with their login type, though
  `ENGINEPERMISSION` keeps only the user id: the change's subject type, or else
  the one login type `SMSS_USER` has for the user id. A limit whose login type
  is not known is kept with none and applies to the user id on any login;
  writing the limit with a login type replaces it, and start up gives it one
  where the user id has one login (`fillDirectLimitLoginTypes`). Read a grant's
  limit with `SecurityModelUsageLimitUtils.findDirectLimit`, which falls back
  to the one kept with none.
- Removing a grant removes its limit on every login type, since the grant was
  the same for each. A change that leaves the limit as it is writes nothing.
- A key has one `MODELUSAGELIMIT` row: `setLimit` deletes every row the
  key has, its rules first, and writes one new row. Should two writes at the
  same time leave two, reads use the one changed last.
- Every write ends with `SecurityModelUsageLimitUtils.markChanged()`, called
  after its transaction commits, never inside it, so the limits cached on the
  server (`ModelUsageGuard`) are read again. That includes writes of grants,
  team memberships and a model being public, which decide which limit applies.
  `setLimit` and the deletes run in the caller's transaction and leave it to
  the caller.
- No limit defined (null) and Unlimited are different: no limit defined passes
  the decision on to the next source, Unlimited ends it.

### How a grant's limit is read when it is saved

1. With `keepingModelLimits()`, a `CHANGE` whose grant does not give its
   usage limit (`isUsageLimitGiven()` is false and `usageLimit` is null) keeps
   the current limit. The engine user methods that take only a level and an end
   date, which the endpoints call when a form has no `usageLimit`, use it, and
   so do the bulk engine edit methods, whose rows without a `usageLimit` key
   keep the person's limit.
2. Otherwise the grant's `usageLimit`: the one a Java caller gave with
   `withUsageLimit`, null included, or the one read from JSON; null is no limit
   defined.

The limit is then normalized (`SecurityModelUsageLimitUtils.normalize`): 0
means no limit defined, so rules of 0 are dropped. In a batch read from JSON,
`"usageLimit": null` is the same as leaving it out: no limit defined, since
`usageLimitGiven` is not read from JSON. The bulk engine methods' rows give the
limit as a `usageLimit` key, a map or JSON text; a key with null gives no limit
defined, so on an edited row it removes the limit
(`PermissionBatchUtils.engineGrant`). Token amounts are longs end to end.

### Who may set a limit

Only a model's owners, and admin mode, set usage limits.

| Whose limit | Owners | Editors | Admin mode |
|---|---|---|---|
| A person's, at any level | Yes | No | Yes |
| Their own | Yes | No (they cannot change their own grant) | Yes |
| A team's limit, the public use limit, the total | Yes | No | Yes |

Editors still add people and change the level of Read-Only and Editor people,
as the level rules allow. An editor's add gives no limit, and an editor's
change keeps the current limit: it sends the limit back as it is, or, through
the engine user methods, sends no `usageLimit`. A change by an editor that
leaves any other limit is refused with "Only owners can set usage limits".
`PermissionAllowedChanges` holds this as `limitLevels` (every level for
owners, none for anyone else) and `canChangeModelLimits` (owners and admin
mode).

Only owners, and admin mode, take away the access of someone whose grant on the
model has a usage limit, rules or Unlimited, on any of their login types, or
give them an earlier end date. The limit goes with the grant, so otherwise an
editor could lift it: take the person away and add them back with no limit, or,
on a public model, just take them away and leave them on the public use limit.
This holds for someone leaving on their own too. Dropping an end date, or moving
it later, is open to editors, since the limit stays for as long or longer.

### What is refused

Each refusal is a reason on the change, following the no-leak rules:

- `INVALID_USAGE_LIMIT`: a limit on an engine that is not a model, or
  `SecurityModelUsageLimitUtils.findProblem` refuses the limit where it is set,
  such as a cost rule on a model without known prices
  (`ModelUsageCost.hasKnownPrices`), a shared rule on a person's grant, or a
  rule for each person on the total.
- `OWNERS_ONLY`: someone who is not an owner leaves a limit other than the
  current one on a person or a team ("Only owners can set usage limits"), or
  changes the public use limit or the total. The engine user methods throw it
  with that message.
- `ENDS_LIMITED_ACCESS`: someone who is not an owner takes away the access of a
  person with a usage limit on the model, their own included, or gives them an
  earlier end date ("Only owners can end or shorten the access of someone with
  a usage limit"). The engine user methods throw it with that message.
- `NOT_WELL_FORMED`: a `PUBLIC` or `MODEL` change that is not a `CHANGE` on an
  engine, or that carries a level or an end date.

### Audit trail

Every usage limit a save leaves different is recorded as a `USAGE_LIMIT_UPDATE`
event (`ModelUsageLimitAudit`), in the `USAGE_LIMIT` category: the limit before
and after as `{"usageLimit": ...}` (null for no limit defined), and
`details.limitType` (`DIRECT`, `TEAM`, `PUBLIC` or `MODEL_TOTAL`, with the team for
a Team Limit). A grant's removal records its limit removed with it
(`removedWithGrant`), and so does a grant that ended, removed by SEMOSS
(`removeExpiredEngineUser`, `reason` `EXPIRED`). Changes to a model's public use
or total record only this event, since they change no access. `PermissionChangeValidation` keeps the limit
before the change (`getCurrentUsageLimit`) for it. A model's owners read its limit changes on its
Audit tab (`ResourceAuditReadService`); admins read them with the access changes, the app and engine
setting changes and the denied requests in Enterprise Usage & Activity (the `audit` source of
`EnterpriseUsageUtils`).

Each change to an app's or engine's own settings records an event of its own: renamed
(`PROJECT_RENAME`, `ENGINE_RENAME`), display name (`..._DISPLAY_NAME_UPDATE`), made public or private
(`..._GLOBAL_UPDATE`), discoverable (`..._DISCOVERABLE_UPDATE`), an app made a template
(`PROJECT_TEMPLATE_UPDATE`) and metadata set or loaded (`..._METADATA_UPDATE`), from the owner's and
the admin's paths alike, with the setting and its new value in `details`.
`AuditEventType.SETTING_CHANGES` lists them; Enterprise Usage counts them as `SETTING_CHANGES`. Saving
an app's blocks (`PROJECT_BLOCKS_UPDATE`), replacing an app by upload (`PROJECT_REPLACE`) and a user
showing or hiding an engine in their own lists (`ENGINE_VISIBILITY_UPDATE`) have their own events too,
on the Audit tab only. No code records `PROJECT_UPDATE` or `ENGINE_UPDATE`; they are kept so stored
events of those types still read.

Exporting a project or an engine for download records `PROJECT_EXPORT` (`ExportProject`,
`ExportProjectApp`, `ExportApp`) or `ENGINE_EXPORT` (`ExportEngine`, `ExportDatabase`), both at `HIGH`
severity, since a copy of the resource leaves the platform. `UserAuditTrailUtils.recordProjectExport`
and `recordEngineExport` record it once the archive is built, with `details.reactor` (the pixel name,
from `AbstractReactor.getPixelName`), the project's `projectType`, and `keepGit` where the export can
keep the version history. An export that fails records its failure under the same event type
(`AUDITED_REACTORS`), never a success. Owners see both on the Audit tab with those three details.

### Reads

- `getPermissions` gives each grant on an engine its `usageLimit`. On a model
  it adds `publicUse` (`isPublic`, `usageLimit`), `modelTotal` (`usageLimit`),
  `costLimitsAvailable`, `price` (`ModelUsageCost.priceEntry`), and
  `allowedPermissionChanges.limitLevels` and `canChangeModelLimits`.
- `getPermissionsToCopy` gives each grant on an engine its `usageLimit`, so a
  copy between models can keep it.
- `EffectivePermissionUtils.getEffectivePermissions` adds each person's `usage`
  on a model (`billedTo`, `unlimited`, `rules`, `nextTeams`, `notApplied`), from
  one `ModelUsageLimitResolver.resolveStored` for everyone, with `publicUse`,
  `ceilings` and each `unlistedTeams` row's `usageLimit`.
- `SecurityAccessPermissionBatchUtils.getUsageLimitPath` (the model's editors
  and owners, or admin mode) and `getMyUsageLimitPath` (anyone who can use the
  model) return how limits apply to one person, and `getMyUsageLimitPaths` how
  they apply to the signed in user on every model they can use. Someone who may
  not see the model gets the same answer as for a model that does not exist.
- `getUsageLimitPath` needs the person's login type, normalized as other login
  types are (`SecurityGroupManagerUtils.normalizeUserType`), in admin mode too.
  Outside admin mode the person must have a grant on the model as stored: their
  own, or a team's they are in; the model being public is not enough. It hides
  what is not on this model (`ModelUsagePathView.toMap(path, true, true)`): the
  `ALL_MODELS` and `PLATFORM` steps have no `rules` and say
  `"rulesHidden": true`, and `TEAM` steps have no `teamTotal`. The person's own
  paths and admin mode show everything (`ModelUsagePathView.toMap(path,
  true)`).

## Errors

A permission method builds its `PermissionErrorRules` from the messages it has
always thrown:

```java
new PermissionErrorRules(endDate)
        .illegalAccess(PermissionProblem.ErrorCode.NO_PERMISSION, INSUFFICIENT_PRIVILEGES)
        .illegalArgument(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS, "This user already has access ...")
        .illegalArgumentWithIds(PermissionProblem.ErrorCode.NO_ACCESS, "... the following users ...: ")
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
  theirs, or is the wrong type (such as a login provider group asked about as a
  custom team). Only admin mode, and those who may see the target, are told
  why it cannot be changed. A refused change never says anything about its
  person: whether they exist, have access, or are a member.
- A new option is a private field with its doc comment, a builder that turns it
  on, an accessor, and the check or write in `PermissionBatchUtils` that reads it.
- A new target resource type needs, together: the `TargetResourceType` value, a
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
- `PermissionBatchUtilsUsageLimitUnitTests`: usage limits on a model's grants,
  its public use and its total: how they are read, who may set them, what is
  refused, the engine user methods and project propagation, and the reads.
- `SecurityModelUsageLimitUtilsUnitTests`: how limits are stored, one row for
  each key, and which one applies, on the security database's own tables.
- `EffectivePermissionUtilsUnitTests`: the effective access view.

`test/prerna/auth/access/ModelUsagePathViewUnitTests` covers the path view, in
full and with the limits on other models hidden.
