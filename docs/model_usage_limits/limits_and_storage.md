# Limits, Rules and Storage

The types every part of model usage limits passes around, the two security
database tables they live in, and the one class that reads and writes them.

## A limit has three states

`ModelUsageLimit` ([source](../../src/prerna/auth/access/ModelUsageLimit.java)):

| Value | Meaning |
| --- | --- |
| `null` | No Limit Defined. The source is skipped, and the next source in the order is tried. |
| `ModelUsageLimit.unlimited()` | Unlimited. A decision that stops the search: no later source applies. Ceilings still apply. |
| `ModelUsageLimit.of(rules)` | Rules. A call must fit in every one of them. |

An amount of 0 means no limit defined. `SecurityModelUsageLimitUtils.normalize`
drops rules of 0 and turns a limit left with no rules into `null`.

## A rule

`ModelUsageLimitRule` ([source](../../src/prerna/auth/access/ModelUsageLimitRule.java)):

| Field | Values | Notes |
| --- | --- | --- |
| `metric` | `TOKENS`, `COST`, `COMPUTE_TIME` | Tokens count prompt and output; cost is in millionths of a US dollar; compute time is in milliseconds. |
| `amount` | a long, 1 or more | |
| `window` | `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR` | Minute and hour windows slide; day and longer are fixed UTC calendar windows (weeks run Sunday to Saturday). |
| `windowLength` | 1 to 59 for minutes, 1 to 24 for hours | Kept as null for 1. Only minute and hour windows have a length. |
| `counts` | `EACH_PERSON`, `SHARED` | `SHARED` is one total for everyone the source covers; `EACH_PERSON` gives each person their own amount. |

"100,000 tokens every 10 minutes for each member" is
`new ModelUsageLimitRule(Metric.TOKENS, 100000L, Window.MINUTE, 10, Counts.EACH_PERSON)`.

As JSON, the shape the endpoints and the UI use:

```json
{ "unlimited": false,
  "rules": [ { "metric": "TOKENS", "amount": 100000, "window": "MINUTE",
               "windowLength": 10, "counts": "EACH_PERSON" } ] }
```

## Where a limit is set: `ModelUsageLimitKey`

A record of `(type, engineId, subjectId, subjectType)`
([source](../../src/prerna/auth/access/ModelUsageLimitKey.java)). Build one with
its factories:

| Factory | Source | `type` | `engineId` | `subjectId` / `subjectType` |
| --- | --- | --- | --- | --- |
| `direct(engineId, userId, type)` | Direct Limit | `USER` | the model | user id / login type |
| `allModels(userId, type)` | User's Total Model Limit | `USER` | null | user id / login type |
| `team(engineId, teamId, teamType)` | Team Limit | `TEAM` | the model | team id / team type |
| `teamTotal(teamId, teamType)` | Team Total (ceiling) | `TEAM` | null | team id / team type |
| `publicUse(engineId)` | Public Use Limit | `PUBLIC` | the model | null / null |
| `modelTotal(engineId)` | Model Total (ceiling) | `MODEL` | the model | null / null |
| `platform()` | General Platform Limit | `PLATFORM` | null | null / null |

`isDirect()` is a `USER` key with a model. `isCeiling()` is the Model Total and
the Team Total.

**Direct Limits and login types.** A grant in `ENGINEPERMISSION` keeps only the
user id, but its Direct Limit keeps the login type: the change's subject type,
or else the one login type `SMSS_USER` has for that user id. When the login type
is not known, the limit is stored with a null type and applies to that user id
on any login. At startup, untyped Direct Limits get a type wherever the user id
has a single login.

## Tables

Both are in the security database, created by
[SecurityOwlCreator](../../src/prerna/auth/utils/SecurityOwlCreator.java):

| Table | Columns |
| --- | --- |
| `MODELUSAGELIMIT` | `ID`, `ENGINEID`, `LIMITTYPE`, `SUBJECTID`, `SUBJECTTYPE`, `UNLIMITED`, `DATEADDED`, `ADDEDBYID`, `ADDEDBYTYPE`, `DATEMODIFIED`, `MODIFIEDBYID`, `MODIFIEDBYTYPE`: one row per key |
| `MODELUSAGELIMITRULE` | `LIMITID`, `METRIC`, `AMOUNT`, `LIMITWINDOW`, `WINDOWLENGTH`, `COUNTS`: the rules of a limit |

`MODELUSAGELIMIT` is indexed on `(ENGINEID, LIMITTYPE, SUBJECTID)`, and
`MODELUSAGELIMITRULE` on `LIMITID`. `LIMITWINDOW` avoids `WINDOW`, a reserved
word in some databases.

The inference logs' `MESSAGE` table records each call's usage with
`USER_TYPE`, the source billed (`BILLED_SOURCE_TYPE`, `BILLED_SUBJECT_ID`,
`BILLED_SUBJECT_TYPE`) and `COST_MICROS`, with indexes on
`(USER_ID, AGENT_ID, DATE_CREATED)` and
`(BILLED_SOURCE_TYPE, BILLED_SUBJECT_ID, AGENT_ID, DATE_CREATED)`. These rows
fill cold counters ([enforcement](enforcement.md#filling-a-counter-from-the-logs)).

## Reading and writing: `SecurityModelUsageLimitUtils`

Every read and write of a limit goes through
[SecurityModelUsageLimitUtils](../../src/prerna/auth/utils/SecurityModelUsageLimitUtils.java).

| Method | Use |
| --- | --- |
| `getLimit(key)`, `getLimits(keys)` | One limit, or several in one read |
| `getEngineLimits(engineId)` | Every limit set on a model |
| `findDirectLimit(limits, engineId, userId, type)` | A person's Direct Limit, by login type (below) |
| `getPlatformDefault()`, `getTeamTotal(teamId, teamType)` | The admin-set limits |
| `setPlatformDefault(adminUtils, ...)`, `setTeamTotal(adminUtils, ...)` | Admin writes, with `SecurityAdminUtils` as proof of an admin |
| `setLimit(connection, key, limit, by)` | Package-private write, in the caller's transaction |
| `readJson(json)`, `normalize(limit)`, `findProblem(limit, key, costLimitsAvailable)`, `requireValid(...)` | Parsing and validation |
| `version()` | The change version the guard caches paths by |

**Writes go through permission batches.** `setLimit` is package-private, so a
limit on a grant is written only by
[PermissionBatchUtils](../../src/prerna/auth/utils/PermissionBatchUtils.java),
in the same transaction as the grant it belongs to (see
[who sets limits](who_sets_limits.md)). `setLimit` deletes every row of the key
and inserts one new row with its rules, keeping who first added the limit. A
`null` limit deletes the key and writes nothing. Writing a typed Direct Limit
also replaces that user's untyped one.

**The change version.** Each server keeps a counter in memory. A write commits
first, then calls `markChanged()`, which moves the version. The guard caches
each person's path under the version it was read at and reads it again as soon
as the version differs, so a change applies on the next call on that server.
Other servers of a cluster read it within 30 seconds, when their cached path
expires.

**Finding a Direct Limit.** `findDirectLimit` looks up the typed limit first,
then the untyped one. With no type asked, it returns the untyped limit, or the
only Direct Limit the user id has. When the user id has typed limits on several
logins and none is untyped, it returns null, since it cannot tell which one
applies.

**One row per key.** Should two rows ever exist for a key, a read returns the
one changed last (by `DATEMODIFIED`, then by row id, so every server picks the
same one), and the next write of the key replaces both.

## Validation: `findProblem`

`findProblem(limit, key, costLimitsAvailable)` returns why a limit cannot be set
at a key, or null when it can. `requireValid` normalizes the limit and throws the
problem as an `IllegalArgumentException`.

| Rule | Message |
| --- | --- |
| Unlimited has no rules | "An Unlimited limit has no rules" |
| A ceiling or the General Platform Limit cannot be Unlimited | "... cannot be Unlimited. Remove its limit to leave it without one" |
| Every rule has a metric, a window and counts | "Each rule needs a metric, a window and whose use it counts" |
| A window length is in range | "A minute window is 1 to 59 minutes long ..." |
| Amounts are 1 or more | "Each rule's amount is 1 or more" |
| `SHARED` only on a team, public use and the Model Total | "Only a team, public use and a model's total can have shared rules" |
| A ceiling has shared rules only | "... has shared rules only" |
| Cost on a model needs known prices | "This model has no known prices, so its limits cannot count cost" |
| One rule per metric, window, length and counts | "A limit has only one rule for each metric, window and whose use it counts" |

Limits across all models (`engineId` null) can always have cost rules; they
count only the models with known prices
([cost and prices](cost_and_prices.md#where-cost-counts)).

## Column migration

[ModelUsageLimitColumnMigration](../../src/prerna/auth/utils/ModelUsageLimitColumnMigration.java)
runs at startup on a security database that still has the earlier limit
columns on `ENGINEPERMISSION` and `SMSS_USER`. It copies each row's limit into
`MODELUSAGELIMIT` as one `EACH_PERSON` rule (an `ALL_TIME` frequency becomes
`YEAR`), in one transaction that locks the rows so two servers starting together
copy once, then drops the columns. A limit already set on a key is kept and not
overwritten by the copy.
