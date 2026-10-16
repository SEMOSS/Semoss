# Who Sets Limits

Limits on a model are set through permission batches, the single write path for
every permission change. The three limits that span models are admin only.
Nobody is exempt by role: an owner who wants no cap sets their own grant to
Unlimited.

## Who may set what

| Limit | Set through | Who |
| --- | --- | --- |
| Direct Limit | `usageLimit` on a person's grant on the model | the model's owners; admins in admin mode |
| Team Limit | `usageLimit` on a team's grant on the model | the model's owners |
| Public Use Limit | a `PUBLIC` change on the model | the model's owners |
| Model Total | a `MODEL` change on the model | the model's owners |
| User's Total Model Limit | `SecurityAdminUtils.editUser` (`model_usage_limit`), `registerUser`, `addNativeUser` | admins |
| Team Total | `SecurityModelUsageLimitUtils.setTeamTotal` | admins |
| General Platform Limit | `SecurityModelUsageLimitUtils.setPlatformDefault` | admins |

An owner can be one through a team: `CurrentPermissionLevel.of` counts the
user's own grant and the grants of every team they are in. Admin entry points
take a `SecurityAdminUtils`, which `SecurityAdminUtils.getInstance(user)` returns
only for an admin, as proof.

Editors add people and move them between Read-Only and Editor, and their level
changes keep the person's limit. They see every limit read-only, with "Only
owners can set usage limits". `getPermissions` returns
`allowedPermissionChanges.limitLevels` and `canChangeModelLimits`, so the UI knows
before a save which limits the viewer can change.

## A limit on a grant

[PermissionBatchUtils](../../src/prerna/auth/utils/PermissionBatchUtils.java)
validates every grant change on a model engine against its usage limit:

1. Work out the limit the change leaves. A removal leaves none, since the limit
   belongs to the grant.
2. A limit left as it is writes nothing and passes for anyone. This is how an
   editor's change of level goes through.
3. Any other limit needs an owner, or admin mode (`OWNERS_ONLY`).
4. The limit must be valid where it is set (`findProblem`, see
   [storage](limits_and_storage.md#validation-findproblem)).

The grant and its limit are written in one transaction. After the commit,
`markChanged()` moves the change version so the guard reads paths again.

**What a change without `usageLimit` means** depends on the entry point:

| Entry point | A change without `usageLimit` | An add without `usageLimit` |
| --- | --- | --- |
| Permission batch (JSON) | no limit defined: the grant is the whole new grant | no limit |
| `editEngineUserPermission` (form without a `usageLimit` key) | keeps the limit | |
| `editEngineUserPermissions` (a row without the key) | keeps the limit | |
| `addEngineUserPermission(s)` | | no limit |
| Team grants, and the admin versions of the above | keeps the limit | no limit |

A batch change that keeps a limit sends the current limit back; the Bulk Access
Management page does this for every staged grant. Legacy BI calls the engine
endpoints without `usageLimit`, so its adds give no limit and its edits keep the
limit.

## The Public Use Limit and the Model Total

These are not grants. A batch changes them with a `PUBLIC` or `MODEL` subject: a
`CHANGE` on a model, whose `to` carries only `usageLimit`:

```json
[{ "target": { "resourceType": "ENGINE", "id": "<model id>" },
   "changes": [{ "action": "CHANGE",
                 "subject": { "category": "PUBLIC" },
                 "to": { "usageLimit": { "unlimited": false, "rules": [
                     { "metric": "TOKENS", "amount": 50000, "window": "DAY", "counts": "EACH_PERSON" } ] } } }] }]
```

Only the model's owners and admin mode can change them, checked before anything
else. The Model Total takes shared rules only. Making a model public is a
separate change (`setEngineGlobal`); a Public Use Limit set on a private model
applies once the model is public.

## A person's own grant, and removals

- Someone who is not an owner cannot give themselves access or change their own
  grant; they can only leave. An owner can set their own limit.
- Removing a grant deletes its limit. A person's grant is kept by user id, so its
  Direct Limits on every login type are deleted with it.
- Only owners, and admin mode, take away the access of someone with a usage limit
  or give them an earlier end date, including someone leaving on their own
  (`ENDS_LIMITED_ACCESS`, "Only owners can end or shorten the access of someone
  with a usage limit"). Editors still remove Read-Only and Editor people who have
  no limit defined.

Refusals reach older callers in the shapes they already handle:
`PermissionErrorRules` maps each problem code to the exception and message of the
engine method that raised it.

## Reading a person's path

[SecurityAccessPermissionBatchUtils](../../src/prerna/auth/utils/SecurityAccessPermissionBatchUtils.java):

| Method | Who | What |
| --- | --- | --- |
| `getMyUsageLimitPath(user, engineId)`, `getMyUsageLimitPaths(user)` | anyone who can use the model | their own path, in full |
| `getUsageLimitPath(user, engineId, userId, userType)` | the model's editors and owners | the path of someone with a grant on the model, their own or a team's; the login type is required |
| `getUsageLimitPath(adminUtils, user, ...)` | admins | anyone's path on any model, in full |

For editors and owners, what the person may use elsewhere stays private: the User's
Total Model Limit and General Platform Limit steps come with `rulesHidden` and no
rules, and team steps have no `teamTotal`. The model being public is not enough to
read someone's path.

## Effective Permissions

[EffectivePermissionUtils](../../src/prerna/auth/utils/EffectivePermissionUtils.java)
adds each person's usage to a model's Effective Permissions:

- `usage.billedTo`: the source billed (`limitType`, `id`, `type`), or null;
- `usage.unlimited` and `usage.rules`;
- `usage.nextTeams`: the teams calls move on to;
- `usage.notApplied`: the sources that do not apply, each with a reason
  (`DIRECT_WINS`, `TEAM_WINS`, `UNLIMITED_TEAM_WINS`, `PUBLIC_WINS`,
  `ALL_MODELS_WINS`, `NOT_PUBLIC`);

and, for the model, `publicUse` (`isPublic`, `usageLimit`) and the `ceilings`.

## Audit trail

Every change to a usage limit is recorded in the audit trail as a
`USAGE_LIMIT_UPDATE` event, in the `USAGE_LIMIT` category (both from
[AuditEventType](../../src/prerna/usertracking/AuditEventType.java), the list of
every event the audit trail records), by
[ModelUsageLimitAudit](../../src/prerna/auth/utils/ModelUsageLimitAudit.java):

| Field | Holds |
| --- | --- |
| `oldValue`, `newValue` | `{"usageLimit": ...}`: the limit before and after, null for no limit defined |
| `details.limitType` | `DIRECT`, `TEAM`, `PUBLIC`, `MODEL_TOTAL`, `TEAM_TOTAL`, `ALL_MODELS` or `PLATFORM`, with `groupId` and `groupType` for a team's limit |
| target | the model for a limit on a model; the user for a User's Total Model Limit; the team for a Team Total; `PLATFORM` for the General Platform Limit |
| subject | the person, for a Direct Limit or a User's Total Model Limit |

- Limits on a model are recorded after their permission batch commits, one event
  per limit a change sets, changes or removes, even when an admin method records
  the access changes as one summary. A grant's removal records its limit removed
  with it (`details.removedWithGrant`).
- The limits admins set across models are recorded after they are saved: the
  General Platform Limit (at high severity), a Team Total, and a person's User's
  Total Model Limit, whether set when editing or registering the user.
- A grant that has ended is removed when the person next reaches the model, and
  its Direct Limits are recorded as removed with it, by SEMOSS
  (`details.reason` is `EXPIRED`).
- A change that leaves a limit as it is records nothing.

Admins see every event on the admin audit trail. A model's owners see the limit
changes on that model on its Audit tab
([ResourceAuditReadService](../../src/prerna/usertracking/ResourceAuditReadService.java)):
the limit before and after and its details, and nothing else from the event.
Admins also see the limit changes, beside the access changes and the denied
requests, on the Access & Limits tab of Enterprise Usage & Activity: the `audit`
source of
[EnterpriseUsageUtils](../../src/prerna/auth/utils/EnterpriseUsageUtils.java)
counts them, lists them by the shared date, user, app and engine filters, and
reads one event's old and new values when it is opened.

## Endpoints

The Monolith endpoints for paths, the platform default, team totals and the
`usageLimit` parameters of the engine and user endpoints are listed in the
[endpoint reference](https://github.com/SEMOSS/Monolith/blob/dev/docs/endpoints.md#model-usage-limits).
