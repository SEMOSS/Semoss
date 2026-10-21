# Which Limit Applies

[ModelUsageLimitResolver](../../src/prerna/auth/utils/ModelUsageLimitResolver.java)
decides, for one person on one model, which limits apply and in what order. It
reads the security database and returns a
[ModelUsageLimitPath](../../src/prerna/auth/access/ModelUsageLimitPath.java). It
does not count usage or refuse calls; the [guard](enforcement.md) does that with
the path.

## The person

A path is resolved for a `ModelUsageLimitResolver.Person`: the primary login, every
login (the primary first), and the teams of every login.

| Factory | Used for | Teams |
| --- | --- | --- |
| `personOf(User)` | a signed-in user making a call, or reading their own limits | custom teams from `CUSTOMGROUPASSIGNMENT`, and login provider teams (such as Microsoft groups) from each login's token |
| `storedPerson(userId, type)` | someone else, by id and login type, such as on Effective Permissions | custom teams only, since login provider teams are known only from the person's own sign-in |

Anonymous users cannot use models; `personOf` throws for them. Memberships that
have ended are left out. Usage is counted under the primary login, while grants
and limits on every login count.

## What is read for the model

The resolver reads, once per model:

- whether the model is public (`ENGINE.GLOBAL`);
- who has a direct grant that has not ended (`ENGINEPERMISSION`);
- which teams have a grant that has not ended, and since when
  (`GROUPENGINEPERMISSION`);
- every limit set on the model.

`resolveStored(engineId, people)` reads the model once and resolves many people
against it, which Effective Permissions uses for its list of people.

## The order

The path starts with the ceilings, then lists every source:

```
CEILINGS   Model Total (and, on each team step, that team's Team Total)
SOURCES    1. Direct Limit             the first login with a grant whose limit is defined,
                                       else the first login with a grant
           2. Team Limits              teams with a grant on the model, in billing order
           3. Public Use Limit         skipped (NOT_PUBLIC) when the model is not public
           4. User's Total Model Limit the first login with one defined, primary first
           5. General Platform Limit
```

The source billed is the **first one with a limit defined**: rules or Unlimited.
An Unlimited source stops the search the same way rules do, so the person is
uncapped except for the ceilings. With no source defined, no limit applies and
only the ceilings are checked.

**Example.** Model `gpt5` is public, with a Public Use Limit of 50,000 tokens a
day. Erin has no direct grant, is in team `analysts` (a grant on `gpt5`, no limit
defined), and has a User's Total Model Limit of $20 a day.

| Step | Result |
| --- | --- |
| Model Total | nothing to check |
| 1. Direct | SKIPPED, `NO_GRANT` |
| 2. Team `analysts` | NOT_DEFINED |
| 3. Public Use | **billed**: 50,000 tokens a day |
| 4. User's Total Model Limit | NOT_REACHED |

## Team billing order

When more than one team has a limit defined, they are sorted:

1. Unlimited teams first;
2. then by **access since**: the later of when the person joined the team and when
   the team got its grant on the model;
3. then by team id.

The guard bills the first team in that order that has usage left, both in its
limit on the model and in its Team Total. A team with nothing left passes the
call to the next one, which the path marks NEXT. The call is refused only when no
team has usage left.

**Example.** Frank is in three teams with grants on `claude`:

| Team | Limit | Joined | Team's grant | Access since |
| --- | --- | --- | --- | --- |
| `red` | 1M tokens a day | Jan 10 | Mar 1 | Mar 1 |
| `blue` | 2M tokens a day | Feb 5 | Jan 1 | Feb 5 |
| `green` | Unlimited | Jun 1 | Jun 1 | Jun 1 |

The order is `green`, `blue`, `red`. Frank's calls go to `green`; `blue` and `red`
are used only if `green` reaches its Team Total, or Frank leaves `green`.

## Step states

| State | Set by | Meaning |
| --- | --- | --- |
| `DEFINED` | resolver | has a limit defined (rules or Unlimited) |
| `NOT_DEFINED` | resolver | no limit defined here |
| `SKIPPED`, with `NO_GRANT` or `NOT_PUBLIC` | resolver | does not apply to this person or model |
| `NOT_REACHED` | resolver | defined, but an earlier source is billed |
| `CHECKED` | resolver, guard | a ceiling with a limit, checked on every call and with usage left |
| `BILLED` | guard | the source this call counts against |
| `NEXT` | guard | a team after the billed one, used when it runs out |
| `REACHED` | guard | nothing left: the call is refused here, or a team passes it on |

## Reading a path

[ModelUsagePathView](../../src/prerna/engine/impl/model/usage/ModelUsagePathView.java)
writes a path as JSON for the endpoints and the UI: `engineId`, `person`,
`billedLimitType`, `ceilings` and `steps`, where each step has its `layer`,
`limitType`, `id`, `type`, `state`, `reason`, `accessSince`, `unlimited`, `rules`
(with `used` and `resetsAt` when read with usage) and a team's `teamTotal`. Who may
read whose path is in [who sets limits](who_sets_limits.md#reading-a-persons-path).
