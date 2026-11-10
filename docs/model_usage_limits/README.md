# Model Usage Limits

Model usage limits cap how much a person can use a model engine: tokens, cost, or
compute time, over a minute, an hour, a day, a week, a month or a year. Every
model call goes through one guard, which checks the limits that apply to the
person, holds what the call may use while it runs, and bills what it used when
it ends.

## Concepts

| Term | Meaning |
| --- | --- |
| Limit | Unlimited, or a list of rules. A call must fit in every rule of a limit. |
| No Limit Defined | No limit is set at a source, so the next source is tried. Different from Unlimited, which stops the search. |
| Rule | A metric, an amount, a window (with an optional length) and whose use it counts: each person, or shared. |
| Source | A place a limit is set, such as a person's grant on a model. One source is billed for each call. |
| Ceiling | A cap checked on every call, on top of the source billed: the Model Total and a team's Team Total. A ceiling is never the source billed. |
| Path | How limits apply to one person on one model: the ceilings, then every source in order, with the one billed. |
| Counter | What one source has used for one rule in one window bucket, with what running calls hold. |
| Reservation | What a running call holds on its counters, so calls at the same time cannot spend the same usage. |

## The sources, in order

A call is billed to the first source with a limit defined. The ceilings apply
whichever source is billed.

| | Source | Set on | Set by |
| --- | --- | --- | --- |
| Ceiling | Model Total | a model, for everyone together | the model's owners |
| Ceiling | Team Total | a team, across all models | admins |
| 1 | Direct Limit | a person's grant on a model | the model's owners |
| 2 | Team Limit | a team's grant on a model | the model's owners |
| 3 | Public Use Limit | a public model, for people with no direct or team limit | the model's owners |
| 4 | User's Total Model Limit | a person, across all models | admins |
| 5 | General Platform Limit | everyone, across all models | admins |

A person in several teams with limits is billed to one team: Unlimited teams
first, then the team they got access through first. When that team has nothing
left, calls move on to the next team.

## How a call is checked

1. **Resolve** the person's path on the model (cached for 30 seconds).
2. **Read** every counter the path's rules use, for the bucket that holds now.
3. **Decide**: a ceiling with nothing left refuses the call; otherwise the first
   source with a limit defined is billed.
4. **Reserve** what the call may use on every token and cost counter it is
   billed on, all or none.
5. The Python client fits the call in what is left (`usage_ceiling`).
6. **Bill** what the call used when it ends, or **release** the reservation when
   it fails.

## Guide map

1. [Limits, rules and storage](limits_and_storage.md): the types, the tables,
   reading and writing, and validation.
2. [Which limit applies](which_limit_applies.md): the resolver, the source order,
   team billing order and step states.
3. [Who sets limits](who_sets_limits.md): permission batches, owners only,
   admin-only limits, the endpoints, and who can see a person's path.
4. [Enforcement](enforcement.md): the guard, counters, windows, reservations,
   the Python ceiling, and refusals.
5. [Cost and prices](cost_and_prices.md): the price a model is billed at, what a
   call costs, and who changes prices.

The REST contract is in the Monolith
[endpoint reference](https://github.com/SEMOSS/Monolith/blob/dev/docs/endpoints.md#model-usage-limits).

## Requirements

- **Inference logs.** Counters for day and longer windows are filled from the
  model-inference database when they are first read, and every call's usage is
  logged there with the source billed and its cost. See
  [internal databases](../platform_services/internal_databases.md#model-inference-database).
- **Redis on a cluster.** On a cluster with Redis turned on (`REDIS_ENABLED` or
  `SEMOSS_IS_CLUSTER_REDIS`), every server counts in the same Redis, so a limit
  holds across servers. A single server counts in memory, and so does a cluster
  without Redis, where each server then holds the whole limit.
- **Prices for cost rules.** A cost rule on a model needs the model's known
  prices; see [cost and prices](cost_and_prices.md).

## Source map

| Area | Source |
| --- | --- |
| Limit types | [ModelUsageLimit](../../src/prerna/auth/access/ModelUsageLimit.java), [ModelUsageLimitRule](../../src/prerna/auth/access/ModelUsageLimitRule.java), [ModelUsageLimitKey](../../src/prerna/auth/access/ModelUsageLimitKey.java), [ModelUsageLimitPath](../../src/prerna/auth/access/ModelUsageLimitPath.java) |
| Storage | [SecurityModelUsageLimitUtils](../../src/prerna/auth/utils/SecurityModelUsageLimitUtils.java) |
| Which limit applies | [ModelUsageLimitResolver](../../src/prerna/auth/utils/ModelUsageLimitResolver.java) |
| Setting limits | [PermissionBatchUtils](../../src/prerna/auth/utils/PermissionBatchUtils.java), [SecurityAccessPermissionBatchUtils](../../src/prerna/auth/utils/SecurityAccessPermissionBatchUtils.java), [EffectivePermissionUtils](../../src/prerna/auth/utils/EffectivePermissionUtils.java) |
| Enforcement | [ModelUsageGuard](../../src/prerna/engine/impl/model/usage/ModelUsageGuard.java), [ModelUsageEvaluation](../../src/prerna/engine/impl/model/usage/ModelUsageEvaluation.java), [ModelUsageTicket](../../src/prerna/engine/impl/model/usage/ModelUsageTicket.java), [ModelUsageRestrictionUtility](../../src/prerna/engine/impl/model/ModelUsageRestrictionUtility.java) |
| Counters and windows | [IModelUsageCounterStore](../../src/prerna/engine/impl/model/usage/IModelUsageCounterStore.java), [LocalModelUsageCounterStore](../../src/prerna/engine/impl/model/usage/LocalModelUsageCounterStore.java), [RedisModelUsageCounterStore](../../src/prerna/engine/impl/model/usage/RedisModelUsageCounterStore.java), [ModelUsageWindows](../../src/prerna/engine/impl/model/usage/ModelUsageWindows.java), [ModelUsageCounterScope](../../src/prerna/engine/impl/model/usage/ModelUsageCounterScope.java) |
| Cost | [ModelUsageCost](../../src/prerna/engine/impl/model/usage/ModelUsageCost.java), [SecurityModelMetadataUtils](../../src/prerna/auth/utils/SecurityModelMetadataUtils.java) |
| Python ceiling | [usage_ceiling.py](../../py/genai_client/usage_ceiling.py) |
| Tests | [usage tests](../../test/prerna/engine/impl/model/usage/), [SecurityModelUsageLimitUtilsUnitTests](../../test/prerna/auth/utils/SecurityModelUsageLimitUtilsUnitTests.java), [PermissionBatchUtilsUsageLimitUnitTests](../../test/prerna/auth/utils/PermissionBatchUtilsUsageLimitUnitTests.java) |
