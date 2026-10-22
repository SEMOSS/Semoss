# Enforcement

[ModelUsageGuard](../../src/prerna/engine/impl/model/usage/ModelUsageGuard.java)
checks every model call against the person's [path](which_limit_applies.md),
holds what the call may use while it runs, and bills what it used. The engines
reach it through
[ModelUsageRestrictionUtility](../../src/prerna/engine/impl/model/ModelUsageRestrictionUtility.java).

## Where calls are checked

| Call path | Entry |
| --- | --- |
| `AbstractModelEngine.askRoom` and `embeddings` | `ModelUsageRestrictionUtility.admit`, then `bill` or `billEmbeddings` |
| `AbstractPythonModelEngine` multi modal embeddings | `admit` |
| `TypeSafeEngine.evaluate`, `EmbeddedModelEngine` | `admit` |
| Batch submission and results | `ModelBatchManager.admitBatch`, then each result billed as it comes in |
| Streaming through the OpenAI, Anthropic and Ollama endpoints | `ModelUsageGuard.check` before the stream starts |

A call with no user, made by SEMOSS itself, has no limits. A call whose path has
no rules anywhere never touches the counters; it is only told which source it is
billed to.

## A call's life

```
ModelUsageGuard.admit(user, engine, request)          before the call starts
  1. resolve   the person's path on the model, cached for 30 seconds
  2. read      every counter the path's rules use, for the bucket that holds now
  3. decide    a ceiling with nothing left refuses; else the first source with a
               limit defined is billed (a team with nothing left passes the call on)
  4. reserve   on every token and cost counter the call is billed on, all or none
  -> ModelUsageTicket

the call runs, given what is left as usage_ceiling

ticket.bill(report)     the call ended: its reservation becomes what it used
ticket.release()        the call failed: its reservation is given back
```

A ticket settles once: the first of `bill` and `release` wins, and the other does
nothing, so a call is never billed twice.

The path cache is keyed by primary login and model. A path is read again as soon
as `SecurityModelUsageLimitUtils.version()` changes on that server, and within 30
seconds on the other servers of a cluster.

## Counters

A counter is one **scope**, one rule's metric and window (with its length), and
one window bucket. The scope says whose use it counts
([ModelUsageCounterScope](../../src/prerna/engine/impl/model/usage/ModelUsageCounterScope.java)):

| Scope | Counts | For |
| --- | --- | --- |
| `PERSON_MODEL` | All of a person's use of one model, whichever source is billed | the Direct Limit, and the Public Use Limit's each-person rules |
| `TEAM_SHARED` | The calls billed to a team on one model | a Team Limit's shared rules |
| `TEAM_MEMBER` | The calls billed to a team on one model, by one member | a Team Limit's each-member rules |
| `PUBLIC_SHARED` | The calls billed to a model's public use | the Public Use Limit's shared rules |
| `TEAM_TOTAL` | The calls billed to a team, on any model | the Team Total |
| `MODEL_TOTAL` | Every call the model answers | the Model Total |
| `PERSON_ALL_MODELS` | A person's use of every model | the User's Total Model Limit and the General Platform Limit |

Rules that share a scope, metric, window and length share a counter. A Direct
Limit and the Public Use Limit's each-person rule of the same measure use the same
`PERSON_MODEL` counter, so a person moved from public use to a direct limit keeps
the day's use.

A call adds to:

- the ceilings, such as the Model Total, always;
- the source billed and, for a team, its Team Total;
- the person's own counters on the model (`PERSON_MODEL`) and across all models
  (`PERSON_ALL_MODELS`), whichever source is billed.

A router model's own call skips the counters that span models, since the model it
routes to counts the call there, so a routed call counts once.

## Windows

[ModelUsageWindows](../../src/prerna/engine/impl/model/usage/ModelUsageWindows.java)
does the time math, in UTC.

- **Day, week, month and year** are fixed calendar windows. A rule counts the
  bucket that holds now and resets when it ends.
- **Minute and hour** windows slide. A rule counts the current bucket plus the
  share of the previous bucket still inside the window, weighted by how much of
  the current bucket is left, rounded up. Buckets line up with the epoch, so a
  10-minute window's buckets start on the hour and every 10 minutes after.
  `resetsAt` for a sliding rule is when enough of the previous bucket has slid out.

**Example.** A rule of 2,000 tokens every hour. At 14:20 UTC, the 14:00 to 15:00
bucket has 500 used and the 13:00 to 14:00 bucket had 1,200. The previous bucket's
weight is 40 / 60, so 800 carries over, 1,300 is counted, and 700 is left.

A fixed bucket's counter is kept until a day after the bucket ends, and a sliding
bucket's until one window and a minute after, since it is then the previous bucket
of the next window.

## Check and reserve

**Check.** For every rule, `counted = used + reserved + carried`. A rule has usage
left while `counted < amount`. A ceiling with nothing left refuses the call. A
source other than a team refuses it when it has nothing left; teams pass it to the
next team in the billing order.

**Reserve.** For tokens and for cost:

- **what is left** is the least `amount - counted` across every rule the call must
  fit in: the ceilings, the source billed, and a billed team's Team Total;
- **the estimate** is the prompt estimate plus the most output the call can
  produce: the caller's `max_tokens`, else the model's maximum, else 8,192. Cost is
  estimated at the dearest rates the call can bill at
  ([cost and prices](cost_and_prices.md#the-most-a-call-can-cost));
- **the reservation** is the lesser of the two, held on every token and cost
  counter the call is billed on.

The store checks every hold against its rule's amount and adds them all at once,
or none. When another call took what was left between the read and the reserve,
the guard reads again, up to three times, then refuses the call as one that does
not fit. Calls running at the same time therefore see each other's holds.

A reservation expires after 15 minutes for a call and 25 hours for a batch, so a
call lost with its server stops holding usage. Compute time is checked before the
call and counted when it ends; it is not reserved.

## The Python ceiling

The ticket's `pythonCeiling()` goes to the Python client as the `usage_ceiling`
keyword argument
([usage_ceiling.py](../../py/genai_client/usage_ceiling.py)):

```
usage_ceiling = {
    "tokens": 120000,             # what is left, prompt and output; or None
    "cost_micros": 2500000,       # what is left of cost, millionths of a dollar; or None
    "rates": { ... },             # dollars per million tokens, when cost_micros is set
    "max_output_tokens": 8192,    # the output maximum the call is reserved for
    "min_output_tokens": 256,     # refuse when less than this is left for output
    "margin": 0.05,               # kept free when the prompt count is an estimate
}
```

The client counts the prompt as it will be sent, then:

- refuses the call before the provider sees it when less than `min_output_tokens`
  is left for output (for embeddings, when the input does not fit), with the error
  reason `USAGE_LIMIT_DOES_NOT_FIT`; nothing is billed;
- otherwise lowers the output maximum to fit, never above the caller's or the
  model's, and lowers the thinking budget below it (or turns thinking off when even
  the provider's smallest budget does not fit);
- marks the response `usageCapped` when it stopped at a maximum the ceiling set.

A router model passes its ceiling on, and the model it routes to fits the call in
the lesser of both.

## Bill and release

`bill` replaces the whole reservation with what the call used, in one store
operation; the difference goes back to the counters. Usage goes in the bucket of
the moment of billing. A settle that fails is logged rather than thrown, since the
call has already run, and its reservation expires on its own.

**Batches** reserve for every request at submission. As results come in,
`billPart` bills each one and gives back that much of the reservation; the rest is
released when the batch ends. Results can be read many times, on any server, so
each is billed and logged once: by a mark in the counter store, and by its RESPONSE
row in the inference logs.

## Filling a counter from the logs

The first time a counter for a day or longer window, or a sliding window of an
hour or more, is read, it is filled from the inference logs' `MESSAGE` rows for
that bucket: a `SUM` of tokens, of `COST_MICROS`, or of response time. The counter
keeps the larger of its own count and the logs', and is marked filled so no other
server fills it again. One read of the logs runs at a time for a counter; when the
logs cannot be read, the call goes on with what the counter holds, and the logs are
tried again after a minute. Shorter windows start at 0.

## Counter stores

| Store | When | How |
| --- | --- | --- |
| [LocalModelUsageCounterStore](../../src/prerna/engine/impl/model/usage/LocalModelUsageCounterStore.java) | a single server, or a cluster without Redis turned on | in memory; every method is synchronized, so checking and holding several counters is one step |
| [RedisModelUsageCounterStore](../../src/prerna/engine/impl/model/usage/RedisModelUsageCounterStore.java) | a cluster with Redis turned on | each counter is a Redis hash (`u` used, `s` filled, `r<minute>` holds by expiry minute); Lua scripts read, reserve and settle several counters at once, and every key shares one hash tag so a script can touch them together on Redis Cluster |

When the counters cannot be reached, a call that a rule could refuse is refused as
unavailable: "Usage limits cannot be checked right now". A call no rule can refuse
runs, and is not counted.

## Refusals and responses

A refusal is a
[ModelUsageLimitException](../../src/prerna/engine/impl/model/usage/ModelUsageLimitException.java)
with the source's `limitType`, whether it is a minute or hour rule
(`isRateLimit()`), when it resets (`getResetsAt()`, ISO 8601 UTC), and whether it
is unavailable. Its message names the limit and when it resets, such as "You have
used your 100,000 tokens a day on GPT-5. It resets at ...", and a team refusal
names only teams the caller is in.

The OpenAI, Anthropic and Ollama endpoints answer a refusal in the provider's
error shape:

| Refusal | Status | `code` | `Retry-After` |
| --- | --- | --- | --- |
| A minute or hour rule | `429` | `rate_limit_exceeded` | the seconds until it resets |
| A day or longer rule | `429` | `usage_limit_reached` | none |
| Limits cannot be checked | `503` | `usage_limits_unavailable` | none |

A model response carries `usageLimits`: the source billed, its rules with `used`
and `resetsAt`, the ceilings, and `capped` when the answer stopped at the ceiling's
output maximum. It also carries `usageRestriction` (`restrictedBy`, `currentValue`,
`maxValue`) for the rule closest to its amount.
