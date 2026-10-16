# Cost and Prices

Cost rules count what calls cost, in millionths of a US dollar (micro-dollars).
[ModelUsageCost](../../src/prerna/engine/impl/model/usage/ModelUsageCost.java)
picks the price a model is billed at, works out what a call cost, and estimates
the most a call can cost before it runs.

Rates are US dollars per million tokens, the models.dev convention, so tokens
times a rate is a cost in micro-dollars: the unit of every cost amount, counter and
`COST_MICROS` log column.

## Which price a model is billed at

A model's prices are `MODELMETADATA.PRICING`: a JSON array with one entry per
serving provider and model id. The model catalog fills it, and owners can edit it.

```json
[ { "servingProvider": "anthropic", "modelId": "claude-sonnet-4-5",
    "input": 3, "output": 15, "cache_read": 0.3, "cache_write": 3.75 },
  { "servingProvider": "amazon-bedrock", "modelId": "anthropic.claude-sonnet-4-5",
    "input": 3.3, "output": 16.5 } ]
```

`priceEntry` picks one entry, in this order:

1. **The serving provider's entry.** The serving provider is
   `MODELMETADATA.SERVINGPROVIDER`, else the smss `PROVIDER`, else the smss
   `MODEL_TYPE` ([ModelServingProvider](../../src/prerna/util/ModelServingProvider.java)).
2. **Else the model maker's entry** (`MODELPROVIDER`), such as the Anthropic price
   of a Claude model served through Vertex.
3. **Else the first entry**, which a catalog list keeps as the maker's.

Provider names are normalized before they are compared. When a provider lists
several entries, the one whose model id matches the engine's is used (exactly, or
without a path and version tag, so `anthropic/claude-opus-4-8@default` matches
`claude-opus-4-8`); otherwise its first. An entry counts only with an `input` and
an `output` rate. A model has **known prices** when such an entry is chosen; a
model without them has no cost to count.

Each model's entry is cached for a minute, and saving its metadata clears the
cached entry on that server.

## What a call costs

When a call is billed, `costMicros` prices what the response reports. Prompt tokens
include cache reads and writes, and response tokens include thinking, so the
formula splits them rather than adding to them:

```
uncached input = prompt tokens - cache read tokens - cache write tokens
visible output = response tokens - thinking tokens

cost = uncached input   * input
     + cache read       * cache_read    (input when there is no cache_read rate)
     + cache write      * cache_write   (input when there is no cache_write rate)
     + visible output   * output
     + thinking         * reasoning     (output when there is no reasoning rate)
```

- **Tiers.** A prompt over a tier's size is priced at that tier's rates: the
  largest `tiers` entry the prompt is over, else `context_over_200k` above 200,000
  tokens. The tier's rates apply to the whole call, as providers bill it; a rate
  the tier does not list stays at its base price. The prompt size counts every
  input token, cache reads and writes included.
- **Rounded up** to a whole micro-dollar, so every billed call costs something.
- **Exact decimals.** Rates are read as `BigDecimal` through their text, so `0.3`
  is exactly 0.3, and large token counts do not overflow.
- **Embeddings** are priced per input token. **Batch** calls are priced at the
  normal rates. Audio tokens are priced at the text rates.

**Example.** Input $3, output $15, cache read $0.30, no reasoning rate. A prompt of
10,000 tokens, 6,000 of them read from the cache, and a response of 2,000 tokens,
500 of them thinking:

| Part | Tokens | Rate | Micro-dollars |
| --- | --- | --- | --- |
| Uncached input | 4,000 | 3 | 12,000 |
| Cache read | 6,000 | 0.30 | 1,800 |
| Visible output | 1,500 | 15 | 22,500 |
| Thinking | 500 | 15 | 7,500 |
| **Total** | | | **43,800** ($0.0438) |

## The most a call can cost

Before a call runs, `estimateMicros` prices it the dearest way it could bill, for
its reservation:

- the prompt estimate at the higher of `input` and `cache_write`, since a client
  such as Anthropic's marks the prompt for caching itself;
- the most output the call can produce at the higher of `output` and `reasoning`;
- the tier picked by the prompt estimate.

With input $3, cache write $3.75 and output $15, a prompt estimate of 8,000 tokens
and an output maximum of 4,096 is estimated at 8,000 x 3.75 + 4,096 x 15 = 91,440
micro-dollars. What the call bills is usually less, and the difference goes back to
the counters when it settles. The Python client gets the rates too (`pythonRates`)
to fit the output in the cost left.

## Where cost counts

- **On one model**, a cost rule needs the model's known prices; validation refuses
  one otherwise ("This model has no known prices, so its limits cannot count
  cost"). `getPermissions` returns `costLimitsAvailable` for a model, so the editor
  shows Cost disabled before anything is staged.
- **Across models** (the User's Total Model Limit, the General Platform Limit and
  the Team Total), cost rules count the models with known prices; a model without
  them adds nothing to a cost rule. A token rule in the same limit counts every
  model.
- **The price at the time of the call.** The guard reads the price entry when it
  admits a call, and the ticket bills at that entry. The cost goes on the call's
  `MESSAGE` row as `COST_MICROS`. A later price change applies to later calls.
- **Filling a cost counter from the logs** adds the window's `COST_MICROS`, plus the
  rows logged before calls recorded a cost (no `USER_TYPE` and no `COST_MICROS`),
  priced per model at the model's base rates. No tier is used for those, since a
  window's prompt tokens added together would price every call as a long prompt.

## Who changes prices

[SecurityModelMetadataUtils](../../src/prerna/auth/utils/SecurityModelMetadataUtils.java):

- **Only owners and admins change `PRICING`.** An owner can be one through a team.
  Editors change every other metadata field ("Only the model's owners can change
  its prices").
- **Prices can be changed, not all removed** ("A model's prices cannot be removed,
  only changed").
- **The entry cost limits use cannot be removed**, only changed. It is matched on
  serving provider and model id. Any other entry can be removed, and owners can add
  an entry for any provider.
- **A catalog sync replaces prices only with prices of its own**, and never empties
  them.
