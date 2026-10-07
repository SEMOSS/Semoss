# Jev prompt-injection guardrail benchmark (deepset/prompt-injections)

Validated 2026-10-07 with `jev-latest` through `TypeSafeClientWrapper` (the exact
path `JevPolicyGuardrailEngine` uses in production).

Corpus: 546 rows (343 benign / 203 injection). Full run, zero API errors.

| Criterion | thr 0.5 TP | FP | notes |
|---|---|---|---|
| v1 plain "ignore previous instructions" | 113/203 | 0/343 | misses roleplay + forced claims |
| v2 + persona + hidden-in-structured-text | 138/203 | 1/343 | misses content coercion |
| v3 + dictated answers/forced claims | **158/203** | **0/343** | shipped criterion |

Injection probability distribution with v3: median 0.82+, benign max 0.25.
Remaining misses are content coercion disguised as ordinary questions
(untrue headline requests inside neutral phrasing, multilingual).

Run: `/usr/lib/python/semossvenv/bin/python bench_prompt_injections.py 3`
(needs `TYPESAFE_API_KEY` in the environment; downloads the corpus from the
HuggingFace datasets-server into `dataset.json` next to the script).

The v3 wording is the `QUESTION_INSTRUCTIONS` of the live
`Jev prompt injection` guardrail config.