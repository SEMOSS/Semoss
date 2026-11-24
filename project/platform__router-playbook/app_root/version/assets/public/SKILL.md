---
name: router-playbook
description: Use when running as the router-optimizer agent to propose one better version of a self-improving model router (mode subtask). Covers reading evidence, making one change, evaluating it on a saved set of past tasks against the live router and the strongest model, and applying it with ApplyRouterCandidate. Do not use to edit a live router by hand.
---

# Router improvement playbook

The task names the live router engine id (`LIVE`), a judge model id, a TRAIN task set id (`TRAIN`), a VALIDATION task set id (`VAL`) and the `jobCostCapMicros` to pass to every `EvaluateRouterCandidate` call. The sets are built by the platform from the router's own recorded runs (the tasks are replays of real runs). A run counts as good when its result is at least as good as the strongest model's result on the same task (the judge compares readable answers and output files side by side; file equality is not a verdict), so quality is agreement with the strongest route, 0 or 1 per run; use the given ids only. TRAIN is where you look for gaps and write skills; VAL is only used to decide whether to apply; never write a skill from VAL results. One run applies at most one candidate. Stop early when a budget is tight.

## Procedure

1. **Evidence.** `GetRouterEvidence(engineId=LIVE, sample=8)`. Read `cells` (subtask x served model: steps, `avg_cost_micros_per_priced_step`, `failed_run_share`, `thumbs_down`), `by_version` and `step_sample` (per subtask: the goal, previous subtask, current tool names and statuses, run status and cost of real routed steps). Defer to the platform's native learning trigger and the supplied replay-safe TRAIN/VAL readiness; do not impose a separate `routed_steps` minimum.
2. **Live config and skill library.** Read `GetModelRouterConfig(engine=LIVE)`. Prior skill IDs are in the live subtasks; use `ReadSkillFile(project=<skill id>, filePath="SKILL.md")` to read the current guidance. Never edit a skill linked to a router version.
3. **Mine step types.** Read the `step_sample` entries across all subtasks. Group steps by what the step is trying to do (read inputs and plan, edit or write a file, compute or look up a value, check the result, answer in text). A subtask may also be defined by the KIND OF GOAL the step belongs to (the `goal` text is part of what the classifier sees at every step), for example goals that edit an office file versus goals answered in plain text. Propose 3 to 8 subtasks, each with an `id`, a one-line definition and 2 or 3 observable `recognition_rules` (conditions on the goal, previous subtask and current tool names and statuses, not topics). With a single generic subtask the router cannot use a cheap model for any part of a task.
4. **Assign routes.** Put every recognisable subtask whose steps are routine (reading, listing, formatting, lookups) on the cheapest route. Keep a strong route only where evidence shows the cheap route fails. The goal is cost down at quality held, and skills are what make a cheap route good enough.
5. **Create the initial private candidate.** `WriteRouterCandidate(liveEngineId=LIVE, map=...)` with the mined subtasks, routes and a new `version` (for example `YYYY-MM-DD.n`); it returns `candidateEngineId` (CLONE). Use it for the initial TRAIN evaluation. A later skill or config correction requires a replacement candidate; do not try to edit a clone with an unavailable tool.
6. **Evaluate the candidate on TRAIN.** `EvaluateRouterCandidate(candidateEngineId=CLONE, taskSet=TRAIN, liveEngineId=LIVE, judgeModelId=<given>, jobCostCapMicros=<given>)`. The strongest model runs the tasks first as the reference and each task uses the configured repeat count, so scores are means across its runs. Read `failure_digest`: tasks where the candidate was worse than the strongest model, worst first, with the reason, `candidate_trace` and `reference_trace`. Write skills only from entries with score 0 (worse in every repeat). An empty digest with a candidate that sends every subtask to the strongest route is not a lack of gaps: stop rather than creating another clone.
7. **Learn from TRAIN and write a replacement candidate.** Use the TRAIN traces to write general guidance for the failing subtask, incorporating useful guidance from its existing skill if any. Always call `CreateSkill` with a new revision name; never change prior skill content. Set the returned `skill_id` in the candidate map and use `WriteRouterCandidate` with a new config version. Do not copy task answers or values into skills.
8. **Re-evaluate the replacement on TRAIN, then VAL.** Run `EvaluateRouterCandidate(candidateEngineId=CLONE, taskSet=TRAIN, liveEngineId=LIVE, judgeModelId=<given>, jobCostCapMicros=<given>)` on the replacement after attaching the skill. If TRAIN is incomplete, stop. If its digest identifies another actionable correction, refine the skill/config and repeat step 7 with a new CLONE, then rerun TRAIN. Once the candidate is settled and TRAIN is complete, evaluate that unchanged CLONE on VAL once with the same parameters except `taskSet=VAL`; keep this fresh `evaluationId` and read `decision`. Do not change CLONE after VAL; its evaluation is bound to that candidate config and live version.
9. **Apply.** `ApplyRouterCandidate(liveEngineId=LIVE, candidateEngineId=CLONE, evaluationId=...)`. The platform independently checks that the current candidate and live version match the evaluation and that complete evidence meets the quality/cost gate. If refused, report the reason and keep the live version.
10. Finish with a short report: the subtasks you mined (id, definition, route), the TRAIN digest size, the skills written (names), the VAL numbers with their noise and the apply result.

## What a good skill looks like

Imperative lines for one step type, general for any task of that type, built from what the traces show: the exact output format or filename the task asked for, the check the strong run made before finishing, the single tool call that replaces three, the mistake the cheap run repeated. No task-specific values.

## Rules

- The live router changes only through `ApplyRouterCandidate`.
- Never put a figure in a skill that you did not read from evidence or a replay.
- One applied candidate per run.
- Evaluate only on the given TRAIN and VAL sets, never on tasks you chose yourself; skills come from TRAIN only.
