# Self-improving model router

A MODEL_ROUTER engine in `subtask` mode can get cheaper over time at the same quality. Each agent step is classified into a
subtask (for example `edit_excel`) by a cheap model, sent to that subtask's route (a model connection) and given that subtask's
skill text. A `router-optimizer` agent proposes better versions from the router's own recorded runs, tests them by replaying real
tasks, and the live router changes only if the measured result passes one rule. Every version is a git commit and can be rolled back.

## Prerequisites

- The router must be editable by the administrator and configured in `subtask` mode. Replace the sample placeholders with model-engine IDs created in the recipient's own SEMOSS instance: one chat-capable classifier, a lower-cost route, and a strong route. The strongest route is `strong_route`, or otherwise the route whose model has the highest list price. Use an authorized chat-model connection for optimizer/judge work as the separate `optimizerModelId`; never copy another installation's IDs or credentials.
- The `router-optimizer` system agent and its `router-playbook` skill must be present after the matching backend assets are installed and seeded. Confirm both are available before opting in. A configured connection is not, by itself, proof that native judge comparisons or billing have been validated.
- Enable learning as an administrator from a non-agent context. Scheduled learning requires the platform scheduler and the creator's job permissions to be working.
- Automatic task construction only admits the documented replay-safe, single-turn tasks using workspace-local tools. Multi-turn work, approvals and external tools are not covered.

## Recipient quickstart: run first, learn later

Engine setup and agent model selection use the deployment’s existing Monolith/semoss-ui flows. With the matching SEMOSS UI, use the model router’s Learning card to configure learning without selecting a replay workspace; the same settings are available through Pixel. Use the [model engine guide](../engines/model_engines.md) for model connections and [Java developer onboarding](../development_guides/java_developer_onboarding.md) for the supported build/install path. For a branch build, use JDK 21 and Maven 3.9.x: build/install Semoss first, then build the matching Monolith WAR with the same `ci.version`; rebuilding core alone does not update a running server.

1. **Prepare model connections.** Create chat-capable model-engine connections in your instance for a classifier, a lower-cost route, and a strong route. Replace every placeholder in [the minimal router config](examples/router.json) with your own SEMOSS model-engine IDs. The router does not grant access to its backing engines: users and the router's execution context must be allowed to invoke the configured classifier and route engines under the deployment's access policy. Never copy model IDs or credentials from another installation.
2. **Create the router.** Register a `MODEL_ROUTER` engine using `prerna.engine.impl.model.ModelRouterEngine`, and load the example JSON as its `assets/router.json` (or configure the documented `ROUTER_CONFIG`/`ROUTER_CONFIG_JSON` SMSS properties). Use the instance's normal engine-management path; a backend code build does not create model connections or register this engine for you. The sample has no `improvement` section, so learning is off for the first run.
3. **Run the current-version report example with learning off.** Select the router as the agent/workspace model and submit the [synthetic report request](examples/status-report-request.txt). Independently reopen `status_report.md` using a workspace file-reading tool; do not accept the agent's completion claim as the check. Verify exactly the headings `Summary`, `Risks`, and `Next steps`; at most 120 words; 85% progress; `$36,000` remaining; Kofi as owner of the first risk; both risks (two engineers on leave and sales scope creep); and both dated milestones (Pilot go-live on the 4th and Training sessions on the 27th of next month). This is a current-branch acceptance check for your installation, separate from the historical comparison; run it before opting in to learning. For an owner/admin, inspect the assistant message's `routing_trace`: a useful trace has `trace_id`, `config_version`, `subtask_id`, `selection_source`/classifier status, and the actually served engine ID. Check each relevant model step; the final answer alone does not prove routing. A skill-bearing subtask also records `skill_id` and `skill_guidance_status`; `attached_to_dispatch` means the guidance part was added to the request, not that a provider acknowledged or followed it. The sample config has no skills, so `not_configured` is expected. If trace metadata is unavailable, resolve observability before claiming successful routing.
4. **Check status, then opt in.** An owner/editor can read `GetRouterLearningStatus(engine, withCosts)`; begin with `withCosts=false`. Only an administrator should enable learning, after confirming the seeded `router-optimizer` agent and `router-playbook` skill are available, the scheduler works, an authorized optimizer chat-model ID is selected, and cost limits are understood. Optimizer, replay and judge calls cost money. For example, run `SetRouterLearning(engine=["<YOUR_ROUTER_ENGINE_ID>"], enabled=[true], optimizerModelId=["<YOUR_OPTIMIZER_MODEL_ENGINE_ID>"], minNewTasks=[100], frequency=["off"]);` from an admin Pixel context to opt in to volume-triggered learning. The `minNewTasks` threshold counts traffic; it does not guarantee a learning attempt, because enough new traffic and enough eligible, replay-safe completed tasks are both required. Learning is not limited to one inference: the candidate process replays tasks and may call a judge. The documented `$30` evaluator admission target is not a shared cycle budget or provider hard cap, and in-flight work can exceed it. Verify status and scheduled jobs afterward. `frequency="off"` disables calendar triggers; it does not disable volume-triggered attempts.
5. **Operate and recover.** Editors can inspect status/history, pause or disable learning, and use `RollbackRouterConfig(engine, version)` to restore a recorded config as a new version; rollback also pauses learning. For a failed backend release, use the platform's controlled deployment rollback separately. Do not treat a config rollback as a Java/asset deployment rollback.

## Access is not inherited through the router

- **Ordinary users** need permission to use the router and its configured classifier/route engines. Router access does not itself grant access to a dependent model engine; verify the deployment's engine-level permissions for the actual execution path.
- **Skill projects** are separate dependencies. A user whose routed subtask names a skill must be able to view that skill project. Otherwise routing can continue without the instructions (`skill_guidance_status=access_denied`); grant only the required skill visibility and verify the trace. An `attached_to_dispatch` status means the text was appended to that model request, not proof of provider receipt or compliance.
- **Router owner/admin** access is separate from ordinary use. Enabling/disabling learning and updating router configuration are administrator operations. The scheduled optimizer job runs as its creator/owner; evidence is pooled for that owner across router users, and candidates/evaluations/skills belong to the owner. Editors can inspect status/history or roll back as allowed, but cannot enable learning unless they are admins.

The system agent and playbook are provisioned from repository assets by the backend seeder; confirm they exist in the target installation rather than asking recipients to copy private class files or local container artifacts. This repository contains the backend reactors and docs, not a dedicated frontend implementation for router learning/history/Undo. Exact engine-management and agent-selection screens depend on the separately deployed Monolith and semoss-ui versions.

## Admin flow

1. **Set up the router.** Create the model connections and a router engine in `subtask` mode: `routes[]` (cheap to strong),
   `classifier_engine` (a chat model), `subtasks[]` (`id`, `recognition_rules`, `route`, optional `skill_id`), `default_subtask`
   and a `version`. The strongest route is `strong_route`, else the route whose model has the highest list price.
2. **One switch.** `SetRouterLearning(engine, enabled, optimizerModelId, minNewTasks, frequency, maxQualityLoss, replay)`, admins only.
   It writes the router's `improvement` section and reconciles one learning job (`RunRouterLearningIfDue`).
   The switch saves the scheduler's returned ID as `improvement.scheduler_job_id`; do not supply it in config edits.
   Disabling removes the bound job. Changing frequency still replaces the job and saves its new ID.
   If a job is missing, enable learning again to recreate it. Before upgrading an RSI prototype, remove its old optimizer and watch jobs in the scheduler; jobs are no longer discovered by name.
   - `minNewTasks` (default 100): when a task starts on the router and this many runs were recorded since the last learning
     attempt (all users' runs), the router triggers the learning job. Runs sharing the optimizer attempt's Insight (the evaluator's
     own replay traffic) are excluded; ordinary task runs during an attempt still count. It looks at most once per 10 minutes.
   - `frequency` (`off`, `hourly`, `daily` (default), `weekly`): the learning job also runs on this clock. With `off` it runs
     only by volume.
   - `maxQualityLoss` (default 0, optional explicit compatibility override, in `improvement.max_quality_loss`): the maximum observed paired
     answer-quality loss (agreement with the strongest route, 0 to 1) a cheaper version may accept. The default refuses any observed held-out
     decline; explicit existing values remain supported. Unknown or incomplete evidence defers rather than promoting.

   The job starts one optimizer run when something is new, no run is going and the recorded runs make enough usable tasks.
3. **The rule.** The optimizer's candidate replaces the live version only if, on the same replayed tasks, (a) at least the configured number of validation tasks were
   compared (else the refusal says how many more are needed), (b) the observed paired quality change is not below `-maxQualityLoss`, and
   (c) it is cheaper by more than noise, or better by more than noise at a cost not higher by more than noise. With the default of zero,
   any observed held-out quality decline refuses a cost-saving promotion; the default does not claim universal statistical proof of no loss.
   Quality is agreement with the strongest route (see below): candidate and live scores (the share of configured runs that agree) are subtracted
   task by task. Noise is the standard error of those paired differences (and of costs); a saving or improvement counts only beyond 1.645
   standard errors (a one-sided 95% test), so noise only works against the candidate. `EvaluateRouterCandidate` returns the numbers and
   decision; `ApplyRouterCandidate` independently makes the same decision from the stored evaluation. Neither the agent nor a candidate
   can change the rule; an explicit admin `maxQualityLoss` remains a compatibility override.
4. **After promotion.** There is no scheduled paid re-evaluation or automatic rollback. Inspect live cost and failures by config version. Use operator rollback if the router needs recovery.
5. **History and rollback.** Use the evaluation id in the version notes with `GetRouterLearningStatus(engine, withCosts=false, evaluationId=...)` to inspect the retained comparison. A `claimed` receipt prevents reusing the evaluation but does not confirm that the config was saved. An `applied` receipt is written only after the save succeeds. If the promotion was interrupted, check the current config and version history before taking action. Retained results contain hashes and scores, not prompts or output transcripts.

    Every save of a subtask-mode config is a git commit of `assets/router.json` and an `ENGINE_UPDATE`
   audit event. `GetRouterLearningStatus` lists the versions with a plain-English `changes` list, the new tasks since the last
   attempt, the schedule and the cost per version. `RollbackRouterConfig(engine, version)` restores a version as a new commit and
   pauses learning; rolling back to the version a rollback came from resumes it, as does `SetRouterLearning` with `enabled=true`.
   Back up and expire replay data under the deployment’s data policy. Historical decisions can be inspected; exact historical re-execution is not exposed by the status command.
6. **Human approval** (optional): set the policy of the `ApplyRouterCandidate` tool to `ask` in the agent's tool policy.

## Status, decisions and recovery

- `GetRouterLearningStatus(engine, withCosts)` is read-only. It reports whether learning is enabled or paused, the optimizer model, `min_new_tasks`, `new_tasks_since_last_attempt`, schedule, live version history and per-version cost. `withCosts=false` skips the slower cost-history scan. This is a configuration/evidence snapshot, not a per-candidate decision log; inspect the scheduled optimizer run and its evaluation result for a specific attempt.
- A volume or scheduled check can defer because learning is off/paused, the new-task threshold is not met, another attempt is active, or too few eligible recorded tasks exist to build TRAIN/VAL. Low traffic is a defer, not a failed candidate. The trigger reports `due=false` with a reason; no candidate is applied.
- Candidate evaluation is incomplete and cannot be applied when required evidence, judge verdicts or prices are unreadable. A complete candidate can still be rejected when quality/cost gates are not met. Neither case changes the live version; do not interpret missing or unknown evidence as a pass. Only the attempt’s frozen VAL evaluation can authorize automatic promotion. Before running VAL or claiming a promotion receipt, the server validates the proposed config with LIVE’s learning settings preserved.
- To undo an applied config, select a committed version from `GetRouterLearningStatus` and call `RollbackRouterConfig(engine, version)`. It writes a new history commit, restores that router configuration and pauses learning. Unknown versions and the current live version are refused. Rolling forward to the version recorded as the rollback source resumes learning; otherwise explicitly re-enable it with `SetRouterLearning` and verify status and scheduled jobs.
- Config rollback is separate from application deployment rollback. Before releasing backend classes or the optimizer skill asset, take the normal deployment backup of each changed artifact and record the live router version. If a release smoke check fails, restore the backed-up application artifacts through the platform's controlled release procedure, then verify runtime hashes, router status/version and scheduled jobs. `RollbackRouterConfig` does not restore Java classes or the skill asset.

## What the optimizer changes

Only a candidate copy of the router (`WriteRouterCandidate`), and only the **step types** (`subtasks` and their recognition rules),
the **routes** (which model serves each subtask) and the **skills** (native skills with a new project ID for each revision). It reads
`GetRouterEvidence` (per subtask and model: steps, cost, failed-run share; a sample of real steps), evaluates a candidate on a TRAIN
set (where it writes skills from the `failure_digest`: the tasks where the cheap run scored below the strongest model, with both
runs' tool-call traces) and on a VAL set (the only evidence for applying), then calls `ApplyRouterCandidate`. The procedure is the
`router-playbook` skill. An agent run cannot call `UpdateModelRouterConfig` on a router with learning on, nor `SetRouterLearning`
or `RollbackRouterConfig`, so `ApplyRouterCandidate` is the only way it changes a live router.

Evidence is pooled per router: the **owner** (the scheduled job runs as its creator) reads the runs of all users of the router;
anyone else reads only their own. Candidates, skills, task sets and evaluations belong to the owner.

## Code review map

| Stage | Where to look |
| --- | --- |
| Serve a user request | [ModelRouterEngine](../../src/prerna/engine/impl/model/ModelRouterEngine.java) dispatches the request. [RouterSubtaskPolicy](../../src/prerna/engine/impl/model/RouterSubtaskPolicy.java) prepares the classifier's input. |
| Start learning | [SetRouterLearning](../../src/prerna/reactor/model/SetRouterLearningReactor.java) configures one job. [RouterLearningTrigger](../../src/prerna/reactor/model/RouterLearningTrigger.java) can wake it on traffic; [RunRouterLearningIfDue](../../src/prerna/reactor/model/RunRouterLearningIfDueReactor.java) decides whether to start the optimizer. |
| Prepare a proposal | [RouterLearningExamples](../../src/prerna/reactor/model/RouterLearningExamples.java) chooses past tasks and saves their original requests and starting files. [GetRouterEvidence](../../src/prerna/reactor/model/GetRouterEvidenceReactor.java) summarizes what happened on those runs. [WriteRouterCandidate](../../src/prerna/reactor/model/WriteRouterCandidateReactor.java) saves the proposal without changing LIVE. |
| Test the proposal | [EvaluateRouterCandidate](../../src/prerna/reactor/model/EvaluateRouterCandidateReactor.java) runs the tasks, accounts for spend, and writes the evaluation receipt. [RouterEvaluationEvidence](../../src/prerna/reactor/model/RouterEvaluationEvidence.java) handles output comparison, judge input, and cached results. |
| Apply, inspect, or undo | [RouterGate](../../src/prerna/reactor/model/RouterGate.java) decides whether the results pass. [ApplyRouterCandidate](../../src/prerna/reactor/model/ApplyRouterCandidateReactor.java) verifies the receipt still matches and uses the [existing config writer](../../src/prerna/reactor/model/UpdateModelRouterConfigReactor.java). [Learning status](../../src/prerna/reactor/model/GetRouterLearningStatusReactor.java) shows the decision; [version history](../../src/prerna/reactor/model/RouterVersions.java) supplies the configs for [operator rollback](../../src/prerna/reactor/model/RollbackRouterConfigReactor.java). |

**How examples and evidence connect:** The builder gets run IDs from [RouterEvidenceStore](../../src/prerna/engine/impl/model/inferencetracking/RouterEvidenceStore.java) and calls the internal `GetRouterEvidenceReactor.run(...)` helper to load each run and its messages. It does not call the Pixel `GetRouterEvidence` command or use its summary to select tasks. The builder writes TRAIN and VAL. Later, the optimizer calls `GetRouterEvidence`, which reads the **TRAIN set's source run IDs** and summarizes those runs. VAL is for checking the proposed change, not for telling the optimizer what to change.

## What gets saved and rerun

A saved task contains the original request, starting files, source run ID, original workspace ID, and names of the tools it used, not the original answer.
The optimizer runs in the seeded optimizer workspace. Reference, LIVE, and candidate each get a fresh room and fresh starting files in the task’s original workspace. Different tasks can use different workspaces; there is no router-wide fallback.
Tasks whose original workspace cannot be established, accessed by the evaluator, or safely used are left out. If a saved task becomes unavailable or loses a required tool, it is skipped in all three arms; the incomplete evaluation cannot promote a candidate.
Learning selects standalone completed tasks with verified starting-file snapshots. Tool results are not user follow-ups.
Older rooms without a snapshot manifest are not eligible. Older snapshots without a workspace can use the run’s explicitly recorded workspace; the current room setting is never used as a fallback. A task stays in the same TRAIN or VAL partition based on its
request and file contents; if either partition is short, learning defers.
The attempt stores exact TRAIN and VAL IDs and its optimizer room ID. Reads and replay exclusions use those IDs, not room-name prefixes.

### Replay settings

Pass a `replay` map to `SetRouterLearning` when enabling or updating learning. The router saves the settings under
`improvement.replay`; the attempt records the resolved values. Omitted fields retain their existing values or defaults. The former `workspace_id` setting is ignored.

| Setting | Default | Purpose |
| --- | --- | --- |
| `history_days`, `max_runs` | 30, 1000 | Bound how far back and how many runs the builder inspects. |
| `min_train_tasks`, `max_train_tasks`, `validation_tasks` | 10, 30, 20 | Bound the two sets. Validation has a floor of 20 and supplies the gate's minimum. |
| `max_snapshot_bytes` | 5242880 | Maximum starting-file bytes per room. |
| `evaluation_budget_micros` | 30000000 | Spend limit for one evaluation, not a cycle-wide or provider hard cap. |
| `repeats`, `parallel_runs`, `max_turns`, `run_timeout_seconds` | 2, 4, 25, 900 | Limit replay repetitions, concurrency, turns and wait time. |

### How the evaluator scores

For each saved task, replay it on the strong reference model, LIVE, and the candidate. Each execution uses the same configured
SEMOSS workspace but works from its fresh room's root, where the saved files are staged. The native harness applies the workspace
policy to default tools. A historical run is eligible only if every tool it called is still in the workspace's enabled default-tool
registry. Missing or disabled tools cause the example to be skipped, not scored as a candidate failure. The evaluator uses today's
implementation of each tool. This selection check doesn't add a new runtime restriction.

The judge reads the original request and both results, including readable workbook contents. It compares candidate vs. reference
and LIVE vs. reference. A matching file is not an automatic pass and a different layout is not an automatic failure. Unreadable
or oversized evidence, an unknown charge, or an inconclusive judge verdict makes the evaluation ineligible for promotion.
Measured spend above the limit also blocks promotion, including when the last run caused the overrun. Already-running work can
still cause the evaluation to spend more than the requested limit. Cached reference and LIVE results are reused within a set
only for matching settings, workspace configuration, and enabled tool IDs in the current server process. A server restart invalidates
these results so a new deployment does not reuse scores from earlier tool implementations. Multi-turn rooms and approval-dependent
runs are not supported replay cases.

## Classifier context

The router uses the same active conversation branch as the serving model. Its separate classifier request uses the latest user request, the previous subtask from backend routing metadata, and the current tool names and statuses, without tool output. It does not compact or summarize the conversation. The serving model still receives the normal SEMOSS context. Old `classifier_context` settings are accepted but no longer used.

## Constants

Everything below is internal; none is an admin setting unless stated. The earlier knobs `max_quality_drop`, `min_cost_reduction_pct`,
`min_tasks`, `min_net_wins`, `max_cost_increase_pct`, `subtask_context_steps` no longer exist; a
stored config that still has them loads, and the keys are ignored.

| Constant | Value | Reason |
| --- | --- | --- |
| `improvement.max_quality_loss` (admin) | 0 by default | refuses any observed paired held-out quality decline; an explicitly configured legacy value remains supported |
| `improvement.min_new_tasks` (admin) | 100 | runs to record before a learning check; about half of them make usable tasks |
| `frequency` (admin) | daily | clock next to the volume trigger |
| confidence z | 1.645 | one-sided 95%: a difference beyond this many standard errors is real, not noise |
| failure digest | 10 tasks | what the optimizer can read |
| judge input | 2,000 chars of answer, 4,000 per file, 12,000 per result | enough to compare a result, not a whole workbook |
| files per replay | 20 files of at most 20 MB | extra or oversized evidence stops evaluation rather than being silently omitted |
| learning check interval | 10 min per router | a check reads the run history; a started run is recorded long before the next |
| skill text added to a step | 8,000 chars | bounds the prompt |
| optimizer budgets | 60 turns, 3,600 s | in the seeded `router-optimizer` agent |

## Data model

| What | Where |
| --- | --- |
| Config, `improvement`, `version` | `router.json` of the router engine; each version is a git commit |
| Routing decision of each step | `ornaments.routing.trace` of the response message (`subtask_id`, `skill_id`, `config_version`, `classifier_inputs`) |
| Cost, feedback, run status | existing MESSAGE and AGENT_RUN rows; nothing new |
| Evaluations and promotion records | server-private `router-learning-evidence`, keyed by engine; not copied when cloning engine assets |
| Replay inputs and cached results | per-router `replay/<setId>/` in the same server-private directory |

## Known limits

- Replay supports native room-file, staged-skill, and todo tools only. It excludes source tasks that used other tools and workspaces with MCP connections, custom hooks, subagents, managed presentation workflows, or Digital Twin behavior. The harness refuses tools outside the replay set even if a model asks for them. This restriction is not a sandbox for arbitrary commands; shell and code execution are not available. The release check should exercise two original workspaces, refuse a prohibited tool, confirm starting files remain unchanged, and show an incomplete evaluation cannot promote a candidate.
- Quality is agreement with the strongest route, not independently established correctness. A separate prompt-aligned direct-LLM study scored 60 balanced synthetic holdout cases (20 per task type, 10 correct and 10 incorrect per type): it caught 10/10 incorrect cases and raised 0/10 false alarms per type. Its 95% Wilson intervals were wide (about 0.72–1.00 for catch rate and 0–0.28 for false alarms). The study used direct text prompts with empty file sections and different boundary-newline assembly; it did not invoke the native comparator. These figures are not native comparator error rates or production-generalization evidence. Independent checker-labelled measurements of the native comparator's error rates remain outstanding; do not infer them from the direct study.
- With 20 VAL tasks small quality differences remain noisy. The default refuses an observed paired decline, but cannot establish a universal no-loss guarantee; explicit nonzero `maxQualityLoss` is an admin override.
- Skills raised quality on a small hard task set but not beyond the noise; the cost reduction is the proven effect.
- Candidate engines are private clones and are not deleted automatically.
