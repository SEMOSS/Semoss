/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.model;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.RouterEvidenceStore;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.engine.impl.model.usage.ModelUsageReport;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.agent.metrics.AssessAgentEffectivenessReactor;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.model.RouterEvaluationEvidence.Output;
import prerna.reactor.model.RouterEvaluationEvidence.Result;
import prerna.reactor.model.RouterLearningExamples.Task;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.AssetUtility;
import prerna.util.ValueUtils;

/**
 * Replays recorded tasks ({@link RouterLearningExamples}) on the strongest model (the reference), on a candidate router and on the live router,
 * and reports the paired differences {@link ApplyRouterCandidateReactor} decides on. Quality is agreement with the reference: a run is
 * good when the judge finds its result at least as good as the strongest model's on the same task. The judge compares both answers and
 * their readable outputs side by side. The live and strongest results are cached per set so other candidates reuse them. The result is
 * stored under an {@code evaluationId} with the candidate's config sha-256 and the caller. Judge tokens count against the cap. Runs
 * already in flight can make total spend exceed it, but over-budget evaluations cannot pass.
 */
public class EvaluateRouterCandidateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EvaluateRouterCandidateReactor.class);

	private static final String CANDIDATE_KEY = "candidateEngineId";
	private static final String JUDGE_KEY = "judgeModelId";
	private static final String CAP_KEY = "jobCostCapMicros";
	private static final String TASK_SET_KEY = "taskSet";
	private static final String LIVE_KEY = "liveEngineId";
	/** The digest holds the worst tasks only: ten traces are what the optimizer can read. */
	private static final int MAX_DIGEST = 10;
	private static final int TRACE_CHARS = 1500;
	private static final Pattern ID = Pattern.compile("[A-Za-z0-9-]{8,64}");

	/** What the reactor reads and runs; a fake replaces it in unit tests. */
	interface Backend {
		default boolean canReplay(Task task) {
			return true;
		}

		/** The run once it ended, needs input or {@code timeoutMs} passed (then its status is {@code timed out}), with its messages. */
		Map<String, Object> await(String runId, long timeoutMs);

		/** Starts a recorded task in a fresh room with its input files staged; returns its run id. */
		String startTask(Task task, String engineId, int maxTurns);

		/** The files a run of the task wrote or changed in its room. */
		List<Output> outputs(Task task, String roomId);

		/** Input plus output list price of a model, 0 when unknown. */
		double price(String engineId);

		/** Sum of COST_MICROS of the room's rows. */
		long costMicros(String roomId);

		void stop(String runId);

		/** Whether the judge finds the candidate run at least as good as the reference run (null when its answer is not parseable), and its short reason. */
		Judged compare(Task task, Map<String, Object> candidate, List<Output> candidateFiles, Map<String, Object> reference, List<Output> referenceFiles,
				String judgeModelId);

		/** Validates the judge evidence and estimates its maximum charge, or returns -1 when it cannot be priced before the call. */
		default long maxCompareCost(Task task, Map<String, Object> candidate, List<Output> candidateFiles, Map<String, Object> reference,
				List<Output> referenceFiles, String judgeModelId) {
			return -1;
		}

		/** A short readable transcript of a run, or null when unavailable. */
		default String trace(String runId, int maxChars) {
			return null;
		}
	}

	record Judged(Boolean equalOrBetter, String reason, long micros) {
	}

	record Comparison(Judged judged, boolean judgeCall) {
	}

	private Backend backend;

	public EvaluateRouterCandidateReactor() {
		this.keysToGet = new String[] { CANDIDATE_KEY, LIVE_KEY, TASK_SET_KEY, JUDGE_KEY, CAP_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	void setBackend(Backend backend) {
		this.backend = backend;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String candidate = ValueUtils.trimToNull(this.keyValue.get(CANDIDATE_KEY));
		String liveId = ValueUtils.trimToNull(this.keyValue.get(LIVE_KEY));
		String taskSet = ValueUtils.trimToNull(this.keyValue.get(TASK_SET_KEY));
		String judge = ValueUtils.trimToNull(this.keyValue.get(JUDGE_KEY));
		Integer cap = ValueUtils.parseWholeNumber(this.keyValue.get(CAP_KEY));
		if (candidate == null || candidate.equals(liveId) || liveId == null || taskSet == null || judge == null || cap == null || cap <= 0) {
			throw new IllegalArgumentException("candidateEngineId, liveEngineId, taskSet, judgeModelId and a positive jobCostCapMicros are required");
		}
		String candidateJson = UpdateModelRouterConfigReactor.readConfig(UpdateModelRouterConfigReactor.editableRouter(this.insight, candidate));
		String liveJson = UpdateModelRouterConfigReactor.readConfig(UpdateModelRouterConfigReactor.editableRouter(this.insight, liveId));
		// hashed before the runs, so an edit made while they run makes the evaluation unusable
		String candidateSha = ModelRouterEngine.sha256(candidateJson);
		Path replay = evidenceDir(liveId).resolve("replay");
		String attemptId = this.insight.getRoomId();
		AccessToken login = GetRouterEvidenceReactor.loginOf(this.insight);
		String split;
		Map<String, String> liveSkills;
		Map<String, String> candidateSkills;
		var improvement = ModelRouterEngine.parseAndValidateConfig(liveJson, "live router", liveId).improvement();
		ReplaySettings settings = improvement == null ? new ReplaySettings() : improvement.replaySettings();
		if (cap > settings.evaluation_budget_micros) {
			throw new IllegalArgumentException("Evaluation budget exceeds the router's configured limit");
		}
		LiveBackend live = this.backend == null ? new LiveBackend(this.insight) : null;
		Backend source = live == null ? this.backend : live;
		String replayContext;
		List<Task> tasks;
		SetJob job;
		try {
			Map<String, Object> attempt = RunRouterLearningIfDueReactor.attempt(liveId, attemptId);
			if (attemptId != null && (attempt == null || !(taskSet.equals(attempt.get("train")) || taskSet.equals(attempt.get("val"))))) {
				throw new IllegalArgumentException("Task set is not assigned to this learning attempt");
			}
			if (attempt != null && (!RunRouterLearningIfDueReactor.ownedBy(attempt, login)
					|| !judge.equals(attempt.get("judge")))) {
				throw new IllegalArgumentException("Evaluator identity or judge does not match the learning attempt");
			}
			if (attempt != null && !new Gson().toJsonTree(settings).equals(new Gson().toJsonTree(attempt.get("replay_settings")))) {
				throw new IllegalArgumentException("Replay settings changed or are missing; start a new learning attempt");
			}
			split = attempt == null ? "diagnostic" : taskSet.equals(attempt.get("val")) ? "val" : "train";
			Path dir = RouterLearningExamples.setDir(replay, taskSet);
			String replayHash = RouterLearningExamples.replayFingerprint(dir);
			if (attempt != null && !replayHash.equals(attempt.get(split + "_sha256"))) {
				throw new IllegalArgumentException("Replay inputs changed after the learning attempt started");
			}
			tasks = RouterLearningExamples.load(dir);
			if (live != null) {
				live.pinWorkspaces(tasks);
			}
			replayContext = new Gson().toJson(List.of(settings, live == null ? Map.of() : live.workspaceFingerprints(), RouterEvaluationEvidence.CACHE_EPOCH));
			if ("val".equals(split)) {
				ApplyRouterCandidateReactor.validatedPromotionConfig(JsonParser.parseString(candidateJson).getAsJsonObject(),
						JsonParser.parseString(liveJson).getAsJsonObject(), liveId);
				Files.createFile(RunRouterLearningIfDueReactor.attemptDir(liveId).resolve(attemptId + ".validation"));
			}
			liveSkills = RouterEvaluationEvidence.skillFingerprints(liveJson);
			candidateSkills = RouterEvaluationEvidence.skillFingerprints(candidateJson);
			job = new SetJob(taskSet, tasks,
					ModelRouterEngine.sha256(replayHash + liveSkills + judge + replayContext),
					dir, candidate, liveId, liveJson,
					RouterEvaluationEvidence.strongest(JsonParser.parseString(liveJson).getAsJsonObject(), source::price), judge, cap, settings);
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not read task set " + taskSet + ": " + e.getMessage(), e);
		}
		Map<String, Object> result = evaluateSet(source, job);
		if (!"train".equals(split)) {
			result.remove("failure_digest");
		}
		Map<String, Object> record = new LinkedHashMap<>(result);
		record.put("split", split);
		record.put("scoring", RouterEvaluationEvidence.SCORING);
		record.put("attempt_id", attemptId);
		try {
			String afterHash = RouterLearningExamples.replayFingerprint(RouterLearningExamples.setDir(replay, taskSet));
			if (!job.manifestSha().equals(ModelRouterEngine.sha256(afterHash + liveSkills + judge + replayContext))
					|| (live != null && !live.workspacesUnchanged())
					|| !liveSkills.equals(RouterEvaluationEvidence.skillFingerprints(liveJson)) || !candidateSkills.equals(RouterEvaluationEvidence.skillFingerprints(candidateJson))) {
				throw new IllegalStateException("Replay inputs, workspaces, or skill contents changed during evaluation");
			}
			record.put("manifest_sha256", afterHash);
		} catch (IOException e) {
			throw new IllegalStateException("Could not verify the evaluated inputs", e);
		}
		record.put("live_skill_sha256", liveSkills);
		record.put("candidate_skill_sha256", candidateSkills);
		record.put("live_config_sha256", ModelRouterEngine.sha256(liveJson));
		record.put("candidateEngineId", candidate);
		record.put("candidateSha256", candidateSha);
		record.put("userId", login.getId());
		record.put("userType", login.getProvider().getLabel());
		String id = UUID.randomUUID().toString();
		try {
			save(evaluationDir(candidate), id, record);
		} catch (IOException e) {
			throw new IllegalStateException("Could not store the evaluation: " + e.getMessage(), e);
		}
		if (!"train".equals(split)) {
			result.remove("candidate_task_scores");
			result.remove("live_task_scores");
		}
		result.put("evaluationId", id);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** The folder of a candidate engine that holds its stored evaluations: one JSON file each, written by the evaluator. */
	static Path evaluationDir(String candidateEngineId) {
		return evidenceDir(candidateEngineId).resolve("router-evaluations");
	}

	/** Keep private replay evidence outside engine assets, which are copied into every candidate. */
	static Path evidenceDir(String engineId) {
		return Path.of(Utility.getBaseFolder(), "router-learning-evidence", ModelRouterEngine.sha256(engineId));
	}

	static Path file(Path dir, String id) {
		if (id == null || !ID.matcher(id).matches()) {
			throw new IllegalArgumentException("Invalid evaluationId");
		}
		return dir.resolve(id + ".json");
	}

	static void save(Path dir, String id, Map<String, Object> record) throws IOException {
		Files.createDirectories(dir);
		Files.writeString(file(dir, id), new Gson().toJson(record));
	}

	/** The stored evaluation, or null when there is none. */
	static Map<String, Object> load(Path dir, String id) throws IOException {
		try {
			return new Gson().fromJson(Files.readString(file(dir, id)), new TypeToken<Map<String, Object>>() {
			}.getType());
		} catch (NoSuchFileException e) {
			return null;
		}
	}

	/** One evaluation; {@code strongEngine} is the model that produces the reference results. */
	record SetJob(String setId, List<Task> tasks, String manifestSha, Path cacheDir, String candidate, String liveId, String liveJson,
			String strongEngine, String judge, long cap, ReplaySettings settings) {
	}

	/**
	 * Runs the set on the strongest model (the reference, first, so a cap hit cannot leave the comparison without it), on the candidate
	 * and on the live router (reference and live cached in {@code cacheDir}). Every candidate and live run is scored 1 when it is at
	 * least as good as the reference result of the same task, else 0. Returns the means, the paired differences against the live
	 * router on the tasks both completed ({@code n}, {@code quality_delta_vs_live}, {@code quality_se}, {@code cost_delta_vs_live},
	 * {@code cost_se}), the plain-words {@code decision} and the {@code failure_digest}.
	 */
	Map<String, Object> evaluateSet(Backend source, SetJob fullSet) {
		List<Task> eligible = fullSet.tasks().stream().filter(source::canReplay).toList();
		int skipped = fullSet.tasks().size() - eligible.size();
		SetJob job = new SetJob(fullSet.setId(), eligible, fullSet.manifestSha(), fullSet.cacheDir(), fullSet.candidate(), fullSet.liveId(),
				fullSet.liveJson(), fullSet.strongEngine(), fullSet.judge(), fullSet.cap(), fullSet.settings());
		AtomicLong spent = new AtomicLong();
		AtomicLong judgeSpent = new AtomicLong();
		AtomicLong judgeCalls = new AtomicLong();
		AtomicReference<String> stop = new AtomicReference<>();
		Map<String, Result> reference = cachedArm(source, job, "reference", ModelRouterEngine.sha256(job.strongEngine()).substring(0, 16), job.strongEngine(),
				null, spent, stop, judgeSpent, judgeCalls);
		Map<String, Result> candidate = runArm(source, job, job.tasks(), job.candidate(), reference, spent, stop, judgeSpent, judgeCalls);
		Map<String, Result> live = cachedArm(source, job, "live", ModelRouterEngine.sha256(job.liveJson() + "|" + job.strongEngine() + "|" + reference.values().stream().flatMap(r -> r.runIds().stream()).sorted().toList()), job.liveId(), reference,
				spent, stop, judgeSpent, judgeCalls);
		long totalSpent = spent.get();
		if (totalSpent > job.cap()) {
			stop.compareAndSet(null, "cap");
		}
		String stopReason = stop.get();
		if (stopReason == null && skipped > 0) {
			stopReason = "replay_workspace_unavailable";
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("task_set", job.setId());
		out.put("quality", mean(candidate, false));
		out.put("cost_micros_per_task", mean(candidate, true));
		out.put("live_quality", mean(live, false));
		out.put("live_cost_micros_per_task", mean(live, true));
		out.put("reference_cost_micros_per_task", mean(reference, true));
		out.put("judge_cost_micros_per_comparison", judgeCalls.get() == 0 ? null : (double) judgeSpent.get() / judgeCalls.get());
		out.put("strong_engine", job.strongEngine());
		out.put("judge_engine", job.judge());
		out.put("stopped_by_cap", "cap".equals(stopReason));
		out.put("complete", stopReason == null && candidate.size() == job.tasks().size() && live.size() == job.tasks().size());
		out.put("stop_reason", stopReason);
		out.put("skipped_tasks", skipped);
		out.put("spent_micros", totalSpent);
		out.put("live_engine_id", job.liveId());
		out.put("live_version", JsonParser.parseString(job.liveJson()).getAsJsonObject().get("version").getAsString());
		List<String> paired = job.tasks().stream().map(Task::id).filter(id -> candidate.containsKey(id) && live.containsKey(id)).toList();
		out.put("n", paired.size());
		Map<String, Object> candidateTaskScores = new LinkedHashMap<>();
		candidate.forEach((id, value) -> candidateTaskScores.put(id, Map.of("score", value.score(), "cost_micros", value.costMicros())));
		Map<String, Object> liveTaskScores = new LinkedHashMap<>();
		live.forEach((id, value) -> liveTaskScores.put(id, Map.of("score", value.score(), "cost_micros", value.costMicros())));
		out.put("candidate_task_scores", candidateTaskScores);
		out.put("live_task_scores", liveTaskScores);
		if (!paired.isEmpty()) {
			RouterGate.Paired quality = RouterGate.Paired.of(paired.stream().mapToDouble(id -> candidate.get(id).score() - live.get(id).score()).toArray());
			RouterGate.Paired cost = RouterGate.Paired.of(paired.stream().mapToDouble(id -> candidate.get(id).costMicros() - live.get(id).costMicros()).toArray());
			out.put("quality_delta_vs_live", quality.mean());
			out.put("quality_se", quality.se());
			out.put("cost_delta_vs_live", cost.mean());
			out.put("cost_se", cost.se());
		}
		double allowedLoss = ModelRouterEngine.parseAndValidateConfig(job.liveJson(), "live router", job.liveId()).improvement() instanceof ModelRouterEngine.Improvement i
				? i.max_quality_loss : 0;
		String refusal = RouterGate.refusal(out, allowedLoss, job.settings().validation_tasks);
		out.put("decision", refusal == null ? "the candidate would be applied: not worse than the live version and cheaper or better beyond noise" : refusal);
		out.put("failure_digest", digest(source, candidate, reference));
		return out;
	}

	/** The tasks where the candidate was worse than the strongest model (worst first) with the reason and what each run did, so the optimizer can write the skill that closes the gap. */
	private static List<Map<String, Object>> digest(Backend source, Map<String, Result> candidate, Map<String, Result> reference) {
		List<Map<String, Object>> worst = new ArrayList<>();
		candidate.forEach((id, r) -> {
			Result ref = reference.get(id);
			if (ref != null && r.score() < 1) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("task", id);
				entry.put("score", r.score());
				entry.put("detail", r.detail());
				entry.put("candidate_trace", traceOf(source, r.runId()));
				entry.put("reference_trace", traceOf(source, ref.runId()));
				worst.add(entry);
			}
		});
		worst.sort(Comparator.comparingDouble(d -> (double) d.get("score")));
		return worst.subList(0, Math.min(MAX_DIGEST, worst.size()));
	}

	private static String traceOf(Backend source, String runId) {
		String trace = runId == null || runId.isEmpty() ? null : source.trace(runId, TRACE_CHARS);
		return trace == null ? "(not available)" : trace;
	}

	/**
	 * What a run did, one line per tool call (name, arguments, result status and output, each cut short) without the task
	 * prompt, then the final answer; the middle is dropped when the text would pass {@code maxChars}.
	 */
	static String compactTrace(Object messages, String finalText, int maxChars) {
		List<String> lines = new ArrayList<>();
		for (Object message : messages instanceof List<?> list ? list : List.of()) {
			String label = message instanceof Map<?, ?> m && m.get("ornaments") instanceof Map<?, ?> o && o.get("routing") instanceof Map<?, ?> r
					&& r.get("trace") instanceof Map<?, ?> t && t.get("subtask_id") != null ? "[" + t.get("subtask_id") + "] " : "";
			for (Object part : message instanceof Map<?, ?> m && m.get("parts") instanceof List<?> parts ? parts : List.of()) {
				if (!(part instanceof Map<?, ?> p)) {
					continue;
				}
				String type = ValueUtils.toStringOrEmpty(p.get("type"));
				if ("TOOL_CALL".equalsIgnoreCase(type) && (p.get("toolCall") != null ? p.get("toolCall") : p.get("tool_call")) instanceof Map<?, ?> c) {
					lines.add("call " + label + c.get("name") + " " + cut(String.valueOf(c.get("arguments")), 160));
				} else if ("TOOL_RESULT".equalsIgnoreCase(type)
						&& (p.get("toolResult") != null ? p.get("toolResult") : p.get("tool_result")) instanceof Map<?, ?> r) {
					lines.add("  -> " + (r.get("toolStatus") != null ? r.get("toolStatus") : r.get("tool_status")) + ": "
							+ cut(ValueUtils.toStringOrEmpty(r.get("output")), 160));
				}
			}
		}
		String tail = "FINAL: " + cut(finalText, 300);
		int budget = Math.max(200, maxChars - tail.length() - 40);
		int used = 0;
		int head = 0;
		while (head < lines.size() && used + lines.get(head).length() + 1 <= budget / 2) {
			used += lines.get(head++).length() + 1;
		}
		int from = lines.size();
		while (from > head && used + lines.get(from - 1).length() + 1 <= budget) {
			used += lines.get(--from).length() + 1;
		}
		StringBuilder out = new StringBuilder();
		lines.subList(0, head).forEach(l -> out.append(l).append('\n'));
		if (from > head) {
			out.append("... ").append(from - head).append(" lines omitted ...\n");
		}
		lines.subList(from, lines.size()).forEach(l -> out.append(l).append('\n'));
		return out.append(tail).toString();
	}

	private static String cut(String text, int max) {
		String flat = text.replaceAll("\\s+", " ").trim();
		return flat.length() <= max ? flat : flat.substring(0, max) + "...";
	}

	/** Mean score (or cost) of the results; null when there are none. */
	private static Double mean(Map<String, Result> results, boolean cost) {
		return results.isEmpty() ? null : results.values().stream().mapToDouble(r -> cost ? r.costMicros() : r.score()).average().getAsDouble();
	}

	/** Cached results of the tasks on an engine; the missing ones run and are cached once they completed. */
	private Map<String, Result> cachedArm(Backend source, SetJob job, String kind, String key, String engine, Map<String, Result> reference,
			AtomicLong spent, AtomicReference<String> stop, AtomicLong judgeSpent, AtomicLong judgeCalls) {
		Path file = job.cacheDir().resolve(kind + "-" + key + ".json");
		Map<String, Result> results = RouterEvaluationEvidence.readCache(file, job.setId(), job.manifestSha(), job.settings().repeats);
		Set<String> eligible = job.tasks().stream().map(Task::id).collect(Collectors.toSet());
		results.keySet().retainAll(eligible);
		List<Task> todo = job.tasks().stream().filter(t -> !results.containsKey(t.id())).toList();
		if (!todo.isEmpty() && stop.get() == null) {
			results.putAll(runArm(source, job, todo, engine, reference, spent, stop, judgeSpent, judgeCalls));
			try {
				RouterEvaluationEvidence.writeCache(file, job.setId(), job.manifestSha(), job.settings().repeats, results);
			} catch (IOException e) {
				classLogger.warn("Could not cache the {} results of task set {}: {}", kind, job.setId(), e.getMessage());
			}
		}
		return results;
	}

	/** Runs tasks on an engine, up to the configured concurrency, scored against {@code reference} (null for the reference arm itself); tasks the cap or an unreadable cost stopped are left out. */
	private Map<String, Result> runArm(Backend source, SetJob job, List<Task> tasks, String engine, Map<String, Result> reference, AtomicLong spent,
			AtomicReference<String> stop, AtomicLong judgeSpent, AtomicLong judgeCalls) {
		Map<String, Result> results = new LinkedHashMap<>();
		ExecutorService pool = Executors.newFixedThreadPool(job.settings().parallel_runs);
		Set<String> activeRuns = ConcurrentHashMap.newKeySet();
		try {
			ExecutorCompletionService<Map.Entry<String, Result>> completed = new ExecutorCompletionService<>(pool);
			for (Task task : tasks) {
				completed.submit(() -> {
					Result result = runTask(source, job, task, engine, reference, spent, stop, judgeSpent, judgeCalls, activeRuns);
					return result == null ? null : Map.entry(task.id(), result);
				});
			}
			for (int i = 0; i < tasks.size(); i++) {
				Map.Entry<String, Result> result = completed.take().get();
				if (stop.get() != null) {
					break;
				}
				if (result != null) {
					results.put(result.getKey(), result.getValue());
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			stop.compareAndSet(null, "interrupted");
		} catch (ExecutionException e) {
			stop.compareAndSet(null, "run_failed");
			throw new IllegalStateException("A task run failed: " + e.getCause().getMessage(), e.getCause());
		} finally {
			pool.shutdownNow();
			boolean interrupted = Thread.interrupted();
			try {
				for (String runId : activeRuns) {
					stopRun(source, activeRuns, runId);
				}
			} finally {
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
		}
		return results;
	}

	private static void stopRun(Backend source, Set<String> activeRuns, String runId) {
		if (activeRuns.remove(runId)) {
			source.stop(runId);
		}
	}

	/** Reads of a finished run's room can meet "room is busy" while the platform still writes to it (parallel runs contend for it): tried five times. */
	private static <T> T retrying(Supplier<T> read) {
		for (int attempt = 1;; attempt++) {
			try {
				return read.get();
			} catch (IllegalStateException e) {
				if (attempt >= 5) {
					throw e;
				}
				try {
					Thread.sleep(3000L * attempt);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					throw e;
				}
			}
		}
	}

	/**
	 * One task over its repeats, each a fresh room and run. A reference run (no {@code reference}) that completes scores 1; any other
	 * run scores 1 when it is at least as good as the reference result of the task, else 0, as does a run that does not complete
	 * (failed, needs input, times out). Null when the cap or an unreadable cost stopped it, or when the reference has no completed run
	 * of the task to compare with. With concurrent replay the cap can be passed by the cost of the runs in flight.
	 */
	private Result runTask(Backend source, SetJob job, Task task, String engine, Map<String, Result> reference, AtomicLong spent,
			AtomicReference<String> stop, AtomicLong judgeSpent, AtomicLong judgeCalls, Set<String> activeRuns) {
		Result standard = reference == null ? null : reference.get(task.id());
		if (reference != null && (standard == null || standard.score() < 1)) {
			return null;
		}
		double score = 0;
		long cost = 0;
		String detail = "";
		List<String> runIds = new ArrayList<>(job.settings().repeats);
		for (int i = 0; i < job.settings().repeats; i++) {
			if (Thread.currentThread().isInterrupted()) {
				stop.compareAndSet(null, "interrupted");
				return null;
			}
			if (stop.get() != null || spent.get() >= job.cap()) {
				stop.compareAndSet(null, "cap");
				return null;
			}
			Map<String, Object> run;
			String lastRunId;
			try {
				lastRunId = source.startTask(task, engine, job.settings().max_turns);
			} catch (IllegalStateException e) {
				stop.compareAndSet(null, "run_start_unconfirmed");
				classLogger.error("Task {} on {} may have started without a recorded run id; evaluation stops without retry: {}", task.id(), engine,
						e.getMessage());
				return null;
			}
			String runId = lastRunId;
			runIds.add(runId);
			activeRuns.add(runId);
			if (stop.get() != null || Thread.currentThread().isInterrupted()) {
				stopRun(source, activeRuns, runId);
				return null;
			}
			try {
				run = retrying(() -> source.await(runId, TimeUnit.SECONDS.toMillis(job.settings().run_timeout_seconds)));
			} catch (RuntimeException e) {
				stop.compareAndSet(null, "run_result_unreadable");
				boolean interrupted = Thread.interrupted();
				try {
					stopRun(source, activeRuns, runId);
				} finally {
					if (interrupted) {
						Thread.currentThread().interrupt();
					}
				}
				classLogger.error("Task {} on {} started as run {} but its result could not be read; evaluation stops: {}", task.id(), engine,
						lastRunId, e.getMessage());
				return null;
			}
			if (run == null) {
				stop.compareAndSet(null, "run_result_unreadable");
				stopRun(source, activeRuns, runId);
				return null;
			}
			String status = ValueUtils.toStringOrEmpty(run.get("status"));
			if (AgentRunStatus.INPUT_REQUIRED.name().equals(status)) {
				stopRun(source, activeRuns, runId);
			} else {
				activeRuns.remove(runId);
			}
			try {
				long runCost = source.costMicros(ValueUtils.toStringOrEmpty(run.get("roomId")));
				spent.addAndGet(runCost);
				cost += runCost;
			} catch (RuntimeException e) {
				// fails closed: spending that cannot be measured is not allowed to go on
				classLogger.error("Run '{}': its cost could not be read, the evaluation stops: {}", lastRunId, e.getMessage());
				stop.compareAndSet(null, "cost_unreadable");
				return null;
			}
			double runScore = 0;
			detail = AgentRunStatus.INPUT_REQUIRED.name().equals(status) ? "the run needed input or approval" : "run ended " + status;
			if (AgentRunStatus.COMPLETED.name().equals(status)) {
				runScore = 1;
				detail = "reference";
				if (standard != null) {
					Judged verdict = scoreAgainst(source, job, task, run, standard.runIds().get(i), spent, stop, judgeSpent, judgeCalls);
					if (verdict.equalOrBetter() == null) {
						return null;
					}
					runScore = verdict.equalOrBetter() ? 1 : 0;
					detail = verdict.reason();
				}
			}
			score += runScore;
		}
		return new Result(score / job.settings().repeats, cost / job.settings().repeats, detail, List.copyOf(runIds));
	}

	static Comparison compareResults(AtomicLong spent, long cap, AtomicReference<String> stop, Backend source, Task task, Map<String, Object> candidate, List<Output> candidateFiles, Map<String, Object> reference,
			List<Output> referenceFiles, String judgeModelId) {
		long estimate = source.maxCompareCost(task, candidate, candidateFiles, reference, referenceFiles, judgeModelId);
		synchronized (spent) {
			if (stop.get() != null || estimate < 0 || estimate > cap - spent.get()) {
				stop.compareAndSet(null, estimate < 0 ? "judge_price_unreadable" : "cap");
				return new Comparison(new Judged(null, "judge request not admitted", 0), false);
			}
			spent.addAndGet(estimate);
		}
		Judged verdict = source.compare(task, candidate, candidateFiles, reference, referenceFiles, judgeModelId);
		if (verdict != null && verdict.micros() >= 0) {
			spent.addAndGet(verdict.micros() - estimate);
			if (spent.get() > cap) {
				stop.compareAndSet(null, "cap");
			}
		}
		return new Comparison(verdict, true);
	}

	/** Asks the judge whether the completed run is at least as good as the reference for the same task and repeat. */
	private Judged scoreAgainst(Backend source, SetJob job, Task task, Map<String, Object> run, String referenceRunId, AtomicLong spent,
			AtomicReference<String> stop, AtomicLong judgeSpent, AtomicLong judgeCalls) {
		try {
			Map<String, Object> reference = retrying(() -> source.await(referenceRunId, 0));
			List<Output> files = retrying(() -> source.outputs(task, ValueUtils.toStringOrEmpty(run.get("roomId"))));
			List<Output> referenceFiles = retrying(() -> source.outputs(task, ValueUtils.toStringOrEmpty(reference.get("roomId"))));
			Comparison comparison = compareResults(spent, job.cap(), stop, source, task, run, files, reference, referenceFiles, job.judge());
			Judged judged = comparison.judged();
			if (!comparison.judgeCall()) {
				return judged;
			}
			if (judged == null || judged.micros() < 0) {
				stop.compareAndSet(null, "judge_cost_unreadable");
				return new Judged(null, judged == null ? "judge returned no cost or verdict" : judged.reason(), 0);
			}
			judgeSpent.addAndGet(judged.micros());
			judgeCalls.incrementAndGet();
			if (judged.equalOrBetter() == null) {
				stop.compareAndSet(null, "judge_verdict_unreadable");
				return judged;
			}
			return judged;
		} catch (IllegalArgumentException e) {
			stop.compareAndSet(null, "judge_evidence_incomplete");
			return new Judged(null, e.getMessage(), 0);
		} catch (RuntimeException e) {
			stop.compareAndSet(null, "judge_cost_or_evidence_unreadable");
			classLogger.error("Judge comparison for task {} failed without retry; evaluation stops: {}", task.id(), e.getMessage());
			return new Judged(null, "judge comparison failed without a reliable verdict or cost", 0);
		}
	}

	private static long maximumJudgeCost(Map<String, Object> price, String prompt) {
		return maximumJudgeCost(price, prompt.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
	}

	private static long maximumJudgeCost(Map<String, Object> price, long promptBytes) {
		return ModelUsageCost.costMicros(price, new ModelUsageReport(promptBytes, 1000, 0, 0, 0, 0));
	}

	private static long requiredTokenCount(Map<String, Object> response, String key) {
		Object value = response.get(key);
		if (!(value instanceof Number)) {
			throw new IllegalStateException("Judge response omitted numeric " + key + " usage");
		}
		long tokens = new BigDecimal(value.toString()).longValueExact();
		if (tokens < 0) {
			throw new IllegalStateException("Judge response reported negative " + key + " usage");
		}
		return tokens;
	}

	static long judgeCostMicros(Map<String, Object> price, AskModelEngineResponse<?> modelResponse,
			Map<String, Object> response) {
		requiredTokenCount(response, "numberOfTokensInPrompt");
		requiredTokenCount(response, "numberOfTokensInResponse");
		return ModelUsageCost.costMicros(price, ModelUsageReport.of(modelResponse, 0));
	}

	/** The real run service, logs and judge model, all as the caller. */
	static final class LiveBackend implements Backend {
		private final Insight insight;
		private final Map<String, RouterReplayWorkspace.Snapshot> workspaces = new TreeMap<>();

		LiveBackend(Insight insight) {
			this.insight = insight;
		}

		void pinWorkspaces(List<Task> tasks) {
			for (Task task : tasks) {
				String id = task.workspaceId();
				if (id != null && !workspaces.containsKey(id)) {
					workspaces.put(id, RouterReplayWorkspace.inspect(insight, id));
				}
			}
		}

		Map<String, String> workspaceFingerprints() {
			Map<String, String> fingerprints = new TreeMap<>();
			workspaces.forEach((id, snapshot) -> fingerprints.put(id, snapshot == null ? "unavailable" : snapshot.fingerprint()));
			return fingerprints;
		}

		boolean workspacesUnchanged() {
			return workspaces.entrySet().stream().allMatch(entry -> Objects.equals(entry.getValue(),
					RouterReplayWorkspace.inspect(insight, entry.getKey())));
		}

		@Override
		public boolean canReplay(Task task) {
			var workspace = task.workspaceId() == null ? null : workspaces.get(task.workspaceId());
			return workspace != null && workspace.tools().containsAll(task.requiredTools());
		}

		@Override
		public Map<String, Object> await(String runId, long timeoutMs) {
			try {
				Map<String, Object> run = AgentRunService.get().waitForRun(runId, insight, timeoutMs);
				if (Boolean.TRUE.equals(run.get("waitTimedOut"))) {
					stop(runId);
					run.put("status", "timed out");
				}
				return run;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted", e);
			}
		}

		@Override
		public String startTask(Task task, String engineId, int maxTurns) {
			if (!canReplay(task)) {
				throw new IllegalArgumentException("Original workspace cannot replay task " + task.id());
			}
			IModelEngine engine = Utility.getModel(engineId);
			if (engine == null) {
				throw new IllegalArgumentException("Unable to load engine '" + engineId + "'");
			}
			String roomId = UUID.randomUUID().toString();
			Room room = RoomUtils.createRoomIfNotExists(roomId, insight, engine, task.prompt(), null, null, null, null, null);
			try {
				Path folder = Path.of(room.getRoomFolderPath());
				RouterLearningExamples.stage(task, folder);
				AgentRunRequest request = new AgentRunRequest(roomId, task.prompt(), engineId, SemossAgentHarness.NAME, task.workspaceId(), maxTurns, 0,
						Map.of(AgentRunner.PARAM_SPACE, AssetUtility.INSIGHT_SPACE_KEY, AgentRunner.PARAM_SUBDIR, ".",
								prerna.engine.impl.model.AbstractModelEngine.BUILT_IN_TOOLS, Map.of()),
						Map.of(RouterReplayWorkspace.REPLAY_RUN, workspaces.get(task.workspaceId()).fingerprint()), insight);
				return AgentRunService.get().run(request).runId();
			} catch (IOException e) {
				throw new IllegalStateException("Could not stage the files of task " + task.id() + ": " + e.getMessage(), e);
			}
		}

		@Override
		public List<Output> outputs(Task task, String roomId) {
			try {
				Map<String, String> inputs = new HashMap<>();
				for (Path input : task.inputs()) {
					inputs.put(input.getFileName().toString(), RouterLearningExamples.fingerprint(input));
				}
				return RouterEvaluationEvidence.changed(Path.of(Room.roomFolderPath(roomId)), inputs);
			} catch (IOException e) {
				throw new IllegalStateException("Could not read the files of room " + roomId + ": " + e.getMessage(), e);
			}
		}

		@Override
		public double price(String engineId) {
			return RouterEvaluationEvidence.listPrice(engineId);
		}

		@Override
		public long costMicros(String roomId) {
			try {
				List<Map<String, Object>> rows = RouterEvidenceStore.callRows(roomId, GetRouterEvidenceReactor.loginOf(insight), false);
				return RouterEvidenceStore.totalCostMicros(rows);
			} catch (Exception e) {
				throw new IllegalStateException("Could not read run cost: " + e.getMessage(), e);
			}
		}

		@Override
		public void stop(String runId) {
			try {
				AgentRunService.get().stop(runId, insight);
			} catch (RuntimeException e) {
				classLogger.warn("Could not stop run '{}': {}", runId, e.getMessage());
			}
		}

		@Override
		public String trace(String runId, int maxChars) {
			try {
				Map<String, Object> run = AgentRunService.get().getRun(runId, insight, true);
				return compactTrace(run.get("messages"), ValueUtils.toStringOrEmpty(run.get("finalText")), maxChars);
			} catch (RuntimeException e) {
				return null;
			}
		}

		@Override
		public long maxCompareCost(Task task, Map<String, Object> candidate, List<Output> candidateFiles, Map<String, Object> reference,
				List<Output> referenceFiles, String judgeModelId) {
			Map<String, Object> price = ModelUsageCost.priceEntry(judgeModelId);
			if (price == null) {
				return -1;
			}
			String prompt = RouterEvaluationEvidence.sideBySidePrompt(task.prompt(), ValueUtils.toStringOrEmpty(candidate.get("finalText")), candidateFiles,
					ValueUtils.toStringOrEmpty(reference.get("finalText")), referenceFiles);
			return maximumJudgeCost(price, prompt);
		}

		static Room createStatelessJudgeRoom(Insight insight, IModelEngine judge) {
			String parentRoomId = insight == null ? null : insight.getInsightId();
			return RoomUtils.createRoomForStatelessAsk(UUID.randomUUID().toString(), insight, judge, "Router quality judge", null, null,
					null, null, parentRoomId);
		}

		static AskModelEngineResponse<?> askStatelessJudge(IModelEngine judge, Room room, String prompt, Map<String, Object> parameters) {
			InputMessage input = InputMessage.builder(room).withText(prompt).withModelType(judge.getModelType()).withParamMap(parameters).build();
			return judge.askRoom(input, room, parameters);
		}

		@Override
		public Judged compare(Task task, Map<String, Object> candidate, List<Output> candidateFiles, Map<String, Object> reference,
				List<Output> referenceFiles, String judgeModelId) {
			IModelEngine judge = Utility.getModel(judgeModelId);
			if (judge == null) {
				throw new IllegalArgumentException("Unable to load judge model engine '" + judgeModelId + "'");
			}
			Map<String, Object> price = ModelUsageCost.priceEntry(judgeModelId);
			if (price == null) {
				return new Judged(null, "judge pricing is unavailable; no judge request was sent", 0);
			}
			String prompt = RouterEvaluationEvidence.sideBySidePrompt(task.prompt(), ValueUtils.toStringOrEmpty(candidate.get("finalText")), candidateFiles,
					ValueUtils.toStringOrEmpty(reference.get("finalText")), referenceFiles);
			Map<String, Object> llmParams = new HashMap<>();
			llmParams.put("max_completion_tokens", 1000);
			Room judgeRoom = createStatelessJudgeRoom(insight, judge);
			AskModelEngineResponse<?> modelResponse = askStatelessJudge(judge, judgeRoom, prompt, llmParams);
			Map<String, Object> response = modelResponse.toMap();
			long micros = judgeCostMicros(price, modelResponse, response);
			Map<String, Object> verdict = AssessAgentEffectivenessReactor.parseAssessment(ValueUtils.toStringOrNull(response.get("response")));
			String answer = verdict == null ? null : ValueUtils.toStringOrNull(verdict.get("verdict"));
			Boolean equalOrBetter = answer == null ? null : "equal_or_better".equalsIgnoreCase(answer.trim()) ? Boolean.TRUE
					: "worse".equalsIgnoreCase(answer.trim()) ? Boolean.FALSE : null;
			String reason = verdict == null ? "judge returned an invalid or insufficient-evidence verdict"
					: ValueUtils.toStringOrEmpty(verdict.get("reason"));
			return new Judged(equalOrBetter, reason, micros);
		}
	}

}
