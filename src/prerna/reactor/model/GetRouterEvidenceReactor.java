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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.inferencetracking.RouterEvidenceStore;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.run.AgentRunService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * Read-only evidence for improving a model router. Steps come from the routing trace the router persists on each
 * response message ({@code ornaments.routing.trace}); their cost from the {@code MESSAGE} rows, joined on
 * {@code TRANSACTION_ID}. They are tallied per (subtask x served model), for the optimizer, and per router config
 * version, for the status read. Pooling rule: the owner of the router reads the runs of all
 * its users, everyone else only their own.
 */
public class GetRouterEvidenceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetRouterEvidenceReactor.class);

	private static final String ENGINE_ID_KEY = "engineId";
	private static final String SAMPLE_KEY = "sample";
	private static final int MAX_SAMPLE = 20;
	private static final int TEXT_CAP = 240;
	private static final String NONE = "(none)";
	/** The newest runs read per call. */
	private static final int MAX_RUNS = 500;

	public GetRouterEvidenceReactor() {
		this.keysToGet = new String[] { ENGINE_ID_KEY, SAMPLE_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = ValueUtils.trimToNull(this.keyValue.get(ENGINE_ID_KEY));
		if (engineId == null) {
			throw new IllegalArgumentException("engineId is required");
		}
		Integer sample = ValueUtils.parseWholeNumber(this.keyValue.get(SAMPLE_KEY));
		if (sample != null && (sample < 0 || sample > MAX_SAMPLE)) {
			throw new IllegalArgumentException(SAMPLE_KEY + " must be a whole number from 0 to " + MAX_SAMPLE);
		}
		try {
			return new NounMetadata(collect(this.insight, engineId, 30, sample == null ? 0 : sample), PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new IllegalStateException("Could not read router evidence: " + e.getMessage(), e);
		}
	}

	/**
	 * Reads the runs of the last {@code days} days that used the router and tallies them (all users' when the caller owns it, else the
	 * caller's own); shared with the status read. With {@code sample} above 0 it also returns that many steps per subtask.
	 */
	static Map<String, Object> collect(Insight insight, String engineId, int days, int sample) throws Exception {
		AccessToken login = loginOf(insight);
		boolean pooled = pooled(insight, engineId);
		List<Map<String, Object>> runs = new ArrayList<>();
		String attemptRoom = insight.getRoomId();
		Map<String, Object> attempt = RunRouterLearningIfDueReactor.attempt(engineId, attemptRoom);
		if (attempt != null && !RunRouterLearningIfDueReactor.ownedBy(attempt, login)) {
			throw new IllegalStateException("No training data assigned to this learning attempt");
		}
		List<String> ids = attempt == null ? RouterEvidenceStore.runIds(login, engineId, Instant.now().minus(days, ChronoUnit.DAYS), MAX_RUNS, pooled)
				: RouterLearningExamples.sourceRuns(RouterLearningExamples.setDir(EvaluateRouterCandidateReactor.evidenceDir(engineId).resolve("replay"), (String) attempt.get("train")),
						(String) attempt.get("train_sha256"));
		for (String runId : ids) {
			try {
				runs.add(run(insight, engineId, runId, pooled));
			} catch (RuntimeException e) {
				classLogger.warn("Skipping run '{}' in router evidence: {}", runId, e.getMessage());
			}
		}
		Map<String, List<Map<String, Object>>> rowsByRoom = new HashMap<>();
		return aggregate(engineId, runs, sample, roomId -> rowsByRoom.computeIfAbsent(roomId, room -> {
			try {
				return RouterEvidenceStore.callRows(room, login, pooled);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}));
	}

	/** The pooling rule: the owner of a router reads the evidence of every user of it, everyone else only their own runs. */
	static boolean pooled(Insight insight, String engineId) {
		return SecurityEngineUtils.userIsOwner(insight.getUser(), engineId);
	}

	/** Runs of the router recorded since its last learning attempt, counted under the pooling rule. */
	static long newTasksSinceLastAttempt(Insight insight, String engineId) throws Exception {
		return RouterEvidenceStore.runsSinceLastAttempt(loginOf(insight), engineId, RunRouterLearningIfDueReactor.attemptRoomIds(engineId), pooled(insight, engineId));
	}

	/** One run with its messages; a pooled read goes through the owner-checked path that may read other users' runs of the router. */
	static Map<String, Object> run(Insight insight, String engineId, String runId, boolean pooled) {
		return pooled ? AgentRunService.get().getRunOfOwnedRouter(runId, insight, engineId)
				: AgentRunService.get().getRun(runId, insight, true);
	}

	/** Tallies the routed steps of runs ({@code runId}, {@code status}, {@code roomId}, {@code messages}). */
	@SuppressWarnings("unchecked")
	static Map<String, Object> aggregate(String routerEngineId, List<Map<String, Object>> runs, int sample,
			Function<String, List<Map<String, Object>>> callRows) {
		Map<String, Integer> sampled = new HashMap<>();
		List<Map<String, Object>> stepSample = new ArrayList<>();
		Map<String, Tally> cells = new TreeMap<>();
		Map<String, Tally> versions = new TreeMap<>();
		int steps = 0;
		for (Map<String, Object> run : runs) {
			String runId = ValueUtils.toStringOrEmpty(run.get("runId"));
			String status = ValueUtils.toStringOrEmpty(run.get("status"));
			Map<String, Long> costByTransaction = null;
			for (Map<String, Object> message : (List<Map<String, Object>>) run.getOrDefault("messages", List.of())) {
				Map<String, Object> routing = map(map(message.get("ornaments")).get("routing"));
				String router = ValueUtils.trimToNull(routing.get("router_engine_id"));
				if (routing.isEmpty() || (router != null && !router.equals(routerEngineId))) {
					continue;
				}
				if (costByTransaction == null) {
					costByTransaction = new HashMap<>();
					for (Map<String, Object> row : callRows.apply(ValueUtils.toStringOrEmpty(run.get("roomId")))) {
						if (row.get("COST_MICROS") != null) {
							costByTransaction.merge(ValueUtils.toStringOrEmpty(row.get("TRANSACTION_ID")), number(row.get("COST_MICROS")), Long::sum);
						}
					}
				}
				Map<String, Object> trace = map(routing.get("trace"));
				String subtask = orElse(trace.get("subtask_id"), NONE);
				String served = orElse(trace.get("served_engine_id"), orElse(routing.get("routed_engine_id"), "unknown"));
				String version = orElse(trace.get("config_version"), NONE);
				String transaction = ValueUtils.toStringOrEmpty(message.get("transactionId"));
				Long cost = costByTransaction.get(transaction);
				boolean down = Boolean.FALSE.equals(map(message.get("feedback")).get("rating"));
				cells.computeIfAbsent(subtask + "\u0000" + served, k -> new Tally("subtask_id", subtask, "served_engine_id", served))
						.add(runId, status, cost, down);
				versions.computeIfAbsent(version, k -> new Tally("config_version", version, null, null)).add(runId, status, cost,
						down);
				steps++;
				if (sample > 0 && sampled.merge(subtask, 1, Integer::sum) <= sample) {
					stepSample.add(sampleOf(runId, status, subtask, served, cost, map(trace.get("classifier_inputs"))));
				}
			}
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("router_engine_id", routerEngineId);
		out.put("runs_scanned", runs.size());
		out.put("routed_steps", steps);
		out.put("cells", cells.values().stream().map(Tally::toMap).toList());
		out.put("by_version", versions.values().stream().map(Tally::toMap).toList());
		if (sample > 0) {
			out.put("step_sample", stepSample);
		}
		return out;
	}

	/** One routed step as the classifier saw it (goal, previous subtask, recent steps) with its outcome and cost, for mining subtasks. */
	private static Map<String, Object> sampleOf(String runId, String status, String subtask, String served, Long cost,
			Map<String, Object> inputs) {
		Map<String, Object> step = new LinkedHashMap<>();
		step.put("run_id", runId);
		step.put("subtask_id", subtask);
		step.put("served_engine_id", served);
		step.put("run_status", status);
		step.put("cost_micros", cost);
		step.put("goal", cap(inputs.get("goal")));
		step.put("previous_subtask", inputs.get("previous_subtask"));
		List<String> recent = new ArrayList<>();
		if (inputs.get("recent_steps") instanceof List<?> list) {
			list.forEach(r -> recent.add(cap(r)));
		}
		step.put("recent_steps", recent);
		return step;
	}

	private static String cap(Object text) {
		String value = ValueUtils.toStringOrEmpty(text);
		return value.length() <= TEXT_CAP ? value : value.substring(0, TEXT_CAP) + "...";
	}

	/** Totals of a group of steps: a (subtask, served model) cell or a config version. */
	private static final class Tally {
		final Map<String, Object> label = new LinkedHashMap<>();
		final Map<String, String> runs = new LinkedHashMap<>();
		int steps;
		int pricedSteps;
		long costMicros;
		int down;

		Tally(String key1, String value1, String key2, String value2) {
			label.put(key1, value1);
			if (key2 != null) {
				label.put(key2, value2);
			}
		}

		/** {@code cost} is null for a step without a priced MESSAGE row. */
		void add(String runId, String status, Long cost, boolean thumbsDown) {
			steps++;
			runs.put(runId, status);
			if (cost != null) {
				pricedSteps++;
				costMicros += cost;
			}
			down += thumbsDown ? 1 : 0;
		}

		Map<String, Object> toMap() {
			Map<String, Object> map = new LinkedHashMap<>(label);
			map.put("steps", steps);
			map.put("avg_cost_micros_per_priced_step", pricedSteps == 0 ? null : (double) costMicros / pricedSteps);
			map.put("runs", runs.size());
			long failed = runs.values().stream().filter("FAILED"::equals).count();
			map.put("failed_runs", failed);
			map.put("failed_run_share", (double) failed / runs.size());
			map.put("thumbs_down", down);
			return map;
		}
	}

	private static String orElse(Object value, String fallback) {
		String text = ValueUtils.trimToNull(value);
		return text == null ? fallback : text;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return value instanceof Map ? (Map<String, Object>) value : new HashMap<>();
	}

	static long number(Object value) {
		return value instanceof Number ? ((Number) value).longValue() : 0L;
	}

	/** The caller's primary login: the id and login type every user-scoped read takes. */
	static AccessToken loginOf(Insight insight) {
		AccessToken login = insight == null || insight.getUser() == null ? null : insight.getUser().getPrimaryLoginToken();
		if (login == null || login.getProvider() == null) {
			throw new SecurityException("A signed in user is required");
		}
		return login;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Reads routing, cost, and failure evidence; the optimizer sees only its assigned training runs")
				.key(ENGINE_ID_KEY, "The model router engine id")
				.key(SAMPLE_KEY, "Number of sample steps per subtask (0 to 20)")
				.output(Map.class, "Routing and cost evidence by subtask, served model, and router version");
	}
}
