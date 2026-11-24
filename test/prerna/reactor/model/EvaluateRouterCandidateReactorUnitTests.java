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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.om.Insight;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.AgentUseAccess;
import prerna.reactor.agent.run.AgentRunHandle;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.model.RouterEvaluationEvidence.Output;
import prerna.reactor.model.RouterLearningExamples.Task;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Task set evaluation: scoring against the strongest model, paired differences, the cache, the cap and the decision. */
class EvaluateRouterCandidateReactorUnitTests {

	static final String LIVE_JSON = "{\"version\":\"v1\",\"classifier_engine\":\"clf\",\"routes\":[{\"name\":\"cheap\",\"engine_id\":\"m1\"},"
			+ "{\"name\":\"big\",\"engine_id\":\"m2\"}],\"subtasks\":[{\"id\":\"a\",\"route\":\"cheap\"}]}";

	@Test
	void liveReplayUsesEachTasksWorkspaceAndFreshStartingFilesForEveryArm() throws Exception {
		Path input = Files.writeString(cache.resolve("budget.csv"), "total\n15\n");
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		IModelEngine engine = mock(IModelEngine.class);
		AgentRunService service = mock(AgentRunService.class);
		Map<String, Path> folders = new HashMap<>();
		List<AgentRunRequest> requests = new ArrayList<>();
		Task a = new Task("a", "sum the workbook", List.of(input), "workspace-a", Set.of("ReadFile"));
		Task b = new Task("b", "sum the workbook", List.of(input), "workspace-b", Set.of());
		Task missing = new Task("missing", "sum the workbook", List.of(input), "missing", Set.of());
		Task disabled = new Task("disabled", "sum the workbook", List.of(input), "workspace-b", Set.of("ReadFile"));
		JSONObject configA = new JSONObject();
		try (var access = mockStatic(AgentUseAccess.class); var projects = mockStatic(SecurityProjectUtils.class);
				var configs = mockStatic(ModelInferenceLogsUtils.class); var utility = mockStatic(Utility.class);
				var rooms = mockStatic(RoomUtils.class); var services = mockStatic(AgentRunService.class)) {
			for (String id : List.of("workspace-a", "workspace-b")) {
				access.when(() -> AgentUseAccess.mayUseAgent(user, null, id)).thenReturn(true);
			}
			configs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("workspace-a"))
					.thenAnswer(ignored -> Map.of("config_json", configA.toString()));
			configs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("workspace-b"))
					.thenReturn(Map.of("config_json", "{\"use_default_agent_tools\":false}"));
			utility.when(() -> Utility.getModel(anyString())).thenReturn(engine);
			rooms.when(() -> RoomUtils.createRoomIfNotExists(anyString(), eq(insight), eq(engine), eq("sum the workbook"),
					isNull(), isNull(), isNull(), isNull(), isNull())).thenAnswer(call -> {
				String roomId = call.getArgument(0);
				Path folder = cache.resolve(roomId);
				folders.put(roomId, folder);
				Room room = mock(Room.class);
				when(room.getRoomFolderPath()).thenReturn(folder.toString());
				return room;
			});
			services.when(AgentRunService::get).thenReturn(service);
			when(service.run(any(AgentRunRequest.class))).thenAnswer(call -> {
				AgentRunRequest request = call.getArgument(0);
				requests.add(request);
				assertEquals(SemossAgentHarness.NAME, request.getHarnessType());
				assertEquals(AssetUtility.INSIGHT_SPACE_KEY, request.getParamMap().get(AgentRunner.PARAM_SPACE));
				assertEquals(".", request.getParamMap().get(AgentRunner.PARAM_SUBDIR));
				Path staged = folders.get(request.getRoomId()).resolve("budget.csv");
				assertEquals("total\n15\n", Files.readString(staged));
				Files.writeString(staged, "changed by replay");
				return new AgentRunHandle("run", request.getRoomId(), AgentRunStatus.SUBMITTED);
			});
			var backend = new EvaluateRouterCandidateReactor.LiveBackend(insight);
			backend.pinWorkspaces(List.of(a, b, missing, disabled));
			for (String arm : List.of("reference", "live", "candidate")) {
				assertEquals("run", backend.startTask(a, arm, 10));
				assertEquals("run", backend.startTask(b, arm, 10));
			}
			assertThrows(IllegalArgumentException.class, () -> backend.startTask(missing, "candidate", 10));
			assertThrows(IllegalArgumentException.class, () -> backend.startTask(disabled, "candidate", 10));
			assertEquals(List.of("workspace-a", "workspace-b", "workspace-a", "workspace-b", "workspace-a", "workspace-b"),
					requests.stream().map(AgentRunRequest::getWorkspaceId).toList());
			assertEquals(6, folders.size());
			assertEquals("total\n15\n", Files.readString(input));
			assertTrue(backend.workspacesUnchanged());
			configA.put("mcps", List.of(Map.of("id", "external")));
			assertFalse(backend.workspacesUnchanged());
		}
	}

	@Test
	void unavailableTasksDoNotRunOrCountAsCandidateFailures() {
		Fake source = new Fake() {
			@Override
			public boolean canReplay(Task task) {
				return !task.id().equals("gone");
			}
		};
		source.good("cand", true, "available").good("live", true, "available");
		var result = new EvaluateRouterCandidateReactor().evaluateSet(source, job(tasks("available", "gone"), "workspace-skip", 100_000));
		assertFalse(source.started.stream().anyMatch(run -> run.contains("gone")));
		assertEquals(1, result.get("skipped_tasks"));
		assertEquals(1, result.get("n"));
		assertEquals(false, result.get("complete"));
		assertEquals("replay_workspace_unavailable", result.get("stop_reason"));
	}

	/** Scripted: the judge finds a task on an engine at least as good as the reference when {@code good[engine:task]}, a run costs {@code cost[engine]}. */
	private static class Fake implements EvaluateRouterCandidateReactor.Backend {
		final Map<String, Boolean> good = new HashMap<>();
		final Map<String, List<Output>> files = new HashMap<>();
		final Map<String, Long> cost = new HashMap<>();
		final Map<String, String[]> runs = new ConcurrentHashMap<>();
		final List<String> started = Collections.synchronizedList(new ArrayList<>());
		final AtomicInteger ids = new AtomicInteger();
		final AtomicInteger judged = new AtomicInteger();
		final ConcurrentLinkedQueue<String> judgePrompts = new ConcurrentLinkedQueue<>();
		String failedRunId;
		long judgeEstimate = 7;
		boolean unreadableCost;
		boolean unparseableJudge;
		String judgeFailure;

		Fake good(String engine, boolean value, String... tasks) {
			Arrays.stream(tasks).forEach(t -> good.put(engine + ":" + t, value));
			return this;
		}

		public Map<String, Object> await(String runId, long timeoutMs) {
			return new HashMap<>(Map.of("runId", runId, "roomId", "room-" + runId, "status", runId.equals(failedRunId) ? "FAILED" : "COMPLETED", "finalText", "answer from " + runId));
		}

		public String startTask(Task task, String engineId, int maxTurns) {
			String id = "run-" + ids.incrementAndGet();
			runs.put(id, new String[] { engineId, task.id() });
			started.add(engineId + ":" + task.id());
			return id;
		}

		@Override
		public String trace(String runId, int maxChars) {
			return "trace of " + runs.get(runId)[0] + " on " + runs.get(runId)[1];
		}

		public List<Output> outputs(Task task, String roomId) {
			return files.getOrDefault(runs.get(roomId.substring(5))[0] + ":" + task.id(), List.of());
		}

		public double price(String engineId) {
			return Map.of("m1", 1.0, "m2", 30.0).getOrDefault(engineId, 0.0);
		}

		public long costMicros(String roomId) {
			if (unreadableCost) {
				throw new IllegalStateException("no cost");
			}
			return cost.getOrDefault(runs.get(roomId.substring(5))[0], 10L);
		}

		public void stop(String runId) {
		}

		@Override
		public long maxCompareCost(Task task, Map<String, Object> candidate, List<Output> candidateFiles,
				Map<String, Object> reference, List<Output> referenceFiles, String judgeModelId) {
			judgePrompt(task, candidate, candidateFiles, reference, referenceFiles);
			return judgeEstimate;
		}

		public EvaluateRouterCandidateReactor.Judged compare(Task task, Map<String, Object> candidate, List<Output> candidateFiles,
				Map<String, Object> reference, List<Output> referenceFiles, String judgeModelId) {
			judged.incrementAndGet();
			judgePrompts.add(judgePrompt(task, candidate, candidateFiles, reference, referenceFiles));
			if (judgeFailure != null) {
				throw new IllegalStateException(judgeFailure);
			}
			String[] of = runs.get(candidate.get("runId"));
			return new EvaluateRouterCandidateReactor.Judged(unparseableJudge ? null : good.get(of[0] + ":" + of[1]), "reason for " + of[1], 7);
		}

		private String judgePrompt(Task task, Map<String, Object> candidate, List<Output> candidateFiles,
				Map<String, Object> reference, List<Output> referenceFiles) {
			return RouterEvaluationEvidence.sideBySidePrompt(task.prompt(), (String) candidate.get("finalText"), candidateFiles,
					(String) reference.get("finalText"), referenceFiles);
		}
	}

	@TempDir
	Path cache;

	List<Task> tasks(String... ids) {
		return Arrays.stream(ids).map(id -> new Task(id, "prompt " + id, List.of())).toList();
	}

	EvaluateRouterCandidateReactor.SetJob job(List<Task> tasks, String manifestSha, long cap) {
		return new EvaluateRouterCandidateReactor.SetJob("set", tasks, manifestSha, cache, "cand", "live", LIVE_JSON, "m2", "judge", cap, new prerna.engine.impl.model.ModelRouterEngine.ReplaySettings());
	}

	Fake fake() {
		Fake f = new Fake();
		f.cost.put("cand", 10L);
		f.cost.put("live", 20L);
		f.cost.put("m2", 40L);
		return f;
	}

	Map<String, Object> run(Fake f, EvaluateRouterCandidateReactor.SetJob job) {
		return new EvaluateRouterCandidateReactor().evaluateSet(f, job);
	}

	@Test
	void eachJudgeComparisonGetsItsOwnStatelessRoomLinkedToTheEvaluatorInsightRoom() {
		Insight insight = mock(Insight.class);
		IModelEngine judge = mock(IModelEngine.class);
		when(insight.getInsightId()).thenReturn("evaluator-insight-room");
		List<String> roomIds = new ArrayList<>();
		try (MockedStatic<RoomUtils> roomUtils = mockStatic(RoomUtils.class)) {
			roomUtils.when(() -> RoomUtils.createRoomForStatelessAsk(anyString(), eq(insight), eq(judge), eq("Router quality judge"), isNull(),
					isNull(), isNull(), isNull(), eq("evaluator-insight-room"))).thenAnswer(invocation -> {
						roomIds.add(invocation.getArgument(0));
						return mock(Room.class);
					});
			EvaluateRouterCandidateReactor.LiveBackend.createStatelessJudgeRoom(insight, judge);
			EvaluateRouterCandidateReactor.LiveBackend.createStatelessJudgeRoom(insight, judge);
		}
		assertEquals(2, roomIds.size());
		assertNotEquals(roomIds.get(0), roomIds.get(1));
	}

	@Test
	void independentStatelessJudgeRoomsAllowConcurrentComparisons() throws Exception {
		IModelEngine judge = mock(IModelEngine.class);
		Room firstRoom = new Room();
		firstRoom.setId("judge-room-one");
		Room secondRoom = new Room();
		secondRoom.setId("judge-room-two");
		CountDownLatch callsEntered = new CountDownLatch(2);
		CountDownLatch releaseCalls = new CountDownLatch(1);
		when(judge.askRoom(any(InputMessage.class), any(Room.class), anyMap())).thenAnswer(invocation -> {
			callsEntered.countDown();
			if (!releaseCalls.await(5, TimeUnit.SECONDS)) {
				throw new IllegalStateException("judge call timed out waiting for the concurrent test");
			}
			return new AskStringModelEngineResponse("verdict", 1, 1);
		});
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			Future<AskModelEngineResponse<?>> first = executor.submit(
					() -> EvaluateRouterCandidateReactor.LiveBackend.askStatelessJudge(judge, firstRoom, "prompt one", Map.of()));
			Future<AskModelEngineResponse<?>> second = executor.submit(
					() -> EvaluateRouterCandidateReactor.LiveBackend.askStatelessJudge(judge, secondRoom, "prompt two", Map.of()));
			boolean bothCallsEntered = callsEntered.await(2, TimeUnit.SECONDS);
			releaseCalls.countDown();
			assertEquals("verdict", first.get(5, TimeUnit.SECONDS).getStringResponse());
			assertEquals("verdict", second.get(5, TimeUnit.SECONDS).getStringResponse());
			assertTrue(bothCallsEntered, "both judge requests should enter before either is released");
			verify(judge, times(2)).askRoom(any(InputMessage.class), any(Room.class), anyMap());
		} finally {
			releaseCalls.countDown();
			executor.shutdownNow();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void scoresAgainstTheStrongestModelPairsWithTheLiveRouterAndCachesBothBaselines() {
		Fake f = fake().good("live", true, "x1", "x2", "x3", "y1").good("cand", true, "x1", "x3", "y1").good("cand", false, "x2");
		Map<String, Object> out = run(f, job(tasks("x1", "x2", "x3", "y1"), "m", 10_000));
		assertEquals(4, out.get("n"));
		assertEquals(-0.25, (double) out.get("quality_delta_vs_live"), 1e-9);
		assertEquals(0.25, (double) out.get("quality_se"), 1e-9);
		assertEquals(-10.0, (double) out.get("cost_delta_vs_live"), 1e-9);
		assertEquals(0.75, (double) out.get("quality"), 1e-9);
		assertEquals(1.0, (double) out.get("live_quality"), 1e-9);
		assertEquals("v1", out.get("live_version"));
		assertTrue(((String) out.get("decision")).startsWith("needs 16 more tasks"), "four tasks are too few to decide");
		List<Map<String, Object>> digest = (List<Map<String, Object>>) out.get("failure_digest");
		assertEquals(List.of("x2"), digest.stream().map(d -> d.get("task")).toList());
		assertEquals("reason for x2", digest.get(0).get("detail"));
		assertEquals("trace of cand on x2", digest.get(0).get("candidate_trace"));
		assertEquals("trace of m2 on x2", digest.get(0).get("reference_trace"));
		// a second candidate on the same set reuses the strongest and live results: only its own tasks run, twice each
		f.started.clear();
		run(f, job(tasks("x1", "x2", "x3", "y1"), "m", 10_000));
		assertEquals(8, f.started.size());
		assertTrue(f.started.stream().allMatch(s -> s.startsWith("cand:")));
		// a changed manifest is a cache miss
		f.started.clear();
		run(f, job(tasks("x1", "x2", "x3", "y1"), "m2", 10_000));
		assertEquals(24, f.started.size());
	}

	@Test
	void aCandidateTheGateWouldApplyIsSaidSoInWords() {
		List<String> ids = java.util.stream.IntStream.range(0, 20).mapToObj(i -> "t" + i).toList();
		Fake f = fake();
		ids.forEach(id -> f.good("live", true, id).good("cand", true, id));
		String decision = (String) run(f, job(tasks(ids.toArray(new String[0])), "m", 1_000_000)).get("decision");
		assertTrue(decision.startsWith("the candidate would be applied"), decision);
	}

	@Test
	void matchingAndDifferentWorkbooksAreScoredOnlyByTheJudge() throws Exception {
		Fake f = fake().good("live", false, "x1").good("cand", true, "x1");
		Output matching = RouterEvaluationEvidenceUnitTests.workbook("out.xlsx", 12);
		f.files.put("m2:x1", List.of(matching));
		f.files.put("live:x1", List.of(matching));
		f.files.put("cand:x1", List.of(RouterEvaluationEvidenceUnitTests.workbook("out.xlsx", 24)));
		Map<String, Object> out = run(f, job(tasks("x1"), "m", 10_000));
		assertEquals(1.0, out.get("quality"), "the judge passes the candidate even with different workbook values");
		assertEquals(0.0, out.get("live_quality"), "the judge rejects the live result even with identical workbook bytes");
		assertEquals(4, f.judged.get());
		assertTrue(f.judgePrompts.stream().allMatch(p -> p.split("out\\.xlsx", -1).length == 3));
		assertEquals(2, f.judgePrompts.stream().filter(p -> p.contains("A1=24")).count());
		assertTrue(f.judgePrompts.stream().allMatch(p -> p.contains("A1=12") && p.contains("Final answer: answer from run-")));
	}

	@Test
	void anUnreadableWorkbookStopsTheEvaluationWithoutAJudgeCall() throws Exception {
		Fake f = fake().good("cand", true, "x1").good("live", true, "x1");
		f.files.put("m2:x1", List.of(RouterEvaluationEvidenceUnitTests.workbook("out.xlsx", 12)));
		f.files.put("cand:x1", List.of(new Output("out.xlsx", new byte[] { 1, 2, 3 })));
		Map<String, Object> out = run(f, job(tasks("x1"), "m", 10_000));
		assertEquals(0, f.judged.get());
		assertEquals(false, out.get("complete"));
		assertEquals("judge_evidence_incomplete", out.get("stop_reason"));
		assertEquals(Map.of(), out.get("candidate_task_scores"));
	}

	@Test
	void aLateUnjudgedRunChargeAboveTheCapMakesTheEvaluationIncomplete() {
		Fake f = fake().good("cand", true, "x1").good("live", true, "x1");
		f.failedRunId = "run-6"; // final live repeat: billed even though it needs no judge
		Map<String, Object> out = run(f, job(tasks("x1"), "m", 160));
		assertEquals(6, f.started.size());
		assertEquals(3, f.judged.get());
		assertEquals(161L, out.get("spent_micros"));
		assertEquals(false, out.get("complete"));
		assertEquals(true, out.get("stopped_by_cap"));
		assertEquals("cap", out.get("stop_reason"));
		assertEquals("the evaluation stopped early (cap)", out.get("decision"));

		Fake exactlyAtCap = fake().good("cand", true, "x1").good("live", true, "x1");
		exactlyAtCap.failedRunId = "run-6";
		Map<String, Object> exact = run(exactlyAtCap, job(tasks("x1"), "another-manifest", 161));
		assertEquals(161L, exact.get("spent_micros"));
		assertEquals(true, exact.get("complete"));
		assertEquals(false, exact.get("stopped_by_cap"));
	}

	@Test
	void aJudgeCallWhichCannotFitIsNotDispatched() {
		Fake f = fake().good("cand", true, "x1");
		f.judgeEstimate = 100;
		Map<String, Object> result = run(f, job(tasks("x1"), "m", 60));
		assertEquals(0, f.judged.get());
		assertEquals(false, result.get("complete"));
		assertEquals("cap", result.get("stop_reason"));
	}

	@Test
	void theStrongestModelRunsFirstSoACapHitLeavesNoCandidateRunsWithoutIt() {
		Fake f = fake().good("cand", true, "x1", "x2", "x3").good("live", true, "x1", "x2", "x3");
		Map<String, Object> out = run(f, job(tasks("x1", "x2", "x3"), "m", 20));
		assertEquals(0, out.get("n"), "the strongest runs cost 40 micros each and use the cap up first");
		assertEquals(true, out.get("stopped_by_cap"));
		assertEquals("cap", out.get("stop_reason"));
		assertTrue(f.started.stream().noneMatch(s -> s.startsWith("cand:")), "no candidate run started");
		assertEquals("the evaluation stopped early (cap)", out.get("decision"));
	}

	@Test
	void anUnreadableCostFailsClosed() {
		Fake f = fake();
		f.unreadableCost = true;
		Map<String, Object> out = run(f, job(tasks("x1"), "m", 10_000));
		assertEquals(0, out.get("n"));
		assertEquals("cost_unreadable", out.get("stop_reason"));
		assertEquals(1, f.started.size(), "nothing further starts");
	}

	@Test
	void judgeCostUsesReportedCacheUsageAndRequiresPromptAndResponseCounts() {
		Map<String, Object> price = Map.of("input", 2.2, "output", 11.0, "cache_read", 0.11, "cache_write", 2.75);
		AskStringModelEngineResponse cached = new AskStringModelEngineResponse("verdict", 1000, 100);
		cached.setNumberOfCacheReadTokens(800);
		cached.setNumberOfCacheCreationTokens(100);
		assertEquals(1683L, EvaluateRouterCandidateReactor.judgeCostMicros(price, cached, cached.toMap()));

		AskStringModelEngineResponse uncached = new AskStringModelEngineResponse("verdict", 1000, 100);
		assertEquals(3300L, EvaluateRouterCandidateReactor.judgeCostMicros(price, uncached, uncached.toMap()));

		AskStringModelEngineResponse missingPrompt = new AskStringModelEngineResponse("verdict", null, 100);
		assertThrows(IllegalStateException.class,
				() -> EvaluateRouterCandidateReactor.judgeCostMicros(price, missingPrompt, missingPrompt.toMap()));
		AskStringModelEngineResponse missingResponse = new AskStringModelEngineResponse("verdict", 1000, null);
		assertThrows(IllegalStateException.class,
				() -> EvaluateRouterCandidateReactor.judgeCostMicros(price, missingResponse, missingResponse.toMap()));
	}

	@Test
	void runsAreAveragedTheJudgesTokensCountAndAnUnparseableAnswerFailsClosed() {
		Fake f = fake().good("cand", true, "t1").good("live", true, "t1");
		Map<String, Object> out = run(f, job(tasks("t1"), "m", 1000));
		assertEquals(1.0, (double) out.get("quality"), 1e-9);
		assertEquals(10.0, (double) out.get("cost_micros_per_task"), 1e-9, "cost is per run");
		assertEquals(2 * 40 + 2 * (10 + 7) + 2 * (20 + 7), (long) out.get("spent_micros"), "the judge's tokens count against the cap");
		f.unparseableJudge = true;
		int judgedBeforeInvalid = f.judged.get();
		Map<String, Object> incomplete = run(f, job(tasks("t2"), "m", 1000));
		assertEquals(false, incomplete.get("complete"));
		assertEquals("judge_verdict_unreadable", incomplete.get("stop_reason"));
		assertEquals(null, incomplete.get("quality"), "an invalid verdict is unknown rather than a reliable wrong-answer score");
		assertEquals(1, f.judged.get() - judgedBeforeInvalid, "the paid judge request is not retried");
		assertEquals(97L, incomplete.get("spent_micros"), "known run and judge charges are retained");
	}

	@Test
	void aBusyJudgeRoomFailureIsNotRetriedAndStopsTheEvaluationFailClosed() {
		Fake f = fake().good("cand", true, "t1").good("live", true, "t1");
		f.judgeFailure = "Room is busy; another request is updating room messages. Please retry.";
		Map<String, Object> out = run(f, job(tasks("t1"), "busy-judge-room", 1000));
		assertEquals(false, out.get("complete"));
		assertEquals("judge_cost_or_evidence_unreadable", out.get("stop_reason"));
		assertEquals(1, f.judged.get(), "the busy error does not trigger a possibly billed retry");
		assertEquals(97L, out.get("spent_micros"), "unknown judge spend keeps its reservation");
		assertEquals(Map.of(), out.get("candidate_task_scores"), "an incomplete judge verdict provides no quality evidence");
	}

	@Test
	void compactTraceKeepsTheStartTheEndAndTheFinalAnswer() {
		List<Object> messages = new ArrayList<>();
		for (int i = 0; i < 40; i++) {
			messages.add(Map.of("parts", List.of(Map.of("type", "TOOL_CALL", "toolCall", Map.of("name", "tool" + i, "arguments", "{}")))));
		}
		String trace = EvaluateRouterCandidateReactor.compactTrace(messages, "done", 600);
		assertTrue(trace.startsWith("call tool0 ") && trace.contains("lines omitted") && trace.contains("tool39") && trace.endsWith("FINAL: done"));
	}
}
