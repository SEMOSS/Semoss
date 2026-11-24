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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.cluster.util.ClusterUtil;
import prerna.usertracking.UserAuditTrailUtils;

/** Applying a candidate: the gate on a stored evaluation, which must be the caller's and current, one apply per evaluation. */
class ApplyRouterCandidateReactorUnitTests {

	static final String LIVE_IMPROVEMENT = "\"improvement\":{\"enabled\":true,\"max_quality_loss\":0.05}";

	static String config(String version, String improvement) {
		return "{\"mode\":\"subtask\",\"classifier_engine\":\"clf\",\"default_subtask\":\"a\",\"version\":\"" + version
				+ "\",\"routes\":[{\"name\":\"r1\",\"engine_id\":\"m1\"}],\"subtasks\":[{\"id\":\"a\",\"route\":\"r1\","
				+ "\"recognition_rules\":[\"x\"]}]" + (improvement == null ? "" : "," + improvement) + "}";
	}

	/**
	 * Stubs the static helpers of UpdateModelRouterConfigReactor: engines by id, a version folder, and a save that records
	 * (json, note) and becomes the engine's next config. Close it after the test.
	 */
	static MockedStatic<UpdateModelRouterConfigReactor> stubUpdater(Map<String, IModelRouterEngine> engines, Path folder, List<String[]> saves) {
		MockedStatic<UpdateModelRouterConfigReactor> s = mockStatic(UpdateModelRouterConfigReactor.class);
		s.when(() -> UpdateModelRouterConfigReactor.editableRouter(any(), any())).thenAnswer(i -> engines.get(i.<String>getArgument(1)));
		s.when(() -> UpdateModelRouterConfigReactor.readConfig(any())).thenAnswer(i -> i.<IModelRouterEngine>getArgument(0).readConfigJson());
		s.when(() -> UpdateModelRouterConfigReactor.versionFolder(any())).thenReturn(folder.toString());
		s.when(() -> UpdateModelRouterConfigReactor.configPath(any())).thenReturn("assets/router.json");
		s.when(() -> UpdateModelRouterConfigReactor.save(any(), any(), any(), any(), any())).thenAnswer(i -> {
			ModelRouterEngine.parseAndValidateConfig(i.getArgument(3), "test", "e");
			saves.add(new String[] { i.getArgument(3), i.getArgument(4) });
			when(i.<IModelRouterEngine>getArgument(2).readConfigJson()).thenReturn(i.<String>getArgument(3));
			return null;
		});
		return s;
	}

	/** Commits each config in order to assets/router.json of the folder (message, json pairs). */
	static void commits(Path folder, String... messageAndJson) throws Exception {
		java.nio.file.Files.createDirectories(folder.resolve("assets"));
		try (Git git = Git.init().setDirectory(folder.toFile()).call()) {
			for (int i = 0; i < messageAndJson.length; i += 2) {
				java.nio.file.Files.writeString(folder.resolve("assets/router.json"), messageAndJson[i + 1]);
				git.add().addFilepattern("assets/router.json").call();
				git.commit().setMessage(messageAndJson[i]).setAuthor("t", "t@t").setCommitter("t", "t@t").call();
			}
		}
	}

	/** Gives the reactor an insight and string nouns (key, value pairs). */
	static <T extends AbstractReactor> T with(T reactor, Insight insight, String... keyValues) {
		reactor.setInsight(insight);
		for (int i = 0; i < keyValues.length; i += 2) {
			reactor.getNounStore().makeNoun(keyValues[i]).add(keyValues[i + 1], PixelDataType.CONST_STRING);
		}
		return reactor;
	}

	@TempDir
	Path dir;

	AccessToken login = mock(AccessToken.class);
	IModelRouterEngine live = mock(IModelRouterEngine.class);
	IModelRouterEngine candidate = mock(IModelRouterEngine.class);
	List<String[]> saves = new java.util.ArrayList<>();
	MockedStatic<UpdateModelRouterConfigReactor> updater;
	MockedStatic<EvaluateRouterCandidateReactor> assets;
	// the candidate's own lenient limit must not be what is applied
	String candidateJson = config("v2", "\"improvement\":{\"enabled\":true,\"max_quality_loss\":1}");

	@BeforeEach
	void setUp() throws Exception {
		when(login.getId()).thenReturn("u1");
		when(login.getProvider()).thenReturn(AuthProvider.NATIVE);
		when(live.readConfigJson()).thenReturn(config("v1", LIVE_IMPROVEMENT));
		when(candidate.readConfigJson()).thenReturn(candidateJson);
		updater = stubUpdater(Map.of("live", live, "cand", candidate), dir, saves);
		// the engine's assets folder is a temp dir; everything else is real
		assets = mockStatic(EvaluateRouterCandidateReactor.class, i -> i.getMethod().getName().equals("evidenceDir") ? dir : i.callRealMethod());
	}

	@AfterEach
	void tearDown() {
		updater.close();
		assets.close();
	}

	private void store(String id, String user, int n, double quality, double cost) throws Exception {
		Map<String, Object> record = new HashMap<>();
		record.put("candidateEngineId", "cand");
		record.put("candidateSha256", ModelRouterEngine.sha256(candidateJson));
		record.put("userId", user);
		record.put("userType", AuthProvider.NATIVE.getLabel());
		String attempt = RunRouterLearningIfDueReactor.newLearningRoom();
		EvaluateRouterCandidateReactor.save(RunRouterLearningIfDueReactor.attemptDir("live"), attempt, Map.of("train", "train", "val", "set", "val_sha256", "frozen", "userId", user, "userType", AuthProvider.NATIVE.getLabel()));
		record.put("attempt_id", attempt);
		record.put("split", "val");
		record.put("scoring", RouterEvaluationEvidence.SCORING);
		record.put("manifest_sha256", "frozen");
		record.put("live_skill_sha256", Map.of());
		record.put("candidate_skill_sha256", Map.of());
		record.put("live_config_sha256", ModelRouterEngine.sha256(live.readConfigJson()));
		record.put("task_set", "set");
		record.put("n", n);
		record.put("complete", true);
		record.put("quality_delta_vs_live", quality);
		record.put("quality_se", 0.01);
		record.put("cost_delta_vs_live", cost);
		record.put("cost_se", 1.0);
		record.put("live_engine_id", "live");
		record.put("live_version", "v1");
		record.put("stopped_by_cap", false);
		record.put("strong_engine", "strong");
		record.put("judge_engine", "judge");
		record.put("reference_cost_micros_per_task", 100.0);
		Map<String, Object> candidateScores = new HashMap<>();
		Map<String, Object> liveScores = new HashMap<>();
		for (int i = 0; i < n; i++) {
			candidateScores.put("task-" + i, Map.of("score", 0.9, "cost_micros", 50));
			liveScores.put("task-" + i, Map.of("score", 0.9, "cost_micros", 100));
		}
		record.put("candidate_task_scores", candidateScores);
		record.put("live_task_scores", liveScores);
		EvaluateRouterCandidateReactor.save(EvaluateRouterCandidateReactor.evaluationDir("cand"), id, record);
	}

	private Map<String, Object> apply(String id) {
		return new ApplyRouterCandidateReactor().apply(login, "live", live, "cand", candidate, id);
	}

	private String reason(String id) {
		Map<String, Object> result = apply(id);
		assertEquals(false, result.get("applied"));
		return result.get("reason").toString();
	}

	private static String refusal(int n, double quality, double cost, double allowedLoss) {
		return RouterGate.refusal(new HashMap<>(Map.of("complete", true, "n", n, "quality_delta_vs_live", quality, "quality_se", 0.01,
				"cost_delta_vs_live", cost, "cost_se", 1.0)), allowedLoss);
	}

	@Test
	void theStoredNumbersGoThroughTheGateAndIncompleteEvaluationsAreRefused() {
		assertNull(refusal(20, 0.0, -10.0, 0.05));
		assertTrue(refusal(20, -0.1, -10.0, 0.05).contains("10 points lower"));
		assertNull(refusal(20, -0.1, -10.0, 0.2), "a loss within the acceptable loss does not block");
		assertTrue(refusal(10, 0.0, -10.0, 0.05).contains("needs 10 more tasks"));
		assertTrue(RouterGate.refusal(Map.of("stopped_by_cap", true, "stop_reason", "cap"), 0).contains("stopped early"));
		assertTrue(RouterGate.refusal(Map.of("complete", false, "n", 20), 0).contains("incomplete"));
		assertTrue(RouterGate.refusal(Map.of("complete", true, "n", 20), 0).contains("no complete comparison"));
	}

	@Test
	void appliesOnceWithTheLiveGuardrailsAndTheEvidenceInTheNote() throws Exception {
		store("eval-00001", "u1", 24, -0.03, -50.0);
		assertEquals(true, apply("eval-00001").get("applied"));

		assertEquals(1, saves.size());
		Map<String, Object> archived = EvaluateRouterCandidateReactor.load(dir.resolve("router-promotions"), "eval-00001");
		assertEquals("applied", archived.get("status"));
		assertEquals("v2", archived.get("applied_version"));
		assertEquals("v1", archived.get("live_version"));
		JsonObject applied = JsonParser.parseString(saves.get(0)[0]).getAsJsonObject();
		assertEquals("v2", applied.get("version").getAsString());
		assertEquals(0.05, applied.getAsJsonObject("improvement").get("max_quality_loss").getAsDouble());
		assertTrue(saves.get(0)[1].contains("eval-00001") && saves.get(0)[1].contains("24 tasks"));
		assertTrue(reason("eval-00001").contains("already applied"));
		assertEquals(1, saves.size());
	}

	@Test
	void refusesOverBudgetThenPersistsOnePromotionAndRestoresThePriorConfig() throws Exception {
		String original = live.readConfigJson();
		commits(dir, "original", original);
		Path config = dir.resolve("assets/router.json");
		ModelRouterEngine diskRouter = new ModelRouterEngine() {
			@Override
			public File resolveConfigFile() {
				return config.toFile();
			}
		};
		diskRouter.setEngineId("live");
		diskRouter.setSmssProp(new Properties());
		updater.close();
		updater = mockStatic(UpdateModelRouterConfigReactor.class, invocation -> switch (invocation.getMethod().getName()) {
			case "editableRouter" -> diskRouter;
			case "versionFolder" -> dir.toString();
			default -> invocation.callRealMethod();
		});

		String rejected = "eval-expensive";
		store(rejected, "u1", 24, 0.0, -50.0);
		Path evaluations = EvaluateRouterCandidateReactor.evaluationDir("cand");
		Map<String, Object> receipt = EvaluateRouterCandidateReactor.load(evaluations, rejected);
		receipt.put("complete", false);
		receipt.put("stopped_by_cap", true);
		receipt.put("stop_reason", "cap");
		EvaluateRouterCandidateReactor.save(evaluations, rejected, receipt);
		String accepted = "eval-accepted";
		store(accepted, "u1", 24, 0.0, -50.0);
		Insight optimizer = mock(Insight.class);
		ApplyRouterCandidateReactor apply = with(new ApplyRouterCandidateReactor(), optimizer);
		try (var audit = mockStatic(UserAuditTrailUtils.class); var cluster = mockStatic(ClusterUtil.class)) {
			when(optimizer.getRoomId()).thenReturn((String) receipt.get("attempt_id"));
			assertTrue(apply.apply(login, "live", diskRouter, "cand", candidate, rejected).get("reason").toString().contains("cap"));
			assertEquals(original, Files.readString(config));
			String acceptedRoom = (String) EvaluateRouterCandidateReactor.load(evaluations, accepted).get("attempt_id");
			when(optimizer.getRoomId()).thenReturn(acceptedRoom);
			Map<String, Object> applied = apply.apply(login, "live", diskRouter, "cand", candidate, accepted);
			assertEquals(true, applied.get("applied"), applied.toString());
			String promoted = Files.readString(config);
			assertEquals(false, apply.apply(login, "live", diskRouter, "cand", candidate, accepted).get("applied"));
			assertEquals(promoted, Files.readString(config));
			assertEquals(List.of("v2", "v1"), RouterVersions.history(dir.toString(), "assets/router.json").stream()
					.map(RouterVersions.Version::version).toList());

			with(new RollbackRouterConfigReactor(), mock(Insight.class), "engine", "live", "version", "v1").execute();
			JsonObject expected = JsonParser.parseString(original).getAsJsonObject();
			expected.getAsJsonObject("improvement").addProperty("paused", true);
			assertEquals(expected, JsonParser.parseString(Files.readString(config)));
		}
	}

	@Test
	void aCandidateIncompatibleWithLearningDoesNotClaimTheReceipt() throws Exception {
		candidateJson = config("v2", null).replace("\"subtask\"", "\"keyword\"");
		when(candidate.readConfigJson()).thenReturn(candidateJson);
		String id = "eval-incompatible";
		store(id, "u1", 24, 0.0, -50.0);
		assertTrue(reason(id).contains("requires mode 'subtask'"));
		assertNull(EvaluateRouterCandidateReactor.load(ApplyRouterCandidateReactor.promotionDir("live"), id));
		assertTrue(saves.isEmpty());
	}

	@Test
	void aFailedConfigSaveLeavesAClaimWithoutReportingAnAppliedVersion() throws Exception {
		store("eval-00006", "u1", 24, 0.0, -50.0);
		updater.when(() -> UpdateModelRouterConfigReactor.save(any(), any(), any(), any(), any()))
				.thenThrow(new IllegalStateException("simulated config save failure"));

		assertThrows(IllegalStateException.class, () -> apply("eval-00006"));
		Map<String, Object> archived = EvaluateRouterCandidateReactor.load(dir.resolve("router-promotions"), "eval-00006");
		assertEquals("claimed", archived.get("status"));
		assertEquals("v2", archived.get("proposed_version"));
		assertTrue(!archived.containsKey("applied_version"));
		assertTrue(reason("eval-00006").contains("already applied or claimed"));
	}

	@Test
	void qualityImprovementAtFlatCostDoesNotNeedToFundAWatch() throws Exception {
		store("eval-00004", "u1", 24, 0.1, 0.0);
		assertEquals(true, apply("eval-00004").get("applied"));
	}

	@Test
	void otherwisePassingTrainingReceiptCannotPromote() throws Exception {
		store("eval-00005", "u1", 24, 0.0, -50.0);
		Path evaluations = EvaluateRouterCandidateReactor.evaluationDir("cand");
		Map<String, Object> receipt = EvaluateRouterCandidateReactor.load(evaluations, "eval-00005");
		receipt.put("split", "train");
		EvaluateRouterCandidateReactor.save(evaluations, "eval-00005", receipt);
		assertTrue(reason("eval-00005").contains("held-out"));
		assertTrue(saves.isEmpty());
	}

	@Test
	void otherwisePassingReceiptFromThePreviousScoringPolicyCannotPromote() throws Exception {
		String id = "eval-legacy";
		store(id, "u1", 24, 0.0, -50.0);
		Path evaluations = EvaluateRouterCandidateReactor.evaluationDir("cand");
		Map<String, Object> receipt = EvaluateRouterCandidateReactor.load(evaluations, id);
		receipt.remove("scoring");
		EvaluateRouterCandidateReactor.save(evaluations, id, receipt);
		assertTrue(reason(id).contains("older scoring policy"));
		assertTrue(saves.isEmpty());
	}

	@Test
	void refusalsChangeNothing() throws Exception {
		store("eval-00002", "u2", 24, 0.0, -50.0);
		assertTrue(reason("eval-00002").contains("not made by this user"));
		assertTrue(reason("eval-99999").contains("unknown"));
		assertThrows(IllegalArgumentException.class, () -> EvaluateRouterCandidateReactor.load(dir, "../../x"));

		store("eval-00003", "u1", 24, -0.5, -50.0);
		assertTrue(reason("eval-00003").contains("50 points lower"));
		// the candidate was edited after it was evaluated
		when(candidate.readConfigJson()).thenReturn(candidateJson.replace("v2", "v3"));
		assertTrue(reason("eval-00003").contains("changed after"));
		// the live router moved to another version since the comparison
		when(candidate.readConfigJson()).thenReturn(candidateJson);
		when(live.readConfigJson()).thenReturn(config("v1b", LIVE_IMPROVEMENT));
		assertTrue(reason("eval-00003").contains("another live router or version"));
		// learning off or paused
		when(live.readConfigJson()).thenReturn(config("v1", "\"improvement\":{\"enabled\":true,\"paused\":true}"));
		assertTrue(reason("eval-00003").contains("off or paused"));
		assertTrue(saves.isEmpty());
		assertTrue(EvaluateRouterCandidateReactor.load(EvaluateRouterCandidateReactor.evaluationDir("cand"), "eval-00003") != null);
	}
}
