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

import static prerna.reactor.model.RouterGate.number;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * The only way an agent run changes a live router that has learning on: applies a candidate router's config to the
 * live router, only when the evaluation stored by {@link EvaluateRouterCandidateReactor} was made by the caller for the
 * candidate's current config and live version and passes {@link RouterGate} for the live router's
 * {@code improvement.max_quality_loss}. The caller passes ids, never numbers, and the live {@code improvement} section is kept. A refusal changes
 * nothing. A durable claim prevents automatic reuse, including after an interrupted apply.
 */
public class ApplyRouterCandidateReactor extends AbstractReactor {

	static Path promotionDir(String liveId) {
		return EvaluateRouterCandidateReactor.evidenceDir(liveId).resolve("router-promotions");
	}

	private static final String LIVE_KEY = "liveEngineId";
	private static final String CANDIDATE_KEY = "candidateEngineId";
	private static final String EVALUATION_KEY = "evaluationId";

	public ApplyRouterCandidateReactor() {
		this.keysToGet = new String[] { LIVE_KEY, CANDIDATE_KEY, EVALUATION_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String liveId = ValueUtils.trimToNull(this.keyValue.get(LIVE_KEY));
		String candidateId = ValueUtils.trimToNull(this.keyValue.get(CANDIDATE_KEY));
		String evaluationId = ValueUtils.trimToNull(this.keyValue.get(EVALUATION_KEY));
		if (liveId == null || candidateId == null || evaluationId == null || liveId.equals(candidateId)) {
			throw new IllegalArgumentException("liveEngineId, a different candidateEngineId and evaluationId are required");
		}
		IModelRouterEngine live = UpdateModelRouterConfigReactor.editableRouter(this.insight, liveId);
		IModelRouterEngine candidate = UpdateModelRouterConfigReactor.editableRouter(this.insight, candidateId);
		return new NounMetadata(apply(GetRouterEvidenceReactor.loginOf(this.insight), liveId, live, candidateId, candidate, evaluationId),
				PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	Map<String, Object> apply(AccessToken login, String liveId, IModelRouterEngine live, String candidateId, IModelRouterEngine candidate,
			String evaluationId) {
		ReentrantLock lock = RouterVersions.lock(liveId);
		lock.lock();
		try {
			return applyLocked(login, liveId, live, candidateId, candidate, evaluationId);
		} finally {
			lock.unlock();
		}
	}

	private Map<String, Object> applyLocked(AccessToken login, String liveId, IModelRouterEngine live, String candidateId,
			IModelRouterEngine candidate, String evaluationId) {
		Path dir = EvaluateRouterCandidateReactor.evaluationDir(candidateId);
		try {
			Path archive = promotionDir(liveId);
			Path receipt = EvaluateRouterCandidateReactor.file(archive, evaluationId);
			if (Files.exists(receipt)) {
				return Map.of("applied", false, "reason", "evaluation already applied or claimed");
			}
			String liveJson = UpdateModelRouterConfigReactor.readConfig(live);
			String candidateJson = UpdateModelRouterConfigReactor.readConfig(candidate);
			ModelRouterEngine.Improvement improvement = ModelRouterEngine.parseAndValidateConfig(liveJson, "live router", liveId).improvement();
			JsonObject liveConfig = JsonParser.parseString(liveJson).getAsJsonObject();
			JsonObject config = JsonParser.parseString(candidateJson).getAsJsonObject();
			Map<String, Object> evaluation = EvaluateRouterCandidateReactor.load(dir, evaluationId);
			if (improvement == null || !improvement.active()) {
				return Map.of("applied", false, "reason", "learning is off or paused on the live router");
			}
			if (evaluation == null) {
				return Map.of("applied", false, "reason", "unknown or already applied evaluation");
			}
			String refusal = mismatch(evaluation, login, liveId, liveConfig, candidateId, candidateJson, config);
			if (refusal == null) {
				refusal = heldOutRefusal(evaluation, login, liveId, liveJson, candidateJson);
			}
			if (refusal == null) {
				refusal = RouterGate.refusal(evaluation, improvement.max_quality_loss, improvement.replaySettings().validation_tasks);
			}
			if (refusal != null) {
				return Map.of("applied", false, "reason", refusal);
			}

			String appliedJson;
			try {
				appliedJson = validatedPromotionConfig(config, liveConfig, liveId);
			} catch (IllegalArgumentException e) {
				return Map.of("applied", false, "reason", e.getMessage());
			}
			String appliedVersion = config.get("version").getAsString();
			Files.createDirectories(archive);
			Map<String, Object> decision = new LinkedHashMap<>(evaluation);
			decision.remove("failure_digest");
			decision.put("evaluationId", evaluationId);
			decision.put("status", "claimed");
			decision.put("proposed_version", appliedVersion);
			decision.put("previous_config_sha256", ModelRouterEngine.sha256(liveJson));
			decision.put("proposed_config_sha256", ModelRouterEngine.sha256(appliedJson));
			decision.put("claimed_at", Instant.now().toString());
			try {
				Files.writeString(receipt, new Gson().toJson(decision), StandardOpenOption.CREATE_NEW);
			} catch (FileAlreadyExistsException e) {
				return Map.of("applied", false, "reason", "evaluation already applied or claimed");
			}
			UpdateModelRouterConfigReactor.save(this.insight, liveId, live, appliedJson,
					String.format("ApplyRouterCandidate: candidate %s, evaluation %s: %d tasks, quality %+.3f, cost %+.0f micros per task",
							candidateId, evaluationId, (int) number(evaluation.get("n")), number(evaluation.get("quality_delta_vs_live")),
							number(evaluation.get("cost_delta_vs_live"))));
			markApplied(receipt, decision, appliedVersion);
			return Map.of("applied", true, "version", appliedVersion, "evaluationId", evaluationId);
		} catch (IOException e) {
			throw new IllegalStateException("Could not apply the candidate: " + e.getMessage(), e);
		}
	}

	static String validatedPromotionConfig(JsonObject candidate, JsonObject live, String liveId) {
		JsonObject promoted = candidate.deepCopy();
		promoted.add("improvement", live.get("improvement"));
		String json = promoted.toString();
		ModelRouterEngine.parseAndValidateConfig(json, "promoted router", liveId);
		return json;
	}

	private static void markApplied(Path receipt, Map<String, Object> decision, String version) {
		decision.put("status", "applied");
		decision.put("applied_version", version);
		decision.put("applied_config_sha256", decision.remove("proposed_config_sha256"));
		decision.remove("proposed_version");
		try {
			Path pending = Files.createTempFile(receipt.getParent(), "promotion-", ".tmp");
			try {
				Files.writeString(pending, new Gson().toJson(decision));
				Files.move(pending, receipt, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} finally {
				Files.deleteIfExists(pending);
			}
		} catch (IOException e) {
			throw new IllegalStateException("Live version " + version + " was saved, but its promotion receipt could not be finalized", e);
		}
	}

	private String heldOutRefusal(Map<String, Object> evaluation, AccessToken login, String liveId, String liveJson,
			String candidateJson) throws IOException {
		String attemptId = (String) evaluation.get("attempt_id");
		Map<String, Object> attempt = RunRouterLearningIfDueReactor.attempt(liveId, attemptId);
		if (!"val".equals(evaluation.get("split")) || attempt == null || !RunRouterLearningIfDueReactor.ownedBy(attempt, login)
				|| !attempt.get("val").equals(evaluation.get("task_set"))
				|| (this.insight != null && !Objects.equals(attemptId, this.insight.getRoomId()))) {
			return "promotion requires the held-out evaluation from this learning attempt";
		}
		if (!RouterEvaluationEvidence.SCORING.equals(evaluation.get("scoring"))) {
			return "evaluation used an older scoring policy; run a new learning attempt";
		}
		if (!ModelRouterEngine.sha256(liveJson).equals(evaluation.get("live_config_sha256"))) {
			return "the live config changed after evaluation";
		}
		if (evaluation.get("manifest_sha256") == null || !evaluation.get("manifest_sha256").equals(attempt.get("val_sha256"))) {
			return "evaluation does not match the frozen held-out inputs";
		}
		if (!RouterEvaluationEvidence.skillFingerprints(candidateJson).equals(evaluation.get("candidate_skill_sha256"))
				|| !RouterEvaluationEvidence.skillFingerprints(liveJson).equals(evaluation.get("live_skill_sha256"))) {
			return "skill contents changed after evaluation";
		}
		return null;
	}

	/** Why the evaluation is not for this user, candidate (as it is now) and live version, or null. */
	private static String mismatch(Map<String, Object> evaluation, AccessToken login, String liveId, JsonObject liveConfig, String candidateId,
			String candidateJson, JsonObject config) {
		if (!candidateId.equals(evaluation.get("candidateEngineId")) || !login.getId().equals(evaluation.get("userId"))
				|| !login.getProvider().getLabel().equals(evaluation.get("userType"))) {
			return "the evaluation was not made by this user for this candidate";
		}
		if (!ModelRouterEngine.sha256(candidateJson).equals(evaluation.get("candidateSha256"))) {
			return "the candidate's config changed after it was evaluated";
		}
		if (!liveId.equals(evaluation.get("live_engine_id")) || !liveConfig.get("version").getAsString().equals(evaluation.get("live_version"))) {
			return "the evaluation compared another live router or version";
		}
		return !config.has("version") || config.get("version").equals(liveConfig.get("version")) ? "the candidate must carry a new version" : null;
	}
}
