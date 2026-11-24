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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine.ReplaySettings;
import prerna.engine.impl.model.inferencetracking.RouterEvidenceStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.ValueUtils;

/**
 * What the standing learning job calls, from the volume trigger ({@link RouterLearningTrigger}) or its schedule: starts one
 * router-optimizer agent run when learning is active, enough new runs have been recorded since the previous attempt, no attempt is
 * running, and the recent runs produce usable TRAIN and VAL sets ({@link RouterLearningExamples}). The evaluator applies its own per-evaluation
 * cost cap; this trigger does not enforce a total cycle spend limit. An agent run cannot call this reactor.
 */
public class RunRouterLearningIfDueReactor extends AbstractReactor {

	public RunRouterLearningIfDueReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		if (this.insight.getRoomId() != null) {
			throw new SecurityException("An agent run cannot start router learning");
		}
		organizeKeys();
		String engineId = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		IModelRouterEngine model = UpdateModelRouterConfigReactor.editableRouter(this.insight, engineId);
		ModelRouterEngine.Improvement improvement = ModelRouterEngine
				.parseAndValidateConfig(UpdateModelRouterConfigReactor.readConfig(model), "router", engineId).improvement();
		Map<String, Object> result = new LinkedHashMap<>();
		String reason = null;
		RouterLearningExamples.Built sets = null;
		long newTasks = 0;
		if (improvement == null || !improvement.active()) {
			reason = "learning is off or paused";
		} else if ((newTasks = newTasks(engineId)) < Math.max(1, improvement.min_new_tasks)) {
			reason = "new tasks have not reached min_new_tasks";
		} else if (attemptRunning(engineId)) {
			reason = "a learning attempt is still running";
		} else if ((sets = taskSets(engineId, improvement.replaySettings())) == null) {
			reason = "not enough usable recorded tasks yet";
		} else if (!start(engineId, improvement.optimizer_model, sets, improvement.replaySettings())) {
			reason = "a learning attempt is still running";
		}
		result.put("due", reason == null);
		result.put("new_tasks", newTasks);
		if (reason != null) {
			result.put("reason", reason);
		}
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** Runs of the router recorded since the last learning attempt. */
	long newTasks(String engineId) {
		try {
			return GetRouterEvidenceReactor.newTasksSinceLastAttempt(this.insight, engineId);
		} catch (Exception e) {
			throw new IllegalStateException("Could not count the new runs of router " + engineId + ": " + e.getMessage(), e);
		}
	}

	/** Whether an optimizer run of this router is still going, so neither trigger starts a second one. */
	boolean attemptRunning(String engineId) {
		try {
			return RouterEvidenceStore.attemptRunning(attemptRoomIds(engineId));
		} catch (Exception e) {
			throw new IllegalStateException("Could not read the learning room of router " + engineId + ": " + e.getMessage(), e);
		}
	}

	/** The same count over all users' runs, for the volume trigger that has no caller; the owner's job counts the same way. */
	static long pooledNewTasks(String engineId) {
		try {
			return RouterEvidenceStore.runsSinceLastAttempt(null, engineId, attemptRoomIds(engineId), true);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** The sentence naming a fresh TRAIN and VAL set built from the router's recorded runs; null when there are not enough usable tasks yet. */
	RouterLearningExamples.Built taskSets(String engineId, ReplaySettings settings) {
		try {
			RouterLearningExamples.Built built = RouterLearningExamples.build(this.insight, engineId, settings);
			return built.train() == null ? null : built;
		} catch (Exception e) {
			throw new IllegalStateException("Could not prepare the task sets of router " + engineId + ": " + e.getMessage(), e);
		}
	}

	boolean start(String engineId, String optimizerModel, RouterLearningExamples.Built sets, ReplaySettings settings) {
		ReentrantLock lock = RouterVersions.lock(engineId);
		lock.lock();
		try {
			if (attemptRunning(engineId)) {
				return false;
			}
			String roomId = newLearningRoom();
			try {
				Path replay = EvaluateRouterCandidateReactor.evidenceDir(engineId).resolve("replay");
				AccessToken login = GetRouterEvidenceReactor.loginOf(this.insight);
				EvaluateRouterCandidateReactor.save(attemptDir(engineId), roomId, Map.of("train", sets.train(), "val", sets.val(), "judge", optimizerModel,
						"userId", login.getId(), "userType", login.getProvider().getLabel(),
						"replay_settings", new Gson().toJsonTree(settings),
						"train_sha256", RouterLearningExamples.replayFingerprint(RouterLearningExamples.setDir(replay, sets.train())),
						"val_sha256", RouterLearningExamples.replayFingerprint(RouterLearningExamples.setDir(replay, sets.val()))));
			} catch (IOException e) {
				throw new IllegalStateException("Could not pin the learning attempt", e);
			}
			NounMetadata result = this.insight.runPixel("RunAgent(roomId=[\"" + roomId + "\"], command=[\"Run one improvement cycle for model router "
					+ engineId + " following your playbook. Judge model id: " + optimizerModel + ". TRAIN task set id: " + sets.train() + ". VAL task set id: " + sets.val() + "."
					+ " jobCostCapMicros for every EvaluateRouterCandidate call: " + settings.evaluation_budget_micros + ".\"], engine=[\"" + optimizerModel
					+ "\"], workspaceId=[\"" + Constants.AGENT_ROUTER_OPTIMIZER + "\"], wait=[false]);").getResults().get(0);
			if (result.getOpType().contains(PixelOperationType.ERROR)) {
				throw new IllegalStateException("Could not start the optimizer: " + result.getValue());
			}
			return true;
		} finally {
			lock.unlock();
		}
	}

	static Path attemptDir(String engineId) {
		return EvaluateRouterCandidateReactor.evidenceDir(engineId).resolve("router-attempts");
	}

	static Map<String, Object> attempt(String engineId, String roomId) throws IOException {
		return roomId != null
				? EvaluateRouterCandidateReactor.load(attemptDir(engineId), roomId) : null;
	}

	static boolean ownedBy(Map<String, Object> attempt, AccessToken login) {
		return login.getId().equals(attempt.get("userId")) && login.getProvider().getLabel().equals(attempt.get("userType"));
	}

	/** The per-router receipt directory is keyed by the exact optimizer room IDs. */
	static List<String> attemptRoomIds(String engineId) throws IOException {
		try (var receipts = Files.list(attemptDir(engineId))) {
			return receipts.filter(Files::isRegularFile).map(path -> path.getFileName().toString())
					.filter(name -> name.endsWith(".json")).map(name -> name.substring(0, name.length() - ".json".length())).toList();
		} catch (NoSuchFileException e) {
			return List.of();
		}
	}

	static String newLearningRoom() {
		return UUID.randomUUID().toString();
	}
}
