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
package prerna.engine.impl.model.batch;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.BatchListResponse;
import prerna.engine.impl.model.responses.BatchResultItem;
import prerna.engine.impl.model.responses.BatchResultsResponse;
import prerna.engine.impl.model.responses.BatchStatusResponse;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.util.Utility;

/**
 * Thin, stateless orchestration shared by the batch Pixel reactors and the
 * compatible REST endpoints. Resolves the engine, enforces the security boundary
 * ({@code userCanViewEngine}, same as a regular ask), and builds/parses the
 * composite REST id ({@code <engineId>.<providerBatchId>}) so stock OpenAI/
 * Anthropic SDK clients can route later GETs without any SEMOSS-side state.
 */
public class ModelBatchManager {

	public static final char COMPOSITE_DELIM = '.';
	public static final String BATCH_SUBMIT_METHOD = "batch_submit";
	private static final Logger classLogger = LogManager.getLogger(ModelBatchManager.class);

	private ModelBatchManager() {
	}

	/**
	 * Resolve a model engine by id, enforcing access and batch support.
	 */
	public static IModelEngine resolveEngine(User user, String engineId) {
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("A model engine id is required");
		}
		if (user != null && !SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		IModelEngine engine = Utility.getModel(engineId);
		if (engine == null) {
			throw new IllegalArgumentException("Could not resolve model engine " + engineId);
		}
		if (!engine.supportsBatch()) {
			throw new UnsupportedOperationException("Batch model calls are not supported for model " + engineId);
		}
		return engine;
	}

	public static BatchSubmissionResponse submit(User user, String engineId, List<Map<String, Object>> requests,
			Map<String, Object> parameters) {
		return resolveEngine(user, engineId).submitBatch(requests, parameters);
	}

	public static BatchStatusResponse status(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		return resolveEngine(user, engineId).getBatchStatus(providerBatchId, parameters);
	}

	public static BatchResultsResponse results(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		return resolveEngine(user, engineId).getBatchResults(providerBatchId, parameters);
	}

	public static BatchListResponse list(User user, String engineId, Map<String, Object> parameters) {
		return resolveEngine(user, engineId).listBatches(parameters);
	}

	public static BatchStatusResponse cancel(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		return resolveEngine(user, engineId).cancelBatch(providerBatchId, parameters);
	}

	/**
	 * Write one RESPONSE row per completed batch item. TRANSACTION_ID is
	 * "batchId.customId" so each row pairs with the INPUT row written at
	 * submit time (same TRANSACTION_ID). Token counts go on the RESPONSE row.
	 * No-ops when inference logging is disabled or the results list is empty.
	 */
	public static void recordBatchResultsUsage(User user, IModelEngine engine, String providerBatchId,
			BatchResultsResponse results, String insightId, String projectId, String contextProjectId,
			String sessionId) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return;
		}
		if (results == null || results.getItems() == null || results.getItems().isEmpty()) {
			return;
		}
		String roomId = "mb_" + providerBatchId;
		ZonedDateTime now = ZonedDateTime.now();
		AccessToken token = user.getPrimaryLoginToken();
		String userId = token.getId();
		String userName = token.getName() != null ? token.getName() : token.getUsername();
		if (userName == null) {
			userName = token.getEmail();
		}
		String userEmail = token.getEmail();
		String engineId = engine.getEngineId();
		for (BatchResultItem item : results.getItems()) {
			final String txnId = providerBatchId + "." + item.getCustomId();
			final String capturedResponse = item.getFirstTextContent();
			final Integer capturedInput = item.getInputTokens();
			final Integer capturedOutput = item.getOutputTokens();
			final String capturedUserId = userId;
			final String capturedUserName = userName;
			final String capturedUserEmail = userEmail;
			Thread t = new Thread(() -> {
				try {
					// RESPONSE row carries assistant-side counts; MESSAGE_TOKENS = output
					// tokens so the usage analytics input/response split stays correct.
					ModelInferenceLogsUtils.doRecordMessage(
							GUID.v7().toUUID().toString(), txnId, "RESPONSE",
							capturedResponse, "batch",
							capturedOutput, null, capturedOutput, null, null, null,
							null, now,
							engineId, insightId, sessionId, roomId,
							capturedUserId, capturedUserName, capturedUserEmail);
					// back-fill the submit-time INPUT row with prompt-side tokens
					ModelInferenceLogsUtils.updateBatchInputTokens(txnId, capturedInput);
				} catch (Exception e) {
					// best-effort; non-fatal
				}
			});
			t.setDaemon(true);
			t.start();
		}
	}

	/**
	 * Write a single MESSAGE row (method="batch_submit") so that
	 * assertUserOwnsBatch can later verify the requesting user submitted this batch.
	 * No-op when inference logging is disabled.
	 */
	/**
	 * Also writes one INPUT row per request so the TRANSACTION_ID ("batchId.customId")
	 * is available for the RESPONSE row written at results time to pair with, and for
	 * updateBatchInputTokens to back-fill input-side token counts. When a request has
	 * no "command" text (e.g. provider-native request bodies from SubmitModelBatch or
	 * REST), the row is still written with a null prompt so the linkage still works.
	 */
	public static void recordBatchSubmission(User user, IModelEngine engine, String providerBatchId,
			List<Map<String, Object>> requests, String insightId, String sessionId) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return;
		}
		Thread t = new Thread(() -> {
			try {
				AccessToken token = user.getPrimaryLoginToken();
				String userId = token.getId();
				String userName = token.getName() != null ? token.getName() : token.getUsername();
				if (userName == null) {
					userName = token.getEmail();
				}
				String userEmail = token.getEmail();
				String roomId = "mb_" + providerBatchId;
				ZonedDateTime now = ZonedDateTime.now();
				// ownership row
				ModelInferenceLogsUtils.doRecordMessage(
						GUID.v7().toUUID().toString(), providerBatchId, "INPUT",
						String.valueOf(requests.size()),
						BATCH_SUBMIT_METHOD,
						null, null, null, null, null, null,
						null, now,
						engine.getEngineId(), insightId, sessionId, roomId,
						userId, userName, userEmail);
				// one INPUT row per request; pairs with RESPONSE row at results time
				for (Map<String, Object> req : requests) {
					Object customId = req.get("custom_id");
					Object command = req.get("command");
					String txnId = providerBatchId + "." + (customId != null ? customId.toString() : "");
					String commandText = command != null ? command.toString() : null;
					ModelInferenceLogsUtils.doRecordMessage(
							GUID.v7().toUUID().toString(), txnId, "INPUT",
							commandText, "batch",
							null, null, null, null, null, null,
							null, now,
							engine.getEngineId(), insightId, sessionId, roomId,
							userId, userName, userEmail);
				}
			} catch (Exception e) {
				classLogger.warn("Failed to record batch submission for batch '{}'", providerBatchId, e);
			}
		});
		t.setDaemon(true);
		t.start();
	}

	/**
	 * Look up the stored input prompts for a batch. Returns customId -> command map.
	 * Returns empty map if logging is disabled or no submission row exists.
	 */
	public static Map<String, String> getBatchInputs(User user, String providerBatchId) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return Collections.emptyMap();
		}
		String userId = user.getPrimaryLoginToken().getId();
		return ModelInferenceLogsUtils.getBatchInputs(userId, providerBatchId);
	}

	/**
	 * Verify that the requesting user submitted this batch by checking MESSAGE.
	 * Throws if the batch was not found for this user. No-op if inference logging is disabled.
	 */
	public static void assertUserOwnsBatch(User user, String providerBatchId) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return;
		}
		String userId = user.getPrimaryLoginToken().getId();
		if (!ModelInferenceLogsUtils.userOwnsBatch(userId, providerBatchId)) {
			throw new IllegalArgumentException(
					"Batch " + providerBatchId + " does not exist or user does not have access");
		}
	}

	/**
	 * List this user's batch submissions for a given engine, most recent first.
	 * Delegates to ModelInferenceLogsUtils (authorized for ModelInferenceLogsDb).
	 * Returns empty list if inference logging is disabled.
	 */
	public static List<Map<String, Object>> listUserBatches(User user, String engineId, int limit) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return new ArrayList<>();
		}
		String userId = user.getPrimaryLoginToken().getId();
		return ModelInferenceLogsUtils.getUserBatches(userId, engineId, limit);
	}

	// ------------------------------------------------------------------
	// Composite id helpers (REST). engineId is a UUID with no '.', so splitting
	// on the first '.' cleanly separates it from the provider batch id.
	// ------------------------------------------------------------------

	public static String encodeCompositeId(String engineId, String providerBatchId) {
		return engineId + COMPOSITE_DELIM + providerBatchId;
	}

	/**
	 * @return [engineId, providerBatchId]
	 */
	public static String[] decodeCompositeId(String compositeId) {
		if (compositeId == null) {
			throw new IllegalArgumentException("Missing batch id");
		}
		int idx = compositeId.indexOf(COMPOSITE_DELIM);
		if (idx <= 0 || idx >= compositeId.length() - 1) {
			throw new IllegalArgumentException("Malformed batch id: " + compositeId);
		}
		return new String[] { compositeId.substring(0, idx), compositeId.substring(idx + 1) };
	}
}
