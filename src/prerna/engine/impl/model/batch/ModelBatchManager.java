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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.f4b6a3.uuid.alt.GUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.ModelTypeEnum;
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

	// VERTEX-only: providerBatchId is "<storageToken>:<folderToken>.<jobIdToken>".
	// MESSAGE.TRANSACTION_ID is VARCHAR(50) -- and per-request rows key off
	// "<providerBatchId>.<customId>" (see recordBatchSubmission /
	// recordBatchResultsUsage), so the id itself must leave real headroom for
	// that suffix, not just fit alone. Each piece is packed accordingly:
	// storageToken is the storage engine's UUID base64url-encoded (22 chars vs.
	// 36), folderToken is a short random token identifying this batch's GCS
	// subfolder (5 chars), and jobIdToken is the bare numeric Vertex job id (the
	// part after the last '/' in the job's full resource name) base36-encoded
	// (~13 chars vs. up to 20 decimal digits) -- id total ~42 chars, leaving ~8
	// for ".<customId>". None of these alphabets contain '.' or ':', so a single
	// split on each delimiter is unambiguous.
	private static final char VERTEX_STORAGE_DELIM = ':';
	private static final String VERTEX_BATCH_ROOT = "semoss-batches/";

	/**
	 * Decoded pieces of a VERTEX providerBatchId; see {@link #VERTEX_STORAGE_DELIM}.
	 */
	private static final class VertexBatchId {
		final String storageEngineId;
		final String folderToken;
		final String bareJobId;

		VertexBatchId(String storageEngineId, String folderToken, String bareJobId) {
			this.storageEngineId = storageEngineId;
			this.folderToken = folderToken;
			this.bareJobId = bareJobId;
		}
	}

	private static String encodeStorageToken(String storageEngineId) {
		UUID uuid = UUID.fromString(storageEngineId);
		ByteBuffer bb = ByteBuffer.allocate(16);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array());
	}

	private static String decodeStorageToken(String token) {
		ByteBuffer bb = ByteBuffer.wrap(Base64.getUrlDecoder().decode(token));
		return new UUID(bb.getLong(), bb.getLong()).toString();
	}

	private static String encodeJobIdToken(String bareDecimalJobId) {
		return new BigInteger(bareDecimalJobId, 10).toString(36);
	}

	private static String decodeJobIdToken(String token) {
		return new BigInteger(token, 36).toString(10);
	}

	private static VertexBatchId decodeVertexBatchId(String providerBatchId) {
		int colonIdx = providerBatchId == null ? -1 : providerBatchId.indexOf(VERTEX_STORAGE_DELIM);
		int dotIdx = providerBatchId == null ? -1 : providerBatchId.indexOf(COMPOSITE_DELIM);
		if (colonIdx <= 0 || dotIdx <= colonIdx) {
			throw new IllegalArgumentException("Malformed Vertex batch id: " + providerBatchId);
		}
		String storageEngineId = decodeStorageToken(providerBatchId.substring(0, colonIdx));
		String folderToken = providerBatchId.substring(colonIdx + 1, dotIdx);
		String bareJobId = decodeJobIdToken(providerBatchId.substring(dotIdx + 1));
		return new VertexBatchId(storageEngineId, folderToken, bareJobId);
	}

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
		IModelEngine engine = resolveEngine(user, engineId);
		if (engine.getModelType() == ModelTypeEnum.VERTEX) {
			// Vertex has no hosted batch API -- submitting requires staging the
			// requests through Cloud Storage first (see submitVertexBatch). A bare
			// engine.submitBatch() call here would silently build the input JSONL
			// and stop, never creating a job.
			throw new IllegalArgumentException(
					"Model " + engineId + " requires GCS staging storage to submit a batch; use submitVertexBatch");
		}
		return engine.submitBatch(requests, parameters);
	}

	public static BatchStatusResponse status(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		IModelEngine engine = resolveEngine(user, engineId);
		if (engine.getModelType() == ModelTypeEnum.VERTEX) {
			BatchStatusResponse response = engine.getBatchStatus(decodeVertexBatchId(providerBatchId).bareJobId,
					parameters);
			// restore the caller-facing composite id -- the engine only saw the
			// decoded bare job id and echoes that back, not what the caller passed in
			response.setProviderBatchId(providerBatchId);
			return response;
		}
		return engine.getBatchStatus(providerBatchId, parameters);
	}

	/**
	 * For a VERTEX engine, providerBatchId is "<storageEngineId>:<batchUuid>.
	 * <vertexJobName>" (see {@link #submitVertexBatch}) since results, unlike
	 * status/list/cancel, needs bucket access again. Re-resolves and
	 * re-authorizes that storage engine here (no SEMOSS-side state persisted
	 * for it) and reads the output itself -- Python never touches the bucket.
	 */
	public static BatchResultsResponse results(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		IModelEngine engine = resolveEngine(user, engineId);
		if (engine.getModelType() == ModelTypeEnum.VERTEX) {
			BatchResultsResponse response = fetchVertexBatchResults(user, engine, providerBatchId, parameters);
			response.setProviderBatchId(providerBatchId);
			return response;
		}
		return engine.getBatchResults(providerBatchId, parameters);
	}

	/**
	 * Resolves and authorizes a GCS storage engine, mirroring StorageReactor's
	 * own STORAGE key handling.
	 */
	public static IStorageEngine resolveStorage(User user, String storageEngineId) {
		String storageId = SecurityQueryUtils.testUserEngineIdForAlias(user, storageEngineId);
		if (!SecurityEngineUtils.userCanViewEngine(user, storageId)) {
			throw new IllegalArgumentException(
					"Storage " + storageId + " does not exist or user does not have access to storage");
		}
		return Utility.getStorage(storageId);
	}

	/**
	 * Anthropic-on-Vertex has no hosted batch API; a Vertex AI batch prediction
	 * job is used instead, staged through Cloud Storage. Java drives every bit
	 * of that GCS I/O itself through the given (already resolved+authorized)
	 * storage engine -- its credentials never leave this method, let alone
	 * reach the out-of-process Python runtime. Two calls into the engine:
	 * first to build the input JSONL (no GCS/Vertex touched), then -- once
	 * Java has uploaded it -- to actually create the job with the resulting
	 * gs:// URIs.
	 */
	public static BatchSubmissionResponse submitVertexBatch(IModelEngine engine, IStorageEngine storage,
			String storageEngineId, List<Map<String, Object>> requests, Map<String, Object> parameters) {
		BatchSubmissionResponse built = engine.submitBatch(requests, parameters);
		Object jsonlObj = built.getRaw() == null ? null : built.getRaw().get("jsonl_content");
		if (!(jsonlObj instanceof String) || ((String) jsonlObj).isEmpty()) {
			throw new IllegalStateException("Engine did not return batch input content to upload");
		}

		String folderToken = Utility.getRandomString(4);
		String batchFolder = VERTEX_BATCH_ROOT + folderToken;
		String inputUri;
		String outputUri;
		try {
			Path tempDir = Files.createTempDirectory("semoss-batch-");
			Path tempFile = tempDir.resolve("input.jsonl");
			try {
				Files.write(tempFile, ((String) jsonlObj).getBytes(StandardCharsets.UTF_8));
				storage.copyToStorage(tempFile.toString(), batchFolder, null);
			} finally {
				Files.deleteIfExists(tempFile);
				Files.deleteIfExists(tempDir);
			}
			inputUri = storage.toExternalUri(batchFolder + "/input.jsonl");
			outputUri = storage.toExternalUri(batchFolder + "/output/");
		} catch (Exception e) {
			throw new IllegalStateException("Failed to upload batch input to storage " + storageEngineId, e);
		}

		Map<String, Object> jobParams = new HashMap<>();
		jobParams.put("inputUri", inputUri);
		jobParams.put("outputUriPrefix", outputUri);
		BatchSubmissionResponse jobResponse = engine.submitBatch(Collections.emptyList(), jobParams);
		String fullJobName = jobResponse.getProviderBatchId();
		if (fullJobName == null) {
			throw new IllegalStateException("Engine did not return a batch id after job creation");
		}
		// the engine returns Vertex's full job resource name
		// (projects/.../locations/.../batchPredictionJobs/<id>); only the
		// trailing numeric id is needed to poll/cancel/fetch it later
		String bareJobId = fullJobName.substring(fullJobName.lastIndexOf('/') + 1);
		jobResponse.setProviderBatchId(encodeStorageToken(storageEngineId) + VERTEX_STORAGE_DELIM + folderToken
				+ COMPOSITE_DELIM + encodeJobIdToken(bareJobId));
		return jobResponse;
	}

	/**
	 * providerBatchId here is "<storageEngineId>:<batchUuid>.<vertexJobName>"
	 * (see {@link #submitVertexBatch}). Splits it back apart, re-resolves and
	 * re-authorizes the storage engine, and downloads the output itself under
	 * the exact same relative prefix it originally chose at submit time --
	 * recursing one level to find whatever shard files Vertex wrote there --
	 * before asking the engine to normalize their content. The engine is only
	 * asked for the plain job name and never touches the bucket.
	 */
	private static BatchResultsResponse fetchVertexBatchResults(User user, IModelEngine engine,
			String providerBatchId, Map<String, Object> parameters) {
		VertexBatchId decoded = decodeVertexBatchId(providerBatchId);
		IStorageEngine storage = resolveStorage(user, decoded.storageEngineId);
		String outputPrefix = VERTEX_BATCH_ROOT + decoded.folderToken + "/output/";

		List<String> rawBlobs = new ArrayList<>();
		try {
			List<String> jsonlPaths = new ArrayList<>();
			collectJsonlBlobPaths(storage, outputPrefix, jsonlPaths, 0);
			for (String path : jsonlPaths) {
				byte[] bytes = storage.readBlobToMemory(path);
				rawBlobs.add(new String(bytes, StandardCharsets.UTF_8));
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to read batch output from storage " + decoded.storageEngineId, e);
		}

		Map<String, Object> effectiveParams = new HashMap<>();
		if (parameters != null) {
			effectiveParams.putAll(parameters);
		}
		effectiveParams.put("rawBlobs", rawBlobs);
		return engine.getBatchResults(decoded.bareJobId, effectiveParams);
	}

	/**
	 * Vertex writes output under its own generated subfolder(s) inside the
	 * prefix a job was given (e.g. "prediction-<model>-<timestamp>/predictions_
	 * 00001.jsonl"), so a plain single-level listing of the output prefix
	 * itself would only see that subfolder, not the files in it. Recurses to
	 * find every ".jsonl" file, bounded in depth since Vertex only nests one
	 * level today but this shouldn't break if that ever changes.
	 */
	private static void collectJsonlBlobPaths(IStorageEngine storage, String prefix, List<String> out, int depth)
			throws Exception {
		if (depth > 5) {
			return;
		}
		for (Map<String, Object> detail : storage.listDetails(prefix)) {
			Object nameObj = detail.get("Name");
			if (nameObj == null) {
				continue;
			}
			String childPath = prefix.endsWith("/") ? prefix + nameObj : prefix + "/" + nameObj;
			if (Boolean.TRUE.equals(detail.get("IsDir"))) {
				collectJsonlBlobPaths(storage, childPath + "/", out, depth + 1);
			} else if (childPath.endsWith(".jsonl")) {
				out.add(childPath);
			}
		}
	}

	public static BatchListResponse list(User user, String engineId, Map<String, Object> parameters) {
		return resolveEngine(user, engineId).listBatches(parameters);
	}

	public static BatchStatusResponse cancel(User user, String engineId, String providerBatchId,
			Map<String, Object> parameters) {
		IModelEngine engine = resolveEngine(user, engineId);
		if (engine.getModelType() == ModelTypeEnum.VERTEX) {
			BatchStatusResponse response = engine.cancelBatch(decodeVertexBatchId(providerBatchId).bareJobId,
					parameters);
			response.setProviderBatchId(providerBatchId);
			return response;
		}
		return engine.cancelBatch(providerBatchId, parameters);
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
