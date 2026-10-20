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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.BatchListResponse;
import prerna.engine.impl.model.responses.BatchResultItem;
import prerna.engine.impl.model.responses.BatchResultsResponse;
import prerna.engine.impl.model.responses.BatchStatusResponse;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.engine.impl.model.usage.ModelUsageBilledTo;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.engine.impl.model.usage.ModelUsageGuard;
import prerna.engine.impl.model.usage.ModelUsageLimitException;
import prerna.engine.impl.model.usage.ModelUsageReport;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.engine.impl.model.usage.ModelUsageStoreException;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.om.ThreadStore;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * Thin, stateless orchestration shared by the batch Pixel reactors and the
 * compatible REST endpoints. Resolves the engine, enforces the security
 * boundary ({@code userCanViewEngine}, same as a regular ask), and
 * builds/parses the composite REST id ({@code <engineId>.<providerBatchId>}) so
 * stock OpenAI/ Anthropic SDK clients can route later GETs without any
 * SEMOSS-side state.
 *
 * <p>
 * Usage limits: a batch is admitted when it is submitted, with its whole
 * ceiling reserved and each request's output maximum capped so the batch fits
 * in it, and is billed as its results come in. A request with no output maximum
 * of its own or of its model is reserved for, and capped at,
 * {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS}. The reservation is held
 * on the server the batch was submitted from until every result is billed, the
 * batch's results are read once it has ended, or it expires with the provider's
 * batch window. Each result is marked in the shared counter store when it is
 * billed and when it is logged, so neither happens twice, whichever server
 * reads the results and however often.
 * </p>
 */
public class ModelBatchManager {

	private static final Logger classLogger = LogManager.getLogger(ModelBatchManager.class);

	public static final char COMPOSITE_DELIM = '.';
	public static final String BATCH_SUBMIT_METHOD = "batch_submit";

	private static final List<String> MAX_OUTPUT_KEYS = List.of("max_tokens", "max_completion_tokens",
			"max_output_tokens", "max_new_tokens");
	private static final List<String> NATIVE_BODY_KEYS = List.of("body", "params");

	/**
	 * How long a batch result stays marked billed and logged; results can be read
	 * for up to 29 days on Anthropic, and a read after the mark is gone bills again
	 */
	private static final Duration ITEM_MARK_TIME = Duration.ofDays(30);
	private static final String BILLED_MARK = "batch|billed|";
	private static final String RECORDED_MARK = "batch|recorded|";
	/** MESSAGE.TRANSACTION_ID is VARCHAR(50) */
	private static final int TRANSACTION_ID_MAX_LENGTH = 50;
	/** The batch statuses after which no more results come in */
	private static final Set<String> ENDED_STATUSES = Set.of("COMPLETED", "FAILED", "EXPIRED", "CANCELED", "CANCELLED",
			"ENDED");

	/** The batches submitted from this server whose results are not all billed */
	private static final Cache<String, HeldBatch> HELD_BATCHES = Caffeine.newBuilder().maximumSize(10_000)
			.expireAfterWrite(ModelUsageRequest.BATCH_RESERVATION).build();
	/**
	 * What each batch result billed on this server was billed to, by transaction
	 * id, for its log row, which is written right after on the same server
	 */
	private static final Cache<String, ItemBilling> ITEM_BILLINGS = Caffeine.newBuilder().maximumSize(500_000)
			.expireAfterWrite(Duration.ofDays(2)).build();
	/** This server's marks, for when the shared counter store cannot be reached */
	private static final Cache<String, Boolean> LOCAL_MARKS = Caffeine.newBuilder().maximumSize(500_000)
			.expireAfterWrite(ITEM_MARK_TIME).build();

	/**
	 * A batch admitted under usage limits.
	 *
	 * @param ticket   what the batch holds and is billed to
	 * @param requests the requests to submit, each output maximum capped to fit
	 */
	public record BatchUsage(ModelUsageTicket ticket, List<Map<String, Object>> requests) {
	}

	/**
	 * A batch's reservation, with how many of its results this server has billed
	 */
	private record HeldBatch(ModelUsageTicket ticket, int requestCount, AtomicInteger billed) {
	}

	/**
	 * Marks batch results for one read of them: in the shared counter store, or in
	 * this server's memory for the rest of the read once the store cannot be
	 * reached
	 */
	private static final class ItemMarks {
		private boolean shared = true;

		boolean markOnce(String key) {
			if (shared) {
				try {
					return ModelUsageGuard.markOnce(key, ITEM_MARK_TIME);
				} catch (ModelUsageStoreException e) {
					classLogger.warn("Batch results are marked on this server only, since the counters cannot be "
							+ "reached: " + e.getMessage());
					shared = false;
				}
			}
			return LOCAL_MARKS.asMap().putIfAbsent(key, Boolean.TRUE) == null;
		}
	}

	private record ItemBilling(ModelUsageBilledTo billedTo, Long costMicros) {
	}

	/**
	 * One batch result to write as a RESPONSE row: its transaction id, answer,
	 * token counts, and the source and cost it was billed at.
	 */
	private record ResultRow(String txnId, String response, Integer inputTokens, Integer outputTokens,
			ModelUsageBilledTo billedTo, Long costMicros) {
	}

	// VERTEX-only: providerBatchId is "<storageToken>:<folderToken>.<jobIdToken>".
	// MESSAGE.TRANSACTION_ID is VARCHAR(50) -- and per-request rows keep the
	// readable "<providerBatchId>.<customId>" only while it fits (see
	// batchTransactionId), so the id itself leaves real headroom for that
	// suffix, not just fits alone. Each piece is packed accordingly:
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
		// the engine admits the batch under the usage limits of the user on the thread
		User previous = ThreadStore.getUser();
		ThreadStore.setUser(user);
		try {
			return engine.submitBatch(requests, parameters);
		} finally {
			ThreadStore.setUser(previous);
		}
	}

	/**
	 * Admits a batch under the usage limits that apply to the person submitting it.
	 * The batch is reserved for the provider's batch window: the lesser of what is
	 * left and its estimate, the prompts and each request's output maximum. A
	 * request with no output maximum of its own or of its model is reserved for
	 * {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS} and capped at it. When
	 * the output maximums do not fit in what is reserved together, each is capped
	 * to an equal share of what is left after the prompts.
	 *
	 * @param user     the person submitting, or null for SEMOSS itself
	 * @param engine   the model
	 * @param requests the batch's requests
	 * @return the ticket, and the requests to submit
	 * @throws ModelUsageLimitException when a limit refuses the batch, or the batch
	 *                                  does not fit
	 */
	public static BatchUsage admitBatch(User user, IModelEngine engine, List<Map<String, Object>> requests) {
		if (user == null || requests == null || requests.isEmpty()) {
			return new BatchUsage(ModelUsageTicket.none(), requests);
		}
		long engineMaxTokens = engine.getMaxTokens();
		// each request counts at its own output maximum, else the model's, else the
		// default, so the batch's estimate is always bounded
		long requestedOutput = 0;
		for (Map<String, Object> request : requests) {
			requestedOutput += ModelUsageRequest.boundedMaxOutput(maxOutputOf(request, engineMaxTokens));
		}
		// the prompts are estimated only when a token or cost rule applies
		ModelUsageRequest usageRequest = ModelUsageRequest.ask(() -> {
			long estimate = 0;
			for (Map<String, Object> request : requests) {
				estimate += ModelUsageRequest.estimateTokens(request);
			}
			return estimate;
		}, requestedOutput).reservingFor(ModelUsageRequest.BATCH_RESERVATION);
		ModelUsageTicket ticket = ModelUsageRestrictionUtility.admit(user, engine, usageRequest);
		// no token or cost rule applies, so there is nothing to fit the requests in
		if (ticket.getTokensReserved() == null && ticket.getCostReserved() == null) {
			return new BatchUsage(ticket, requests);
		}

		long promptEstimate = usageRequest.getPromptEstimateTokens();
		// the requests fit as asked: only those with no maximum get the default
		// they were reserved for, so none can go past the reservation
		if (fits(ticket, promptEstimate, requestedOutput)) {
			return new BatchUsage(ticket, withDefaultMaxOutput(requests, engineMaxTokens, engine.getModelType()));
		}
		// otherwise every request is capped at an equal share of the output left
		// after the prompts, and a share too small to answer in refuses the batch
		long share = outputShare(ticket, promptEstimate, requests.size());
		if (share < ModelUsageTicket.MIN_OUTPUT_TOKENS) {
			ticket.release();
			throw ticket.doesNotFit(ModelUsageLimitException.doesNotFit("This batch does not fit in what is left"));
		}
		List<Map<String, Object>> capped = new ArrayList<>();
		for (Map<String, Object> request : requests) {
			capped.add(request == null ? null : withMaxOutput(request, share, engine.getModelType()));
		}
		return new BatchUsage(ticket, capped);
	}

	/**
	 * Keeps a submitted batch's reservation until its results are billed, or gives
	 * it back when the batch was not submitted.
	 *
	 * @param engine          the model
	 * @param providerBatchId the provider's batch id, or null when the batch was
	 *                        not submitted
	 * @param usage           the batch's usage
	 */
	public static void holdBatchUsage(IModelEngine engine, String providerBatchId, BatchUsage usage) {
		if (usage == null || usage.ticket() == null) {
			return;
		}
		if (providerBatchId == null || providerBatchId.isEmpty()) {
			usage.ticket().release();
			return;
		}
		// held in this server's memory for the provider's batch window, the same as
		// the reservation's own expiry in the counters
		int count = usage.requests() == null ? 0 : usage.requests().size();
		HELD_BATCHES.put(batchKey(engine.getEngineId(), providerBatchId),
				new HeldBatch(usage.ticket(), count, new AtomicInteger()));
	}

	/**
	 * Bills the results of a batch that have come in, once each across every
	 * server. The batch's reservation is given back once every result is billed, or
	 * once its results are read after it has ended, since no more come in. A batch
	 * submitted from another server is billed to the source its calls would be
	 * billed to now.
	 *
	 * @param user            the person the batch belongs to
	 * @param engine          the model
	 * @param providerBatchId the provider's batch id
	 * @param results         the results
	 */
	public static void billBatchResults(User user, IModelEngine engine, String providerBatchId,
			BatchResultsResponse results) {
		if (results == null) {
			return;
		}
		String key = batchKey(engine.getEngineId(), providerBatchId);
		HeldBatch held = HELD_BATCHES.getIfPresent(key);
		List<BatchResultItem> items = results.getItems() == null ? List.of() : results.getItems();
		if (!items.isEmpty()) {
			// a batch submitted from another server, or held here past its window,
			// has no reservation on this server, so its results are billed without one
			ModelUsageTicket ticket = held != null ? held.ticket() : null;
			if (ticket == null) {
				try {
					ticket = ModelUsageGuard.forBilling(user, engine);
				} catch (RuntimeException e) {
					classLogger.warn("Unable to bill the results of batch '{}' to their usage limits", providerBatchId,
							e);
					return;
				}
			}
			ItemMarks marks = new ItemMarks();
			// a result with its RESPONSE row was billed by an earlier read, even one
			// whose marks a restart lost
			Set<String> recorded = ModelInferenceLogsUtils.getRecordedBatchResults(providerBatchId);
			for (BatchResultItem item : items) {
				String transactionId = batchTransactionId(providerBatchId, item.getCustomId());
				// results are read again and again, on any server: the mark bills each
				// result once
				if (recorded.contains(transactionId) || !marks.markOnce(BILLED_MARK + transactionId)) {
					continue;
				}
				ModelUsageReport report = new ModelUsageReport(tokens(item.getInputTokens()),
						tokens(item.getOutputTokens()), 0, 0, 0, 0);
				// bills the result and gives back as much of the reservation
				Long cost = ticket.billPart(report);
				// a ticket with no prices, such as one for no user, still logs the cost
				if (cost == null && ModelUsageCost.hasKnownPrices(engine.getEngineId())) {
					cost = ModelUsageCost.costMicros(ModelUsageCost.priceEntry(engine.getEngineId()), report);
				}
				ITEM_BILLINGS.put(transactionId, new ItemBilling(ticket.billedTo(), cost));
				if (held != null) {
					held.billed().incrementAndGet();
				}
			}
		}
		// once the batch has ended or every result is billed, what is left of the
		// reservation, such as the output the requests did not use, goes back
		if (held != null && (hasEnded(results.getStatus())
				|| (held.requestCount() > 0 && held.billed().get() >= held.requestCount()))) {
			held.ticket().release();
			HELD_BATCHES.invalidate(key);
		}
	}

	/**
	 * Gives back the reservation of a batch that failed before any of its requests
	 * ran, such as one that failed validation, so it has no results to bill.
	 *
	 * @param engine          the model
	 * @param providerBatchId the provider's batch id
	 * @param status          the batch's status, as the provider reports it
	 */
	public static void releaseFailedBatch(IModelEngine engine, String providerBatchId, String status) {
		if (status == null || !"FAILED".equals(status.trim().toUpperCase(Locale.ROOT))) {
			return;
		}
		String key = batchKey(engine.getEngineId(), providerBatchId);
		HeldBatch held = HELD_BATCHES.getIfPresent(key);
		if (held != null) {
			held.ticket().release();
			HELD_BATCHES.invalidate(key);
		}
	}

	/**
	 * Whether a batch's status is one it ends in, such as COMPLETED, FAILED or
	 * EXPIRED. Reading an ended batch's results gives back what is left of its
	 * reservation.
	 *
	 * @param status a batch's status, as the provider reports it, or null
	 * @return whether the batch has ended, so no more results come in
	 */
	static boolean hasEnded(String status) {
		return status != null && ENDED_STATUSES.contains(status.trim().toUpperCase(Locale.ROOT));
	}

	private static String batchKey(String engineId, String providerBatchId) {
		return engineId + COMPOSITE_DELIM + providerBatchId;
	}

	/**
	 * The TRANSACTION_ID of one batch request's log rows: "batchId.customId", or a
	 * UUID named after it when that is longer than the column holds, so a long batch
	 * or custom id still logs. Submit, billing and results all key a request by it.
	 *
	 * @param providerBatchId the batch id the caller holds
	 * @param customId        the request's custom id
	 * @return the request's transaction id, at most 50 characters
	 */
	public static String batchTransactionId(String providerBatchId, Object customId) {
		String id = providerBatchId + COMPOSITE_DELIM + (customId == null ? "" : customId);
		return id.length() <= TRANSACTION_ID_MAX_LENGTH ? id
				: UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8)).toString();
	}

	private static long tokens(Integer tokens) {
		return tokens == null ? 0 : tokens.longValue();
	}

	/**
	 * A batch is not fitted by the Python client, so it must fit in what is
	 * reserved, not only in what is left.
	 *
	 * @return whether the batch fits in what is reserved with the output maximums
	 *         asked for
	 */
	private static boolean fits(ModelUsageTicket ticket, long promptEstimate, long requestedOutput) {
		Long tokensReserved = ticket.getTokensReserved();
		if (tokensReserved != null && promptEstimate + requestedOutput > tokensReserved) {
			return false;
		}
		Long costReserved = ticket.getCostReserved();
		if (costReserved != null && ticket.getPriceEntry() != null) {
			Long cost = ModelUsageCost.estimateMicros(ticket.getPriceEntry(), promptEstimate, requestedOutput, false);
			return cost == null || cost <= costReserved;
		}
		return true;
	}

	/**
	 * Works out the output each request of a batch is capped at when the batch does
	 * not fit as asked: an equal share of what the token and cost reservations
	 * leave after the prompts.
	 *
	 * @return each request's equal share of the output what is reserved leaves
	 *         after the prompts, with the prompts priced at the higher of the input
	 *         and cache write rates
	 */
	private static long outputShare(ModelUsageTicket ticket, long promptEstimate, int requestCount) {
		Long tokensReserved = ticket.getTokensReserved();
		Long costReserved = ticket.getCostReserved();
		long share = Long.MAX_VALUE;
		if (tokensReserved != null) {
			share = Math.min(share, (tokensReserved - promptEstimate) / requestCount);
		}
		Map<String, Object> priceEntry = ticket.getPriceEntry();
		if (costReserved != null && priceEntry != null) {
			Map<String, Object> rates = ModelUsageCost.pythonRates(priceEntry, promptEstimate / requestCount);
			double input = Math.max(rate(rates.get(ModelUsageCost.INPUT)), rate(rates.get(ModelUsageCost.CACHE_WRITE)));
			double output = Math.max(rate(rates.get(ModelUsageCost.OUTPUT)), rate(rates.get(ModelUsageCost.REASONING)));
			if (output > 0) {
				double outputMicros = costReserved - promptEstimate * input;
				share = Math.min(share, (long) Math.floor(outputMicros / output / requestCount));
			}
		}
		return share;
	}

	/**
	 * Caps each request with no output maximum of its own or of its model at
	 * {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS}, for a batch that fits in
	 * its reservation as asked.
	 *
	 * @return the requests, each with no output maximum of its own or of its model
	 *         capped at the default it was reserved for
	 */
	private static List<Map<String, Object>> withDefaultMaxOutput(List<Map<String, Object>> requests,
			long engineMaxTokens, ModelTypeEnum modelType) {
		List<Map<String, Object>> bounded = new ArrayList<>();
		for (Map<String, Object> request : requests) {
			boolean unbounded = request != null && maxOutputOf(request, engineMaxTokens) == null;
			bounded.add(unbounded ? withMaxOutput(request, ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, modelType)
					: request);
		}
		return bounded;
	}

	private static double rate(Object rate) {
		return rate instanceof Number ? ((Number) rate).doubleValue() : 0;
	}

	/**
	 * Reads the most output a batch request can ask for, which the batch is
	 * reserved for.
	 *
	 * @return a request's output maximum: its own, under any name the clients
	 *         accept and in a provider-native body too, or else the model's; null
	 *         when neither is known
	 */
	private static Long maxOutputOf(Map<String, Object> request, long engineMaxTokens) {
		if (request != null) {
			Long own = ModelUsageRestrictionUtility.requestedMaxTokens(request, 0);
			if (own != null) {
				return own;
			}
			for (String bodyKey : NATIVE_BODY_KEYS) {
				if (request.get(bodyKey) instanceof Map) {
					@SuppressWarnings("unchecked")
					Map<String, Object> body = (Map<String, Object>) request.get(bodyKey);
					own = ModelUsageRestrictionUtility.requestedMaxTokens(body, 0);
					if (own != null) {
						return own;
					}
				}
			}
		}
		return engineMaxTokens > 0 ? engineMaxTokens : null;
	}

	/**
	 * Caps a batch request's output maximum, in its provider-native body when it
	 * has one, and adds a maximum when the request has none.
	 *
	 * @return a copy of the request whose output maximum is at most the cap, set
	 *         under the name the request's shape uses
	 */
	@SuppressWarnings("unchecked")
	static Map<String, Object> withMaxOutput(Map<String, Object> request, long cap, ModelTypeEnum modelType) {
		Map<String, Object> copy = new LinkedHashMap<>(request);
		// both Vertex clients take max_tokens; Claude on Vertex refuses max_completion_tokens
		boolean usesMaxTokens = modelType == ModelTypeEnum.ANTHROPIC || modelType == ModelTypeEnum.VERTEX;
		for (String bodyKey : NATIVE_BODY_KEYS) {
			if (copy.get(bodyKey) instanceof Map) {
				Map<String, Object> body = new LinkedHashMap<>((Map<String, Object>) copy.get(bodyKey));
				String added = usesMaxTokens ? "max_tokens"
						: body.containsKey("input") ? "max_output_tokens" : "max_completion_tokens";
				capMaxOutput(body, cap, added);
				copy.put(bodyKey, body);
				return copy;
			}
		}
		String added = copy.containsKey("message_json") || usesMaxTokens ? "max_tokens" : "max_completion_tokens";
		capMaxOutput(copy, cap, added);
		return copy;
	}

	private static void capMaxOutput(Map<String, Object> request, long cap, String keyWhenMissing) {
		boolean found = false;
		for (String key : MAX_OUTPUT_KEYS) {
			if (request.containsKey(key)) {
				Long value = ValueUtils.toPositiveLongOrNull(request.get(key));
				request.put(key, value == null ? cap : Math.min(value, cap));
				found = true;
			}
		}
		if (!found) {
			request.put(keyWhenMissing, cap);
		}
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
		// the engine bills the results to the usage limits of the user on the thread
		User previous = ThreadStore.getUser();
		ThreadStore.setUser(user);
		try {
			if (engine.getModelType() == ModelTypeEnum.VERTEX) {
				BatchResultsResponse response = fetchVertexBatchResults(user, engine, providerBatchId, parameters);
				response.setProviderBatchId(providerBatchId);
				return response;
			}
			return engine.getBatchResults(providerBatchId, parameters);
		} finally {
			ThreadStore.setUser(previous);
		}
	}

	/**
	 * Resolves and authorizes a GCS storage engine, mirroring StorageReactor's
	 * own STORAGE key handling. Submitting writes the batch's input into the
	 * bucket, so it needs edit access, as PushToStorage does; reading the results
	 * needs view access.
	 */
	public static IStorageEngine resolveStorage(User user, String storageEngineId, boolean edit) {
		String storageId = SecurityQueryUtils.testUserEngineIdForAlias(user, storageEngineId);
		if (edit ? !SecurityEngineUtils.userCanEditEngine(user, storageId)
				: !SecurityEngineUtils.userCanViewEngine(user, storageId)) {
			throw new IllegalArgumentException("Storage " + storageId + " does not exist or user does not have "
					+ (edit ? "edit " : "") + "access to storage");
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
			List<Map<String, Object>> requests, Map<String, Object> parameters) {
		// the resolved id, not an alias the caller passed, encoded before anything is
		// uploaded or a job is created so a failure here leaves no paid job behind
		String storageEngineId = storage.getEngineId();
		String storageToken = encodeStorageToken(storageEngineId);
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
		jobResponse.setProviderBatchId(storageToken + VERTEX_STORAGE_DELIM + folderToken
				+ COMPOSITE_DELIM + encodeJobIdToken(bareJobId));
		return jobResponse;
	}

	/**
	 * providerBatchId here is "<storageEngineId>:<batchUuid>.<vertexJobName>"
	 * (see {@link #submitVertexBatch}). Splits it back apart, re-resolves and
	 * re-authorizes the storage engine, and downloads the output files under
	 * the exact same relative prefix it originally chose at submit time --
	 * recursing one level to find whatever shard files Vertex wrote there --
	 * into a temp folder, deleted after, that the engine reads and normalizes.
	 * The engine is asked under the caller's id, which the results are billed
	 * and logged under, is given the plain job id as jobId and the downloaded
	 * files as outputFiles, and never touches the bucket.
	 */
	private static BatchResultsResponse fetchVertexBatchResults(User user, IModelEngine engine,
			String providerBatchId, Map<String, Object> parameters) {
		VertexBatchId decoded = decodeVertexBatchId(providerBatchId);
		IStorageEngine storage = resolveStorage(user, decoded.storageEngineId, false);
		String outputPrefix = VERTEX_BATCH_ROOT + decoded.folderToken + "/output/";

		Path localFolder = null;
		try {
			// python reads the output from disk, so it is never sent in the call itself
			List<String> outputFiles = new ArrayList<>();
			try {
				List<String> jsonlPaths = new ArrayList<>();
				collectJsonlBlobPaths(storage, outputPrefix, jsonlPaths, 0);
				localFolder = Files.createTempDirectory("semoss-batch-results-");
				for (String path : jsonlPaths) {
					// a folder each, since files in different output folders can share a name
					Path fileFolder = localFolder.resolve(String.valueOf(outputFiles.size()));
					storage.copyToLocal(path, fileFolder.toString());
					outputFiles.add(fileFolder.resolve(path.substring(path.lastIndexOf('/') + 1)).toString());
				}
			} catch (Exception e) {
				throw new IllegalStateException("Failed to read batch output from storage " + decoded.storageEngineId,
						e);
			}

			Map<String, Object> effectiveParams = new HashMap<>();
			if (parameters != null) {
				effectiveParams.putAll(parameters);
			}
			effectiveParams.put("outputFiles", outputFiles);
			effectiveParams.put("jobId", decoded.bareJobId);
			return engine.getBatchResults(providerBatchId, effectiveParams);
		} finally {
			if (localFolder != null) {
				FileUtils.deleteQuietly(localFolder.toFile());
			}
		}
	}

	/**
	 * Vertex writes output under its own generated subfolder(s) inside the
	 * prefix a job was given (e.g. "prediction-<model>-<timestamp>/predictions_
	 * 00001.jsonl"), so a plain single-level listing of the output prefix
	 * itself would only see that subfolder, not the files in it. Recurses to
	 * find the ".jsonl" files, bounded in depth since Vertex only nests one
	 * level today but this shouldn't break if that ever changes.
	 */
	private static void collectJsonlBlobPaths(IStorageEngine storage, String prefix, List<String> out, int depth)
			throws Exception {
		if (depth > 5) {
			return;
		}
		List<Map<String, Object>> details = storage.listDetails(prefix);
		// Vertex also writes the predictions in chunks as the job runs; once the final
		// file is beside them they only repeat it, and reading both lists results twice
		boolean hasFinal = details.stream().anyMatch(detail -> !Boolean.TRUE.equals(detail.get("IsDir"))
				&& String.valueOf(detail.get("Name")).endsWith(".jsonl"));
		for (Map<String, Object> detail : details) {
			Object nameObj = detail.get("Name");
			if (nameObj == null) {
				continue;
			}
			String childPath = prefix.endsWith("/") ? prefix + nameObj : prefix + "/" + nameObj;
			if (Boolean.TRUE.equals(detail.get("IsDir"))) {
				if (hasFinal && "incremental_predictions".equals(nameObj)) {
					continue;
				}
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
	 * Write one RESPONSE row per completed batch item, all on one virtual thread
	 * for the read. TRANSACTION_ID is "batchId.customId" so each row pairs with the
	 * INPUT row written at submit time (same TRANSACTION_ID). Token counts go on
	 * the RESPONSE row. No-ops when inference logging is disabled or the results
	 * list is empty.
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
		String userType = loginType(user);
		String userName = token.getName() != null ? token.getName() : token.getUsername();
		if (userName == null) {
			userName = token.getEmail();
		}
		String userEmail = token.getEmail();
		String engineId = engine.getEngineId();
		ItemMarks marks = new ItemMarks();
		// a result with its RESPONSE row is not written again, even when a restart
		// lost its mark
		Set<String> recorded = ModelInferenceLogsUtils.getRecordedBatchResults(providerBatchId);
		// marked here, before the write, so a read of the same results at the same
		// time skips them
		List<ResultRow> rows = new ArrayList<>();
		for (BatchResultItem item : results.getItems()) {
			String txnId = batchTransactionId(providerBatchId, item.getCustomId());
			// each result is written once, however often and wherever the results are read
			if (recorded.contains(txnId) || !marks.markOnce(RECORDED_MARK + txnId)) {
				continue;
			}
			ItemBilling billing = ITEM_BILLINGS.getIfPresent(txnId);
			rows.add(new ResultRow(txnId, item.getFirstTextContent(), item.getInputTokens(), item.getOutputTokens(),
					billing == null ? null : billing.billedTo(), billing == null ? null : billing.costMicros()));
		}
		if (rows.isEmpty()) {
			return;
		}
		String name = userName;
		// one thread writes every row of the read in order, so a large batch does not
		// start a thread for each result
		Thread.ofVirtual().name("batch-results-log").start(() -> {
			for (ResultRow row : rows) {
				try {
					ModelUsageBilledTo billedTo = row.billedTo();
					// RESPONSE row carries assistant-side counts; MESSAGE_TOKENS = output
					// tokens so the usage analytics input/response split stays correct.
					ModelInferenceLogsUtils.doRecordMessage(GUID.v7().toUUID().toString(), row.txnId(), "RESPONSE",
							row.response(), "batch", row.outputTokens(), null, row.outputTokens(), null, null, null,
							null, now, engineId, insightId, sessionId, roomId, userId, userType, name, userEmail,
							billedTo == null ? null : billedTo.sourceType(),
							billedTo == null ? null : billedTo.subjectId(),
							billedTo == null ? null : billedTo.subjectType(), row.costMicros());
					// back-fill the submit-time INPUT row with prompt-side tokens
					ModelInferenceLogsUtils.updateBatchInputTokens(row.txnId(), row.inputTokens());
				} catch (Exception e) {
					classLogger.warn("Failed to record batch result '{}'", row.txnId(), e);
				}
			}
		});
	}

	/**
	 * Write a single MESSAGE row (method="batch_submit") so that
	 * assertUserOwnsBatch can later verify the requesting user submitted this
	 * batch. No-op when inference logging is disabled.
	 */
	/**
	 * Also writes one INPUT row per request so the TRANSACTION_ID
	 * ("batchId.customId") is available for the RESPONSE row written at results
	 * time to pair with, and for updateBatchInputTokens to back-fill input-side
	 * token counts. When a request has no "command" text (e.g. provider-native
	 * request bodies from SubmitModelBatch or REST), the row is still written with
	 * a null prompt so the linkage still works.
	 */
	public static void recordBatchSubmission(User user, IModelEngine engine, String providerBatchId,
			List<Map<String, Object>> requests, String insightId, String sessionId) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			return;
		}
		HeldBatch held = HELD_BATCHES.getIfPresent(batchKey(engine.getEngineId(), providerBatchId));
		ModelUsageBilledTo billedTo = held == null ? null : held.ticket().billedTo();
		String billedSourceType = billedTo == null ? null : billedTo.sourceType();
		String billedSubjectId = billedTo == null ? null : billedTo.subjectId();
		String billedSubjectType = billedTo == null ? null : billedTo.subjectType();
		Thread.ofVirtual().name("batch-submission-log").start(() -> {
			try {
				AccessToken token = user.getPrimaryLoginToken();
				String userId = token.getId();
				String userType = loginType(user);
				String userName = token.getName() != null ? token.getName() : token.getUsername();
				if (userName == null) {
					userName = token.getEmail();
				}
				String userEmail = token.getEmail();
				String roomId = "mb_" + providerBatchId;
				ZonedDateTime now = ZonedDateTime.now();
				// ownership row
				ModelInferenceLogsUtils.doRecordMessage(GUID.v7().toUUID().toString(), providerBatchId, "INPUT",
						String.valueOf(requests.size()), BATCH_SUBMIT_METHOD, null, null, null, null, null, null, null,
						now, engine.getEngineId(), insightId, sessionId, roomId, userId, userType, userName, userEmail,
						billedSourceType, billedSubjectId, billedSubjectType, null);
				// one INPUT row per request; pairs with RESPONSE row at results time
				for (Map<String, Object> req : requests) {
					Object customId = req.get("custom_id");
					Object command = req.get("command");
					String txnId = batchTransactionId(providerBatchId, customId);
					String commandText = command != null ? command.toString() : null;
					ModelInferenceLogsUtils.doRecordMessage(GUID.v7().toUUID().toString(), txnId, "INPUT", commandText,
							"batch", null, null, null, null, null, null, null, now, engine.getEngineId(), insightId,
							sessionId, roomId, userId, userType, userName, userEmail, billedSourceType, billedSubjectId,
							billedSubjectType, null);
				}
			} catch (Exception e) {
				classLogger.warn("Failed to record batch submission for batch '{}'", providerBatchId, e);
			}
		});
	}

	/**
	 * Reads the type of a user's primary login, for the user type of an inference
	 * log row.
	 *
	 * @param user a signed in user
	 * @return the primary login's type, or null
	 */
	private static String loginType(User user) {
		AuthProvider primaryLogin = user.getPrimaryLogin();
		return primaryLogin == null ? null : primaryLogin.getLabel();
	}

	/**
	 * Look up the stored input prompts for a batch. Returns transactionId -> command
	 * map, keyed by {@link #batchTransactionId}. Returns empty map if logging is
	 * disabled or no submission row exists.
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
	 * Throws if the batch was not found for this user. No-op if inference logging
	 * is disabled.
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
	 * Splits a composite REST batch id at its first {@code .} into the engine id
	 * and the provider's batch id.
	 *
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
