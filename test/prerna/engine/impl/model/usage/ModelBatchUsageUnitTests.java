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
package prerna.engine.impl.model.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Layer;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.engine.impl.model.batch.ModelBatchManager;
import prerna.engine.impl.model.batch.ModelBatchManager.BatchUsage;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.BatchResultsResponse;

/**
 * Batch usage in {@link ModelBatchManager}, with counters in memory. It sits in
 * this package to admit batches on hand-built paths and to choose the counters.
 */
public class ModelBatchUsageUnitTests {

	private static final String MODEL = "model-m";
	private static final String PERSON = "eli";
	private static final String LOGIN = "NATIVE";
	private static final Instant NOW = Instant.parse("2026-10-12T14:30:15Z");

	private MutableTestClock clock;
	private LocalModelUsageCounterStore store;
	private IModelEngine engine;

	@BeforeEach
	void setUp() {
		clock = new MutableTestClock(NOW);
		store = new LocalModelUsageCounterStore(clock);
		ModelUsageGuard.setCounterStore(store);
		ModelUsageGuard.setClock(clock);
		ModelUsageGuard.setSeeder(null);
		ModelUsageGuard.setPrices(engineId -> null);
		engine = mock(IModelEngine.class);
		// no engine id, so no prices are read
		when(engine.getEngineId()).thenReturn(null);
		when(engine.getMaxTokens()).thenReturn(0L);
		when(engine.getModelType()).thenReturn(ModelTypeEnum.OPEN_AI);
	}

	@AfterEach
	void tearDown() {
		ModelUsageGuard.setCounterStore(null);
		ModelUsageGuard.setClock(null);
		ModelUsageGuard.resetSeeder();
		ModelUsageGuard.setPrices(null);
	}

	@Test
	void aRequestWithNoOutputMaximumIsReservedForAndCappedAtTheDefault() {
		List<Map<String, Object>> requests = new ArrayList<>();
		requests.add(new LinkedHashMap<>(Map.of("custom_id", "r1", "message_json", "[]")));
		requests.add(new LinkedHashMap<>(Map.of("custom_id", "r2", "message_json", "[]", "max_tokens", 500)));
		BatchUsage usage = admitBatch(requests);

		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, usage.requests().get(0).get("max_tokens"));
		assertEquals(500, usage.requests().get(1).get("max_tokens"));
		long prompts = ModelUsageRequest.estimateTokens(requests.get(0))
				+ ModelUsageRequest.estimateTokens(requests.get(1));
		assertEquals(prompts + ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS + 500, usage.ticket().getTokensReserved());
		usage.ticket().release();
	}

	@Test
	void aVertexRequestIsCappedUnderMaxTokens() {
		// Claude on Vertex refuses max_completion_tokens, and Gemini on Vertex takes max_tokens too
		when(engine.getModelType()).thenReturn(ModelTypeEnum.VERTEX);
		BatchUsage usage = admitBatch(List.of(Map.of("custom_id", "r1", "command", "hi")));

		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, usage.requests().get(0).get("max_tokens"));
		assertFalse(usage.requests().get(0).containsKey("max_completion_tokens"));
		usage.ticket().release();
	}

	@Test
	void eachResultIsBilledOnceHoweverOftenItIsRead() {
		BatchUsage usage = admitBatch(
				List.of(Map.of("custom_id", "r1", "max_tokens", 100), Map.of("custom_id", "r2", "max_tokens", 100)));
		ModelBatchManager.holdBatchUsage(engine, "batch-once", usage);
		BatchResultsResponse results = results(null, item("r1", 40, 60));
		ModelBatchManager.billBatchResults(null, engine, "batch-once", results);
		ModelBatchManager.billBatchResults(null, engine, "batch-once", results);

		assertEquals(100, used());
		// the mark is shared, so another server reading the results does not bill them
		assertFalse(store.markOnce("batch|billed|batch-once.r1", Duration.ofDays(1)));
		assertTrue(store.markOnce("batch|billed|batch-once.r2", Duration.ofDays(1)));
	}

	@Test
	void aResultInTheLogsIsNotBilledAgainWhenItsMarkIsGone() {
		BatchUsage usage = admitBatch(
				List.of(Map.of("custom_id", "r1", "max_tokens", 100), Map.of("custom_id", "r2", "max_tokens", 100)));
		ModelBatchManager.holdBatchUsage(engine, "batch-restart", usage);
		// the store has no mark for r1, as after a restart, but its RESPONSE row is in
		// the logs
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getRecordedBatchResults("batch-restart"))
					.thenReturn(Set.of("batch-restart.r1"));
			ModelBatchManager.billBatchResults(null, engine, "batch-restart",
					results("IN_PROGRESS", item("r1", 40, 60), item("r2", 10, 20)));
		}

		assertEquals(30, used());
		// r1 is skipped before its mark is made, and r2 is marked billed
		assertTrue(store.markOnce("batch|billed|batch-restart.r1", Duration.ofDays(1)));
		assertFalse(store.markOnce("batch|billed|batch-restart.r2", Duration.ofDays(1)));
		usage.ticket().release();
	}

	@Test
	void aRequestIdTooLongForTheLogsStillBillsOnce() {
		// a Vertex batch id is 42 characters, so "<batchId>.<customId>" passes the column's 50 from req-1000
		String vertexBatchId = "bMRys8aHSpq5nnnv1XN8WA:aXEKW.1kyedurkn96gw";
		assertEquals(vertexBatchId + ".req-999", ModelBatchManager.batchTransactionId(vertexBatchId, "req-999"));
		String longId = ModelBatchManager.batchTransactionId(vertexBatchId, "req-1000");
		assertEquals(36, longId.length());
		assertEquals(longId, ModelBatchManager.batchTransactionId(vertexBatchId, "req-1000"));
		assertNotEquals(longId, ModelBatchManager.batchTransactionId(vertexBatchId, "req-1001"));

		BatchUsage usage = admitBatch(List.of(Map.of("custom_id", "req-1000", "max_tokens", 100)));
		ModelBatchManager.holdBatchUsage(engine, vertexBatchId, usage);
		ModelBatchManager.billBatchResults(null, engine, vertexBatchId, results("COMPLETED", item("req-1000", 40, 60)));
		assertEquals(100, used());
		assertFalse(store.markOnce("batch|billed|" + longId, Duration.ofDays(1)));
	}

	@Test
	void anEndedBatchGivesBackWhatItStillHolds() {
		BatchUsage usage = admitBatch(
				List.of(Map.of("custom_id", "r1", "max_tokens", 100), Map.of("custom_id", "r2", "max_tokens", 100)));
		ModelBatchManager.holdBatchUsage(engine, "batch-ended", usage);
		assertTrue(reserved() > 0);

		// one of two results in, and the batch still running: the rest stays held
		ModelBatchManager.billBatchResults(null, engine, "batch-ended", results("IN_PROGRESS", item("r1", 40, 60)));
		assertTrue(reserved() > 0);
		// the batch ended with no result for the second request
		ModelBatchManager.billBatchResults(null, engine, "batch-ended", results("COMPLETED", item("r1", 40, 60)));
		assertEquals(0, reserved());
		assertEquals(100, used());
	}

	@Test
	void aFailedBatchGivesBackItsReservation() {
		BatchUsage usage = admitBatch(List.of(Map.of("custom_id", "r1", "max_tokens", 100)));
		ModelBatchManager.holdBatchUsage(engine, "batch-failed", usage);
		ModelBatchManager.releaseFailedBatch(engine, "batch-failed", "IN_PROGRESS");
		assertTrue(reserved() > 0);
		ModelBatchManager.releaseFailedBatch(engine, "batch-failed", "FAILED");
		assertEquals(0, reserved());
	}

	/*
	 * Helpers
	 */

	private BatchUsage admitBatch(List<Map<String, Object>> requests) {
		ModelUsageLimitPath path = directPath(ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1_000_000L, Window.DAY, Counts.EACH_PERSON))));
		try (MockedStatic<ModelUsageRestrictionUtility> admit = mockStatic(ModelUsageRestrictionUtility.class,
				CALLS_REAL_METHODS)) {
			admit.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any()))
					.thenAnswer(call -> ModelUsageGuard.admit(path, "GPT-5", null, call.getArgument(2)));
			return ModelBatchManager.admitBatch(mock(User.class), engine, requests);
		}
	}

	private static ModelUsageLimitPath directPath(ModelUsageLimit limit) {
		ModelUsageLimitPath path = new ModelUsageLimitPath(MODEL, PERSON, LOGIN);
		Step direct = new Step(Layer.BILLED, StepLimitType.DIRECT, ModelUsageLimitKey.direct(MODEL, PERSON, null),
				limit);
		path.addStep(direct);
		path.setBilled(StepLimitType.DIRECT, List.of(direct));
		return path;
	}

	private static Map<String, Object> item(String customId, int inputTokens, int outputTokens) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("custom_id", customId);
		item.put("ok", true);
		item.put("input_tokens", inputTokens);
		item.put("output_tokens", outputTokens);
		return item;
	}

	@SafeVarargs
	private static BatchResultsResponse results(String status, Map<String, Object>... items) {
		Map<String, Object> results = new LinkedHashMap<>();
		results.put("status", status);
		results.put("results", List.of(items));
		return BatchResultsResponse.fromObject(results);
	}

	private String key() {
		return ModelUsageEvaluation.keyOf(ModelUsageCounterScope.personModel(MODEL, PERSON, LOGIN), Metric.TOKENS,
				Window.DAY, ModelUsageWindows.bucketStart(Window.DAY, clock.instant()));
	}

	private long used() {
		return store.get(List.of(key())).get(key()).used();
	}

	private long reserved() {
		return store.get(List.of(key())).get(key()).reserved();
	}
}
