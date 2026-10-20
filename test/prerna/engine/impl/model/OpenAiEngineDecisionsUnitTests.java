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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IOpenAIDecisionsEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.responses.OpenAIDecisionsModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageLimitException;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.om.Insight;

class OpenAiEngineDecisionsUnitTests {

	@Test
	void passesCallerDataAsJsonAndBillsNativeUsage() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		String input = "Quotes: \" ' ''' \\\n\t\u00e9 ); __import__('os').system('untrusted')";
		Map<String, Object> extra = new LinkedHashMap<>();
		extra.put("untrusted') or __import__('os').system('untrusted') #", null);
		extra.put("enabled", false);
		Map<String, Object> parameters = Map.of("timeout", 5, "extra_body", extra);
		Map<String, Object> output = output();
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output);
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), same(engine), any()))
					.thenReturn(ModelUsageTicket.none());
			OpenAIDecisionsModelEngineResponse response = engine.decisions(input, questions(), insight, parameters);
			assertEquals(output, response.getResponse());
			assertEquals(425, response.getNumberOfTokensInPrompt());
			assertEquals(0, response.getNumberOfTokensInResponse());
			assertTrue(engine.socketChecked);
			usage.verify(() -> ModelUsageRestrictionUtility.bill(any(ModelUsageTicket.class), same(response), any(), any()));
		}
		Map<?, ?> request = pythonRequest(engine, insight);
		assertEquals(input, request.get("input"));
		assertEquals(questions(), request.get("questions"));
		assertEquals(5.0, request.get("timeout"));
		assertEquals(extra, request.get("extra_body"));
		assertFalse(request.containsKey("parameters"));
		assertFalse(request.containsKey("model"));
		assertFalse(request.containsKey("messages"));
		assertFalse(request.containsKey(ModelUsageTicket.USAGE_CEILING));
	}

	@Test
	void preservesNativeMessageAndImageInput() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		List<Map<String, Object>> input = List.of(Map.of("role", "user", "content", List.of(
				Map.of("type", "input_text", "text", "Does this receipt show two charges?"),
				Map.of("type", "input_image", "image_url", "data:image/png;base64,aW1hZ2U="))));
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output());
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ModelUsageTicket.none());
			engine.decisions(input, questions(), insight, null);
		}
		assertEquals(input, pythonRequest(engine, insight).get("input"));
	}

	@Test
	void usageRestrictionFailurePreventsPythonCall() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		ModelUsageLimitException rejected = new ModelUsageLimitException("Usage limit reached",
				StepLimitType.DIRECT, false, "2026-10-20T00:00:00Z");
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenThrow(rejected);
			assertSame(rejected, assertThrows(ModelUsageLimitException.class,
					() -> engine.decisions("ticket", questions(), insight, null)));
		}
		assertFalse(engine.socketChecked);
		verifyNoInteractions(engine.pyTranslator);
	}

	@Test
	void forwardsUsageCeilingBesideNativeSdkOptions() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		Map<String, Object> ceiling = new LinkedHashMap<>();
		ceiling.put("tokens", 5000L);
		ceiling.put("cost_micros", null);
		ceiling.put("rates", null);
		ceiling.put("min_output_tokens", 256L);
		ceiling.put("margin", 0.05);
		when(ticket.pythonCeiling()).thenReturn(ceiling);
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output());
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ticket);
			engine.decisions("ticket", questions(), insight, Map.of("timeout", 5, "max_retries", 2));
		}
		Map<?, ?> request = pythonRequest(engine, insight);
		assertEquals("ticket", request.get("input"));
		assertEquals(questions(), request.get("questions"));
		assertEquals(5.0, request.get("timeout"));
		assertEquals(2.0, request.get("max_retries"));
		Gson gson = new GsonBuilder().serializeNulls().create();
		assertEquals(gson.fromJson(gson.toJson(ceiling), Map.class), request.get(ModelUsageTicket.USAGE_CEILING));
		assertFalse(request.containsKey("parameters"));
	}

	@Test
	void callThatDoesNotFitReleasesReservationAndKeepsNamedLimitError() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		ModelUsageLimitException rejected = new ModelUsageLimitException("There is not enough left of your limit.",
				StepLimitType.DIRECT, true, "2026-10-20T00:00:00Z");
		when(ticket.doesNotFit(any())).thenReturn(rejected);
		Map<String, Object> refusal = Map.of("messageType", "ERROR", "message", "Request does not fit usage limit",
				"code", 429, "reason", "USAGE_LIMIT_DOES_NOT_FIT");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(refusal);
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ticket);
			assertSame(rejected, assertThrows(ModelUsageLimitException.class,
					() -> engine.decisions("ticket", questions(), insight, null)));
			usage.verify(() -> ModelUsageRestrictionUtility.bill(any(), any(), any(), any()), never());
		}
		verify(ticket).release();
	}

	@Test
	void pythonFailureReleasesReservationWithoutBilling() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		IllegalStateException failed = new IllegalStateException("Provider failed");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenThrow(failed);
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ticket);
			assertSame(failed, assertThrows(IllegalStateException.class,
					() -> engine.decisions("ticket", questions(), insight, null)));
			usage.verify(() -> ModelUsageRestrictionUtility.bill(any(), any(), any(), any()), never());
		}
		verify(ticket).release();
	}

	@Test
	void malformedProviderResponseReleasesReservationWithoutBilling() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString()))
				.thenReturn(Map.of("error", "invalid response"));
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ticket);
			assertThrows(IllegalArgumentException.class, () -> engine.decisions("ticket", questions(), insight, null));
			usage.verify(() -> ModelUsageRestrictionUtility.bill(any(), any(), any(), any()), never());
		}
		verify(ticket).release();
	}

	@Test
	void recordsDecisionsAndUsageWhenInferenceLoggingIsEnabled() {
		TestEngine engine = new TestEngine();
		engine.inferenceLogsEnbaled = true;
		Insight insight = insight();
		when(insight.getInsightId()).thenReturn("insight-id");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output());
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class);
				MockedConstruction<ModelEngineInferenceLogsWorker> records = mockConstruction(
						ModelEngineInferenceLogsWorker.class, (worker, context) -> {
							List<?> args = context.arguments();
							assertEquals("decisions", args.get(2));
							assertSame(engine, args.get(3));
							assertEquals("insight-id", args.get(9));
							assertTrue(args.get(12) instanceof Map);
							assertEquals(425, args.get(13));
							assertEquals(0, args.get(16));
							assertEquals(425, args.get(18));
							assertEquals(0, args.get(19));
						})) {
			usage.when(() -> ModelUsageRestrictionUtility.admit(any(), any(), any())).thenReturn(ModelUsageTicket.none());
			engine.decisions("ticket", questions(), insight, null);
			assertEquals(1, records.constructed().size());
		}
	}

	@Test
	void preservesAnswerOrderRefusalsNullNamesAndFutureFields() {
		Map<String, Object> raw = output();
		OpenAIDecisionsModelEngineResponse response = OpenAIDecisionsModelEngineResponse.fromObject(raw);
		assertEquals(raw, response.getResponse());
		assertEquals(raw, response.toMap().get("response"));
		assertEquals(425, response.getNumberOfTokensInPrompt());
		assertEquals(0, response.getNumberOfTokensInResponse());
	}

	@Test
	void extractsOptionalUsageDetailsWithoutChangingNativeResponse() {
		Map<String, Object> raw = output();
		Map<String, Object> usage = new LinkedHashMap<>(Map.of("input_tokens", 425L, "output_tokens", 0));
		usage.put("input_tokens_details", Map.of("cached_tokens", 80L, "cache_write_tokens", 12.0));
		usage.put("output_tokens_details", Map.of("reasoning_tokens", 0L, "future_tokens", 0));
		raw.put("usage", usage);
		OpenAIDecisionsModelEngineResponse response = OpenAIDecisionsModelEngineResponse.fromObject(raw);
		assertEquals(80, response.getNumberOfCacheReadTokens());
		assertEquals(12, response.getNumberOfCacheCreationTokens());
		assertEquals(0, response.getNumberOfThinkingTokens());
		assertEquals(raw, response.getResponse());

		Map<String, Object> nullDetails = new LinkedHashMap<>();
		nullDetails.put("cached_tokens", null);
		usage.put("input_tokens_details", nullDetails);
		usage.put("output_tokens_details", null);
		response = OpenAIDecisionsModelEngineResponse.fromObject(raw);
		assertNull(response.getNumberOfCacheReadTokens());
		assertNull(response.getNumberOfCacheCreationTokens());
		assertNull(response.getNumberOfThinkingTokens());
		assertEquals(raw, response.getResponse());
	}

	@Test
	void rejectsMalformedNativeShapeAndUnbillableTokenCounts() {
		assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject("SDK repr"));
		Map<String, Object> raw = output();
		raw.put("answers", Map.of("urgent", true));
		assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject(raw));
		raw.put("answers", List.of());
		for (Object count : List.of(-1, 0.5, Double.NaN, Double.POSITIVE_INFINITY, "10", Long.MAX_VALUE)) {
			raw.put("usage", Map.of("input_tokens", count, "output_tokens", 0));
			assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject(raw));
			raw.put("usage", Map.of("input_tokens", 0, "output_tokens", count));
			assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject(raw));
		}
		for (String key : List.of("input_tokens", "output_tokens")) {
			Map<String, Object> incomplete = new LinkedHashMap<>(Map.of("input_tokens", 10, "output_tokens", 0));
			incomplete.remove(key);
			raw.put("usage", incomplete);
			assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject(raw));
			incomplete.put(key, null);
			assertThrows(IllegalArgumentException.class, () -> OpenAIDecisionsModelEngineResponse.fromObject(raw));
		}
	}

	@Test
	void rejectsOverridesOfEngineRequestAndUsageCeilingBeforeAdmission() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			for (String key : List.of("model", "input", "questions", ModelUsageTicket.USAGE_CEILING)) {
				assertThrows(IllegalArgumentException.class,
						() -> engine.decisions("ticket", questions(), insight, Map.of(key, "override")));
				assertThrows(IllegalArgumentException.class, () -> engine.decisions("ticket", questions(), insight,
						Map.of("extra_body", Map.of(key, "override"))));
			}
			usage.verifyNoInteractions();
		}
		assertFalse(engine.socketChecked);
		verifyNoInteractions(engine.pyTranslator);
	}

	@Test
	void rejectsMissingInputQuestionsAndUserBeforePython() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		assertThrows(IllegalArgumentException.class, () -> engine.decisions(null, questions(), insight, null));
		assertThrows(IllegalArgumentException.class, () -> engine.decisions(Map.of("ticket", "help"), questions(), insight, null));
		assertThrows(IllegalArgumentException.class, () -> engine.decisions(List.of("not a message"), questions(), insight, null));
		assertThrows(IllegalArgumentException.class, () -> engine.decisions("ticket", List.of(), insight, null));
		assertThrows(IllegalArgumentException.class, () -> engine.decisions("ticket", questions(), null, null));
		assertThrows(IllegalArgumentException.class, () -> engine.decisions("ticket", questions(), mock(Insight.class), null));
		assertFalse(engine.socketChecked);
		verifyNoInteractions(engine.pyTranslator);
	}

	@Test
	void guardedOpenAiEngineRetainsOnlyItsSupportedCapability() {
		OpenAiEngine openAi = mock(OpenAiEngine.class);
		configureProxy(openAi);
		IModelEngine guarded = EngineProxyFactory.createGuardedModelEngine(openAi);
		assertTrue(Proxy.isProxyClass(guarded.getClass()));
		assertTrue(guarded instanceof IOpenAIDecisionsEngine);
		assertFalse(guarded instanceof ITypeSafeEngine);
		assertFalse(guarded instanceof OpenAiEngine);
		AzureOpenAiEngine azure = mock(AzureOpenAiEngine.class);
		configureProxy(azure);
		assertFalse(EngineProxyFactory.createGuardedModelEngine(azure) instanceof IOpenAIDecisionsEngine);
	}

	private static void configureProxy(IModelEngine engine) {
		when(engine.getSmssProp()).thenReturn(new CaseInsensitiveProperties());
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
		when(engine.getEngineId()).thenReturn("engine");
		when(engine.getCatalogSubType(any())).thenReturn("OPEN_AI");
	}

	private static Map<?, ?> pythonRequest(TestEngine engine, Insight insight) {
		ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
		verify(engine.pyTranslator).runDirectPyNoCancelTrace(eq(insight), command.capture());
		String prefix = "test_model.decisions(**__import__('json').loads(";
		String script = command.getValue();
		assertTrue(script.startsWith(prefix));
		assertTrue(script.endsWith("))"));
		Gson gson = new Gson();
		String json = gson.fromJson(script.substring(prefix.length(), script.length() - 2), String.class);
		return gson.fromJson(json, Map.class);
	}

	private static Insight insight() {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		return insight;
	}

	private static List<Map<String, Object>> questions() {
		return List.of(Map.of("name", "is_urgent", "type", "predicate", "instructions", "Does this need action today?"));
	}

	private static Map<String, Object> output() {
		Map<String, Object> unnamed = new LinkedHashMap<>();
		unnamed.put("name", null);
		unnamed.put("type", "refusal");
		unnamed.put("refusal", "Cannot answer this question");
		return new LinkedHashMap<>(Map.of("id", "decision-id", "model", "gpt-6-luna",
				"usage", Map.of("input_tokens", 425L, "output_tokens", 0.0, "future_usage", 2),
				"answers", List.of(Map.of("name", "is_urgent", "type", "predicate", "probability", 0.99), unnamed,
						Map.of("name", "future", "type", "future", "value", List.of(1, 2))),
				"future_metadata", Map.of("preserved", true)));
	}

	private static class TestEngine extends OpenAiEngine {
		boolean socketChecked;

		TestEngine() {
			this.varName = "test_model";
			this.engineId = "engine";
			this.pyTranslator = mock(PyTranslator.class);
			this.inferenceLogsEnbaled = false;
		}

		@Override
		protected void checkSocketStatus() {
			this.socketChecked = true;
		}
	}
}
