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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;

class TypeSafeEngineUnitTests {

	@Test
	void passesJsonDataAndExecutionInsightWithoutChatArguments() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		Map<String, Object> state = new LinkedHashMap<>();
		state.put("text", "Quotes: \" ' ''' \\\n\t\u00e9");
		state.put("active", false);
		state.put("context", null);
		Map<String, Object> criteria = new LinkedHashMap<>();
		criteria.put("billing", null);
		criteria.put("technical", "bugs");
		Map<String, Object> questions = Map.of("department", Map.of("type", "choice", "criteria", criteria));
		Map<String, Object> parameters = Map.of("timeout", 5);
		Map<String, Object> output = output();
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output);

		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), "engine"))
					.thenReturn(Map.of());
			TypeSafeModelEngineResponse response = engine.evaluate(state, questions, insight, parameters);
			assertEquals(output, response.getResponse());
			assertEquals(425, response.getNumberOfTokensInPrompt());
			assertEquals(73, response.getNumberOfTokensInResponse());
			assertTrue(engine.socketChecked);
			usage.verify(() -> ModelUsageRestrictionUtility.updateRestrictionMapCurrentUsage(anyMap(), same(response),
					any(), any()));
		}
		ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
		verify(engine.pyTranslator).runDirectPyNoCancelTrace(eq(insight), command.capture());
		String prefix = "test_model.ask(**__import__('json').loads(";
		String script = command.getValue();
		assertTrue(script.startsWith(prefix));
		assertTrue(script.endsWith("))"));
		// PyUtils's quoted literal is also a JSON string, so decode both layers and
		// verify quotes, newlines, booleans, and null descriptions survived intact.
		Gson gson = new Gson();
		String json = gson.fromJson(script.substring(prefix.length(), script.length() - 2), String.class);
		Map<?, ?> request = gson.fromJson(json, Map.class);
		assertEquals(state, request.get("state"));
		assertEquals(questions, request.get("questions"));
		assertEquals(Map.of("timeout", 5.0), request.get("parameters"));
		assertEquals(3, request.size());
	}

	@Test
	void recordsEvaluationAndTokenUsageWhenInferenceLoggingIsEnabled() {
		TestEngine engine = new TestEngine();
		engine.inferenceLogsEnbaled = true;
		Insight insight = insight();
		when(insight.getInsightId()).thenReturn("insight-id");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output());
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class);
				MockedConstruction<ModelEngineInferenceLogsWorker> records = mockConstruction(
						ModelEngineInferenceLogsWorker.class, (worker, context) -> {
							List<?> args = context.arguments();
							assertEquals("system_one", args.get(2));
							assertSame(engine, args.get(3));
							assertEquals("insight-id", args.get(9));
							assertTrue(args.get(12) instanceof Map);
							assertEquals(425, args.get(13));
							assertEquals(73, args.get(16));
							assertEquals(425, args.get(18));
							assertEquals(73, args.get(19));
						})) {
			engine.evaluate("ticket", Map.of("q", Map.of("type", "noul")), insight, null);
			assertEquals(1, records.constructed().size());
		}
	}

	@Test
	void usageRestrictionFailurePreventsPythonCall() {
		TestEngine engine = new TestEngine();
		Insight insight = insight();
		try (MockedStatic<ModelUsageRestrictionUtility> usage = mockStatic(ModelUsageRestrictionUtility.class)) {
			usage.when(() -> ModelUsageRestrictionUtility.getModelUsageRestriction(insight.getUser(), "engine"))
					.thenThrow(new IllegalArgumentException("Token limit exceeded"));
			assertThrows(IllegalArgumentException.class,
					() -> engine.evaluate("ticket", Map.of("q", Map.of("type", "noul")), insight, null));
		}
		assertFalse(engine.socketChecked);
		verifyNoInteractions(engine.pyTranslator);
	}

	@Test
	void invalidRequestsAndChatCallsNeverStartPython() {
		TestEngine engine = new TestEngine();
		assertThrows(IllegalArgumentException.class, () -> engine.evaluate(null, Map.of(), insight(), null));
		assertThrows(IllegalArgumentException.class, () -> engine.evaluate("ticket", Map.of(), insight(), null));
		assertThrows(IllegalArgumentException.class,
				() -> engine.evaluate("ticket", Map.of("q", Map.of("type", "noul")), null, null));
		assertThrows(UnsupportedOperationException.class, () -> engine.ask("ticket", null, null, null));
		assertThrows(UnsupportedOperationException.class, () -> engine.askCall(null, null, null, null));
		assertThrows(UnsupportedOperationException.class, () -> engine.embeddingsCall(List.of("ticket"), null, null));
		assertThrows(UnsupportedOperationException.class,
				() -> engine.multiModalEmbeddings(null, null, null, null, null));
		assertFalse(engine.supportsBatch());
		assertFalse(engine.socketChecked);
		verifyNoInteractions(engine.pyTranslator);
		assertEquals(TypeSafeEngine.class.getName(), ModelTypeEnum.getEnumFromName("typesafe").getModelClass());
	}

	@Test
	void responsePreservesNullUsageAndFutureAnswerKinds() {
		Map<String, Object> raw = output();
		Map<String, Object> usage = new LinkedHashMap<>();
		usage.put("input_tokens", null);
		raw.put("usage", usage);
		raw.put("answers", Map.of("future", Map.of("type", "future", "value", List.of(1, 2))));
		TypeSafeModelEngineResponse response = TypeSafeModelEngineResponse.fromObject(raw);
		assertEquals(raw, response.toMap().get("response"));
		assertEquals(0, response.getNumberOfTokensInPrompt());
		assertEquals(0, response.getNumberOfTokensInResponse());
		assertThrows(IllegalArgumentException.class, () -> TypeSafeModelEngineResponse.fromObject("SDK repr"));
		assertThrows(IllegalArgumentException.class, () -> TypeSafeModelEngineResponse.fromObject(Map.of()));
		for (Object count : List.of(-1, 0.5, Double.NaN, "10", Long.MAX_VALUE)) {
			raw.put("usage", Map.of("input_tokens", count));
			assertThrows(IllegalArgumentException.class, () -> TypeSafeModelEngineResponse.fromObject(raw));
		}
	}

	private static Insight insight() {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		return insight;
	}

	private static Map<String, Object> output() {
		return new LinkedHashMap<>(Map.of("model", "jev-1.13.0", "usage",
				Map.of("input_tokens", 425L, "output_tokens", 73.0), "answers",
				Map.of("is_urgent", Map.of("type", "noul", "noul", 0.99))));
	}

	private static class TestEngine extends TypeSafeEngine {
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
