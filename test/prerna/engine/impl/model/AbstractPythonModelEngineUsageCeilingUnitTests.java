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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.ds.py.PyTranslator;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageLimitException;
import prerna.engine.impl.model.usage.ModelUsageTicket;
import prerna.om.Insight;

class AbstractPythonModelEngineUsageCeilingUnitTests {

	@Test
	void theCeilingReachesPythonAsADictKeywordArgument() {
		TestEngine engine = new TestEngine();
		Map<String, Object> rates = new LinkedHashMap<>();
		rates.put("input", 2.5);
		rates.put("output", 10.0);
		rates.put("cache_read", null);
		Map<String, Object> ceiling = new LinkedHashMap<>();
		ceiling.put("tokens", 120000L);
		ceiling.put("cost_micros", 2500000L);
		ceiling.put("rates", rates);
		ceiling.put("min_output_tokens", 256L);
		ceiling.put("margin", 0.05);
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put("message_json", "[]");
		parameters.put("max_tokens", 4096);
		parameters.put(ModelUsageTicket.USAGE_CEILING, ceiling);

		String command = ask(engine, parameters, Map.of("response", "ok"));

		assertTrue(command.contains("usage_ceiling={\"tokens\": 120000, \"cost_micros\": 2500000, \"rates\": "
				+ "{\"input\": 2.5, \"output\": 10.0, \"cache_read\": None}, \"min_output_tokens\": 256, "
				+ "\"margin\": 0.05}"), command);
		// the output maximum always rides with the ceiling
		assertTrue(command.contains("max_tokens=4096"), command);
	}

	@Test
	void aCallThatDoesNotFitIsRefusedAsAUsageLimit() {
		TestEngine engine = new TestEngine();
		Map<String, Object> refused = new LinkedHashMap<>();
		refused.put("messageType", "ERROR");
		refused.put("message", "There is not enough left of your usage limit for this request");
		refused.put("error_type", "Usage Limit");
		refused.put("code", 429);
		refused.put("reason", "USAGE_LIMIT_DOES_NOT_FIT");
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class,
				() -> ask(engine, Map.of("message_json", "[]"), refused));
		assertTrue(e.callDoesNotFit());
		assertEquals("There is not enough left of your usage limit for this request", e.getMessage());
	}

	@Test
	void embeddingsThatDoNotFitAreRefusedBeforeTheResponseIsRead() {
		TestEngine engine = new TestEngine();
		Map<String, Object> refused = new LinkedHashMap<>();
		refused.put("messageType", "ERROR");
		refused.put("response", "There is not enough left of your usage limit for this request");
		refused.put("code", 429);
		refused.put("reason", "USAGE_LIMIT_DOES_NOT_FIT");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(anyString())).thenReturn(refused);
		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class,
				() -> engine.embeddingsCall(List.of("a"), mock(Insight.class), Map.of()));
		assertTrue(e.callDoesNotFit());
	}

	@Test
	void multiModalEmbeddingsTakeTheCeilingAndAreRefusedWhenTheyDoNotFit() {
		TestEngine engine = new TestEngine();
		Map<String, Object> refused = new LinkedHashMap<>();
		refused.put("messageType", "ERROR");
		refused.put("response", "There is not enough left of your usage limit for this request");
		refused.put("code", 429);
		refused.put("reason", "USAGE_LIMIT_DOES_NOT_FIT");
		when(engine.pyTranslator.runDirectPyNoCancelTrace(anyString())).thenReturn(refused);
		// a ceiling passed on, as a router model does
		Map<String, Object> ceiling = new LinkedHashMap<>();
		ceiling.put("tokens", 500L);
		Map<String, Object> parameters = new LinkedHashMap<>();
		parameters.put(ModelUsageTicket.USAGE_CEILING, ceiling);

		ModelUsageLimitException e = assertThrows(ModelUsageLimitException.class, () -> engine
				.multiModalEmbeddings(List.of("a"), List.of("image"), null, mock(Insight.class), parameters));
		assertTrue(e.callDoesNotFit());
		ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
		verify(engine.pyTranslator).runDirectPyNoCancelTrace(command.capture());
		assertTrue(command.getValue().contains("usage_ceiling={\"tokens\": 500}"), command.getValue());
		// the caller's parameters are left as they are
		assertTrue(parameters.containsKey(ModelUsageTicket.USAGE_CEILING));
	}

	@Test
	void anAnswerCutShortByTheCeilingIsMarked() {
		TestEngine engine = new TestEngine();
		Map<String, Object> capped = new LinkedHashMap<>();
		capped.put("response", "partial");
		capped.put("numberOfTokensInPrompt", 10);
		capped.put("numberOfTokensInResponse", 256);
		capped.put("usageCapped", true);
		Insight insight = insight();
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(capped);
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			AskModelEngineResponse<?> response = engine.askCall(null, insight, "room-test",
					Map.of("message_json", "[]"));
			assertTrue(response.isUsageCapped());
		}
		assertFalse(AskModelEngineResponse.fromObject(Map.of("response", "full")).isUsageCapped());
	}

	@Test
	void pythonEnginesTakeTheCeiling() {
		assertTrue(new TestEngine().acceptsUsageCeiling());
	}

	private static String ask(TestEngine engine, Map<String, Object> parameters, Map<String, Object> output) {
		Insight insight = insight();
		when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString())).thenReturn(output);
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			engine.askCall(null, insight, "room-test", parameters);
		}
		ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
		verify(engine.pyTranslator).runDirectPyNoCancelTrace(eq(insight), command.capture());
		return command.getValue();
	}

	private static Insight insight() {
		Insight insight = mock(Insight.class);
		when(insight.getUserId()).thenReturn("user-test");
		return insight;
	}

	private static class TestEngine extends AbstractPythonModelEngine {
		private TestEngine() {
			this.varName = "test_model";
			this.pyTranslator = mock(PyTranslator.class);
			setBasic(true);
		}

		@Override
		protected void checkSocketStatus() {
			// Unit tests capture the Python call instead of starting a process.
		}

		@Override
		public ModelTypeEnum getModelType() {
			return ModelTypeEnum.OPEN_AI;
		}
	}
}
