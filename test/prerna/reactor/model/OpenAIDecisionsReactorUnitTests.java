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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IOpenAIDecisionsEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.responses.OpenAIDecisionsModelEngineResponse;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

class OpenAIDecisionsReactorUnitTests {

	private OpenAIDecisionsReactor reactor;
	private Insight insight;
	private User user;
	private IOpenAIDecisionsEngine engine;
	private final Map<String, Object> question = Map.of("name", "urgent", "type", "predicate", "instructions", "Urgent?");
	private final Map<String, Object> message = Map.of("role", "user", "content", "Please fix my invoice.");
	private final Map<String, Object> nativeResponse = Map.of("model", "decision-model", "answers",
			List.of(Map.of("name", "urgent", "type", "refusal")), "usage",
			Map.of("input_tokens", 10, "output_tokens", 0), "provider_detail", "preserved");

	@BeforeEach
	void setup() {
		reactor = new OpenAIDecisionsReactor();
		reactor.In();
		insight = mock(Insight.class);
		user = mock(User.class);
		engine = mock(IOpenAIDecisionsEngine.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);
		reactor.getNounStore().makeNoun("engine").addLiteral("model-id");
		reactor.getNounStore().makeNoun("input").addLiteral("ticket");
		reactor.getNounStore().makeNoun("questions").add(question, PixelDataType.MAP);
		when(engine.decisions(any(), anyList(), same(insight), anyMap()))
				.thenReturn(OpenAIDecisionsModelEngineResponse.fromObject(nativeResponse));
	}

	@Test
	void invokesThroughTheRealModelProxyAndRetainsAuditLogging() {
		when(engine.getSmssProp()).thenReturn(new CaseInsensitiveProperties());
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
		when(engine.getEngineId()).thenReturn("model-id");
		when(engine.getEngineName()).thenReturn("OpenAI");
		when(engine.getCatalogSubType(any())).thenReturn("OPEN_AI");
		Logger logger = mock(Logger.class);
		when(engine.getEngineLogger("EngineLogger")).thenReturn(logger);
		IModelEngine guarded = EngineProxyFactory.createGuardedModelEngine(engine);
		assertTrue(Proxy.isProxyClass(guarded.getClass()));
		assertInstanceOf(IOpenAIDecisionsEngine.class, guarded);

		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility();
				MockedStatic<ThreadStore> threadStore = mockStatic(ThreadStore.class)) {
			utility.when(() -> Utility.getModel("model-id")).thenReturn(guarded);
			reactor.execute();
			verify(engine).decisions(eq("ticket"), eq(List.of(question)), same(insight), eq(Map.of()));
			verify(logger).info(any(Map.class));
		}
	}

	@Test
	void preservesNativeAnswersIncludingRefusalsInTheSemossEnvelope() {
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			NounMetadata result = reactor.execute();
			assertEquals(PixelDataType.MAP, result.getNounType());
			Map<?, ?> envelope = (Map<?, ?>) result.getValue();
			assertEquals(nativeResponse, envelope.get("response"));
			assertEquals(10, envelope.get("numberOfTokensInPrompt"));
			assertEquals(0, envelope.get("numberOfTokensInResponse"));
		}
	}

	@Test
	void preservesMultipleQuestionsAndRequestOptions() {
		Map<String, Object> secondQuestion = Map.of("type", "choice", "instructions", "Route?", "choices",
				List.of(Map.of("value", "billing", "description", "Payments"),
						Map.of("value", "technical", "description", "Bugs")));
		Map<String, Object> parameters = Map.of("timeout", 30, "max_retries", 2);
		reactor.getNounStore().makeNoun("questions").add(secondQuestion, PixelDataType.MAP);
		reactor.getNounStore().makeNoun("paramValues").add(parameters, PixelDataType.MAP);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq("ticket"), eq(List.of(question, secondQuestion)), same(insight), same(parameters));
		}
	}

	@Test
	void preservesASingleNativeMessageAsAList() {
		replaceInput(message, PixelDataType.MAP);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq(List.of(message)), eq(List.of(question)), same(insight), eq(Map.of()));
		}
	}

	@Test
	void preservesMultipleNativeMessagesAndContentParts() {
		Map<String, Object> imageMessage = Map.of("role", "user", "content",
				List.of(Map.of("type", "input_text", "text", "Read the invoice"),
						Map.of("type", "input_image", "image_url", "data:image/png;base64,fixture")));
		replaceInput(message, PixelDataType.MAP);
		reactor.getNounStore().makeNoun("input").add(imageMessage, PixelDataType.MAP);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq(List.of(message, imageMessage)), eq(List.of(question)), same(insight), eq(Map.of()));
		}
	}

	@Test
	void preservesNestedListsFromVariables() {
		replaceInput(List.of(message), PixelDataType.VECTOR);
		GenRowStruct questions = reactor.getNounStore().makeNoun("questions");
		questions.clear();
		questions.add(List.of(question), PixelDataType.VECTOR);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq(List.of(message)), eq(List.of(question)), same(insight), eq(Map.of()));
		}
	}

	@Test
	void supportsPositionalInputAlongsideNamedQuestions() {
		reactor.getNounStore().removeNoun("input");
		reactor.getCurRow().addLiteral("positional ticket");
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq("positional ticket"), eq(List.of(question)), same(insight), eq(Map.of()));
		}
	}

	@Test
	void supportsPositionalListsAndOptionsWithoutConfusingMaps() {
		reactor.getNounStore().removeNoun("engine");
		reactor.getNounStore().removeNoun("input");
		reactor.getNounStore().removeNoun("questions");
		Map<String, Object> parameters = Map.of("timeout", 10);
		reactor.getCurRow().addLiteral("model-id");
		reactor.getCurRow().add(List.of(message), PixelDataType.VECTOR);
		reactor.getCurRow().add(List.of(question), PixelDataType.VECTOR);
		reactor.getCurRow().add(parameters, PixelDataType.MAP);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).decisions(eq(List.of(message)), eq(List.of(question)), same(insight), same(parameters));
		}
	}

	@Test
	void checksAccessBeforeLoadingModel() {
		try (MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			assertThrows(IllegalArgumentException.class, reactor::execute);
			utility.verifyNoInteractions();
			verifyNoInteractions(engine);
		}
	}

	@Test
	void rejectsEnginesWithoutTheDecisionsCapability() {
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			utility.when(() -> Utility.getModel("model-id")).thenReturn(mock(IModelEngine.class));
			assertTrue(assertThrows(IllegalArgumentException.class, reactor::execute).getMessage().contains("OpenAIDecisions"));
			verifyNoInteractions(engine);
		}
	}

	@Test
	void rejectsRawStateMapsAndNonNativeInputValues() {
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			for (Object invalid : List.of(Map.of("ticket", "help"), 42, true, List.of(), List.of("first", "second"))) {
				replaceInput(invalid, PixelDataType.VECTOR);
				assertTrue(assertThrows(IllegalArgumentException.class, reactor::execute).getMessage().contains("input"));
			}
			verifyNoInteractions(engine);
		}
	}

	@Test
	void rejectsMalformedQuestionListsAndParameterMaps() {
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			GenRowStruct questions = reactor.getNounStore().makeNoun("questions");
			for (Object invalid : List.of(List.of(), "not a question", Map.of(), Map.of(1, "value"))) {
				questions.clear();
				questions.add(invalid, PixelDataType.VECTOR);
				assertTrue(assertThrows(IllegalArgumentException.class, reactor::execute).getMessage().contains("questions"));
			}
			questions.clear();
			questions.add(question, PixelDataType.MAP);
			GenRowStruct parameters = reactor.getNounStore().makeNoun("paramValues");
			for (Object invalid : List.of("not a map", Map.of(1, "value"), List.of(Map.of(), Map.of()))) {
				parameters.clear();
				parameters.add(invalid, PixelDataType.VECTOR);
				assertTrue(assertThrows(IllegalArgumentException.class, reactor::execute).getMessage().contains("paramValues"));
			}
			verifyNoInteractions(engine);
		}
	}

	@Test
	void requiresInputAndQuestions() {
		reactor.getNounStore().removeNoun("input");
		reactor.getNounStore().removeNoun("questions");
		String error = assertThrows(IllegalArgumentException.class, reactor::execute).getMessage();
		assertTrue(error.contains("input"));
		assertTrue(error.contains("questions"));
		verifyNoInteractions(engine);
	}

	private void replaceInput(Object value, PixelDataType type) {
		GenRowStruct input = reactor.getNounStore().makeNoun("input");
		input.clear();
		input.add(value, type);
	}

	private MockedStatic<SecurityEngineUtils> security() {
		MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class);
		security.when(() -> SecurityEngineUtils.userCanViewEngine(user, "model-id")).thenReturn(true);
		return security;
	}

	private MockedStatic<Utility> utility() {
		MockedStatic<Utility> utility = mockStatic(Utility.class);
		utility.when(() -> Utility.getModel("model-id")).thenReturn(engine);
		return utility;
	}
}
