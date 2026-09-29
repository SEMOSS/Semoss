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
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.TypeSafeEngine;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

class TypeSafeReactorUnitTests {

	private TypeSafeReactor reactor;
	private Insight insight;
	private User user;
	private TypeSafeEngine engine;
	private final Map<String, Object> questions = Map.of("urgent", Map.of("type", "noul", "instructions", "Urgent?"));

	@BeforeEach
	void setup() {
		reactor = new TypeSafeReactor();
		reactor.In();
		insight = mock(Insight.class);
		user = mock(User.class);
		engine = mock(TypeSafeEngine.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);
		reactor.getNounStore().makeNoun("engine").addLiteral("model-id");
		reactor.getNounStore().makeNoun("questions").add(questions, PixelDataType.MAP);
		TypeSafeModelEngineResponse response = TypeSafeModelEngineResponse.fromObject(Map.of("model", "jev-latest",
				"usage", Map.of("input_tokens", 10, "output_tokens", 2), "answers",
				Map.of("urgent", Map.of("type", "noul", "noul", 0.9))));
		when(engine.evaluate(any(), anyMap(), same(insight), anyMap())).thenReturn(response);
	}

	@Test
	void evaluatesThroughTheRealModelProxyAndRetainsAuditLogging() {
		reactor.getNounStore().makeNoun("state").addLiteral("ticket");
		when(engine.getSmssProp()).thenReturn(new CaseInsensitiveProperties());
		when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.MODEL);
		when(engine.getEngineId()).thenReturn("model-id");
		when(engine.getEngineName()).thenReturn("Jev");
		when(engine.getCatalogSubType(any())).thenReturn("TYPESAFE");
		Logger logger = mock(Logger.class);
		when(engine.getEngineLogger("EngineLogger")).thenReturn(logger);
		IModelEngine guarded = EngineProxyFactory.createGuardedModelEngine(engine);
		assertTrue(Proxy.isProxyClass(guarded.getClass()));
		assertFalse(guarded instanceof TypeSafeEngine);

		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility();
				MockedStatic<ThreadStore> threadStore = mockStatic(ThreadStore.class)) {
			utility.when(() -> Utility.getModel("model-id")).thenReturn(guarded);
			NounMetadata result = reactor.execute();
			assertEquals(PixelDataType.MAP, result.getNounType());
			verify(engine).evaluate(eq("ticket"), same(questions), same(insight), eq(Map.of()));
			verify(logger).info(any(Map.class));
		}
	}

	@Test
	void passesNamedMapsWithoutConfusingStateQuestionsAndParameters() {
		Map<String, Object> state = Map.of("ticket", "help", "active", false);
		Map<String, Object> parameters = Map.of("max_retries", 0);
		reactor.getNounStore().makeNoun("state").add(state, PixelDataType.MAP);
		reactor.getNounStore().makeNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey()).add(parameters, PixelDataType.MAP);
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			NounMetadata result = reactor.execute();
			assertEquals(PixelDataType.MAP, result.getNounType());
			assertTrue(((Map<?, ?>) result.getValue()).get("response") instanceof Map);
			verify(engine).evaluate(same(state), same(questions), same(insight), same(parameters));
		}
	}

	@Test
	void preservesMultipleStateValuesAndDefaultsOptionalParameters() {
		reactor.getNounStore().makeNoun("state").addLiteral("first");
		reactor.getNounStore().makeNoun("state").addLiteral("second");
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).evaluate(eq(List.of("first", "second")), same(questions), same(insight), eq(Map.of()));
		}
	}

	@Test
	void supportsPositionalStateAlongsideNamedQuestions() {
		reactor.getCurRow().addLiteral("ticket");
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			reactor.execute();
			verify(engine).evaluate(eq("ticket"), same(questions), same(insight), eq(Map.of()));
		}
	}

	@Test
	void checksAccessBeforeLoadingModel() {
		reactor.getNounStore().makeNoun("state").addLiteral("ticket");
		try (MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			assertThrows(IllegalArgumentException.class, reactor::execute);
			utility.verifyNoInteractions();
			verifyNoInteractions(engine);
		}
	}

	@Test
	void rejectsWrongEngineAndMalformedMaps() {
		reactor.getNounStore().makeNoun("state").addLiteral("ticket");
		try (MockedStatic<SecurityEngineUtils> security = security(); MockedStatic<Utility> utility = utility()) {
			utility.when(() -> Utility.getModel("model-id")).thenReturn(mock(IModelEngine.class));
			assertTrue(assertThrows(IllegalArgumentException.class, reactor::execute).getMessage().contains("TYPESAFE"));
			utility.when(() -> Utility.getModel("model-id")).thenReturn(engine);
			reactor.getNounStore().makeNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey()).addLiteral("not a map");
			assertThrows(IllegalArgumentException.class, reactor::execute);
			verifyNoInteractions(engine);
		}
	}

	@Test
	void requiresStateAndQuestions() {
		assertThrows(IllegalArgumentException.class, reactor::execute);
		verifyNoInteractions(engine);
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
