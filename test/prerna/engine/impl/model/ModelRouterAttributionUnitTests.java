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
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

class ModelRouterAttributionUnitTests {

	@TempDir
	Path directory;

	@Test
	void recordsSuccessfulDestinationAndKeepsItOnStickyFollowUps() throws Exception {
		Room room = new Room();
		room.setId("room");
		InputMessage input = InputMessage.builder(room).withText("code").build();
		IModelEngine target = target("primary", "Code Model");
		when(target.askRoom(eq(input), eq(room), anyMap()))
				.thenAnswer(invocation -> new AskStringModelEngineResponse("answer", 1, 1));
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				MockedStatic<SecurityModelMetadataUtils> settings = mockStatic(SecurityModelMetadataUtils.class)) {
			utility.when(() -> Utility.getEngine("primary")).thenReturn(target);
			ModelRouterEngine router = router();
			rooms.when(() -> RoomUtils.createRoomForStatelessAsk("room", null, router, null)).thenReturn(room);
			for (int turn = 0; turn < 2; turn++) {
				AskModelEngineResponse<?> response = router.askCall(input, null, "room", Map.of());
				assertEquals("primary", response.getMetadata().get("routed_engine_id"));
				assertEquals("Code Model", response.getMetadata().get("routed_engine_name"));
				assertEquals("coding", response.getMetadata().get("routed_route_name"));
				assertEquals("router", response.getMetadata().get("router_engine_id"));
			}
			verify(target, times(2)).askRoom(eq(input), eq(room), anyMap());
		}
	}

	@Test
	void attributesTheFallbackThatActuallySucceeded() throws Exception {
		Room room = new Room();
		room.setId("room");
		InputMessage input = InputMessage.builder(room).withText("code").build();
		IModelEngine primary = target("primary", "Unavailable Model");
		IModelEngine fallback = target("fallback", "Backup Model");
		when(primary.askRoom(eq(input), eq(room), anyMap())).thenThrow(new IllegalStateException("unavailable"));
		when(fallback.askRoom(eq(input), eq(room), anyMap())).thenReturn(new AskStringModelEngineResponse("answer", 1, 1));
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				MockedStatic<SecurityModelMetadataUtils> settings = mockStatic(SecurityModelMetadataUtils.class)) {
			utility.when(() -> Utility.getEngine("primary")).thenReturn(primary);
			utility.when(() -> Utility.getEngine("fallback")).thenReturn(fallback);
			ModelRouterEngine router = router();
			rooms.when(() -> RoomUtils.createRoomForStatelessAsk("room", null, router, null)).thenReturn(room);
			AskModelEngineResponse<?> response = router.askCall(input, null, "room", Map.of());
			assertEquals("fallback", response.getMetadata().get("routed_engine_id"));
			assertEquals("Backup Model", response.getMetadata().get("routed_engine_name"));
			assertNull(response.getMetadata().get("routed_route_name"));
		}
	}

	@Test
	void preservesTheFinalServingModelFromANestedRouter() throws Exception {
		Room room = new Room();
		room.setId("room");
		InputMessage input = InputMessage.builder(room).withText("code").build();
		IModelEngine nested = target("primary", "Inner Router");
		AskStringModelEngineResponse answer = new AskStringModelEngineResponse("answer", 1, 1);
		answer.setMetadata(Map.of("router_engine_id", "primary", "routed_engine_id", "leaf",
				"routed_engine_name", "Leaf Model", "routed_route_name", "inner-route"));
		when(nested.askRoom(eq(input), eq(room), anyMap())).thenReturn(answer);
		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				MockedStatic<SecurityModelMetadataUtils> settings = mockStatic(SecurityModelMetadataUtils.class)) {
			utility.when(() -> Utility.getEngine("primary")).thenReturn(nested);
			ModelRouterEngine router = router();
			rooms.when(() -> RoomUtils.createRoomForStatelessAsk("room", null, router, null)).thenReturn(room);
			Map<String, Object> metadata = router.askCall(input, null, "room", Map.of()).getMetadata();
			assertEquals("leaf", metadata.get("routed_engine_id"));
			assertEquals("Leaf Model", metadata.get("routed_engine_name"));
			assertEquals("router", metadata.get("router_engine_id"));
		}
	}

	@Test
	void serializesAndReloadsAttributionWithoutChangingSelectedModelOrLeakingProviderMetadata() {
		AskStringModelEngineResponse answer = new AskStringModelEngineResponse("answer", 1, 1);
		answer.setMetadata(Map.of("router_engine_id", "router", "routed_engine_id", "primary",
				"routed_engine_name", "Answer Model", "routed_route_name", "coding", "private-provider-field", "hidden"));
		ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(answer).build();
		response.setModel(target("router", "Selected Router"));
		String persisted = MessageUtils.toJsonArray(List.of(response));
		Room room = new Room();
		room.setId("reload-room");
		AbstractMessage reloaded = RoomMessageStore.loadFromPersistedJson(room, persisted).get(0);
		assertEquals("router", reloaded.getModelId());
		assertEquals("Selected Router", reloaded.getOrnaments().get("modelName"));
		assertEquals(Map.of("router_engine_id", "router", "routed_engine_id", "primary",
				"routed_engine_name", "Answer Model", "routed_route_name", "coding"), reloaded.getOrnaments().get("routing"));
		assertFalse(persisted.contains("private-provider-field"));
		assertTrue(MessageUtils.toJsonWithImage(response).contains("Answer Model"));
	}

	@Test
	void copiesAttributionForPartsBasedToolResponsesAndLeavesDirectResponsesAlone() {
		AskStringModelEngineResponse answer = new AskStringModelEngineResponse("answer", 1, 1);
		answer.setParts(List.of(new prerna.engine.impl.model.message.ToolCallMessagePart(
				Map.of("id", "call", "type", "function", "function", Map.of("name", "lookup", "arguments", "{}")))));
		answer.setMetadata(Map.of("routed_engine_id", "primary", "routed_engine_name", "Answer Model"));
		assertNotNull(ResponseMessage.Builder.fromAskModelEngineResponse(answer).build().getOrnaments().get("routing"));
		answer.setMetadata(null);
		assertFalse(ResponseMessage.Builder.fromAskModelEngineResponse(answer).build().getOrnaments().containsKey("routing"));
	}

	private IModelEngine target(String id, String name) {
		IModelEngine target = mock(IModelEngine.class);
		when(target.getEngineId()).thenReturn(id);
		when(target.getDisplayName()).thenReturn(name);
		when(target.getModelType()).thenReturn(ModelTypeEnum.OPEN_AI);
		return target;
	}

	private ModelRouterEngine router() throws Exception {
		Path config = directory.resolve("router.json");
		Files.writeString(config, """
				{"routes": [{"name": "coding", "engine_id": "primary", "keywords": ["code"]}],
				 "default_route": "primary", "fallbacks": ["fallback"]}
				""");
		ModelRouterEngine router = new ModelRouterEngine() {
			@Override
			public File resolveConfigFile() { return config.toFile(); }
		};
		router.setBasic(true);
		Properties properties = new Properties();
		properties.setProperty(Constants.ENGINE, "router");
		properties.setProperty(Constants.ENGINE_ALIAS, "test-router");
		router.open(properties);
		return router;
	}
}
