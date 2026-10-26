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
package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

class GenerateInputValuesReactorUnitTests {

	private GenerateInputValuesReactor reactor;
	private Map<String, String> keyValues;
	private NounStore nounStore;
	private Insight insight;

	@BeforeEach
	void setUp() {
		reactor = new GenerateInputValuesReactor();
		keyValues = reactor.keyValue;
		nounStore = new NounStore("test");
		insight = mock(Insight.class);

		reactor.setNounStore(nounStore);
		reactor.setInsight(insight);
	}

	@Test
	void executeGeneratesInputsFromHistoryAndModelResponse() {
		String engineId = "engine-123";
		String sessionId = "session-456";
		String historyRoomId = "history-room";

		keyValues.put("engine", engineId);
		keyValues.put("sessionId", sessionId);
		keyValues.put("roomId", historyRoomId);

		Map<String, Object> params = new HashMap<>();
		List<Map<String, Object>> inputs = new ArrayList<>();
		Map<String, Object> usernameInput = new LinkedHashMap<>();
		usernameInput.put("id", "username");
		usernameInput.put("label", "Username");
		inputs.add(usernameInput);
		params.put("inputs", inputs);
		addParamsToStore(params);

		Room historyRoom = mock(Room.class);
		InputMessage historyQuestion = InputMessage.builder(historyRoom).withText("Where should we go?").build();
		ResponseMessage historyAnswer = ResponseMessage.builder().withText("Head to Wonderland").build();
		List<AbstractMessage> historyMessages = List.of(historyQuestion, historyAnswer);
		when(historyRoom.getMessages()).thenReturn(historyMessages);

		IModelEngine modelEngine = mock(IModelEngine.class);
		Room promptRoom = mock(Room.class);
		String modelResponseText = "```json[{\"id\":\"username\",\"value\":\"Alice\"}]```";
		ResponseMessage modelResponse = ResponseMessage.builder().withText(modelResponseText).build();
		when(promptRoom.ask(any(InputMessage.class), eq(modelEngine))).thenReturn(modelResponse);

		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class);
				MockedStatic<RoomUtils> roomUtils = Mockito.mockStatic(RoomUtils.class)) {

			utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
			roomUtils.when(() -> RoomUtils.getOrLoadRoom(historyRoomId, insight)).thenReturn(historyRoom);
			roomUtils.when(() -> RoomUtils.getPagedMessages(anyList(), eq("ASC"), eq(0), eq(-1)))
					.thenReturn(historyMessages);
			roomUtils.when(() -> RoomUtils.createRoomIfNotExists(anyString(), eq(insight), eq(modelEngine), any()))
					.thenReturn(promptRoom);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.MAP, result.getNounType());
			@SuppressWarnings("unchecked")
			Map<String, Object> resultMap = (Map<String, Object>) result.getValue();
			assertTrue((Boolean) resultMap.get("success"));
			assertEquals(sessionId, resultMap.get("sessionId"));
			assertEquals(historyRoomId, resultMap.get("historyRoomId"));
			assertEquals(modelResponseText, resultMap.get("rawResponse"));
			assertEquals("[{\"id\":\"username\",\"value\":\"Alice\"}]", resultMap.get("inputsJson"));

			assertNotNull(resultMap.get("roomId"));
			assertTrue(resultMap.get("roomId") instanceof String);
			assertFalse(((String) resultMap.get("roomId")).isEmpty());
			assertNotEquals(historyRoomId, resultMap.get("roomId"));

			ArgumentCaptor<InputMessage> promptCaptor = ArgumentCaptor.forClass(InputMessage.class);
			verify(promptRoom).ask(promptCaptor.capture(), eq(modelEngine));
			String prompt = promptCaptor.getValue().getFullInputPrompt();
			assertTrue(prompt.contains("CONVERSATION HISTORY"));
			assertTrue(prompt.contains("Where should we go?"));
			assertTrue(prompt.contains("Head to Wonderland"));
			assertTrue(prompt.contains("label: Username"));
		}
	}

	@Test
	void executeReturnsErrorWhenInputsMissing() {
		keyValues.put("engine", "engine-123");
		keyValues.put("sessionId", "session-456");

		addParamsToStore(new HashMap<>());

		NounMetadata result = reactor.execute();

		assertEquals(PixelDataType.MAP, result.getNounType());
		@SuppressWarnings("unchecked")
		Map<String, Object> resultMap = (Map<String, Object>) result.getValue();
		assertFalse((Boolean) resultMap.get("success"));
		assertEquals("inputs are required", resultMap.get("error"));
		assertEquals("", resultMap.get("rawResponse"));
	}

	private void addParamsToStore(Map<String, Object> params) {
		GenRowStruct grs = new GenRowStruct();
		grs.add(new NounMetadata(params, PixelDataType.MAP));
		nounStore.addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), grs);
	}
}
