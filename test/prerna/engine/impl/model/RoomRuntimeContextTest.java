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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.MessagePartType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.TextMessagePart;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.om.Insight;

class RoomRuntimeContextTest {

	private static final String SYSTEM = "Stable agent instructions. Use the latest SEMOSS runtime status.";

	private Room room() {
		var room = spy(new Room());
		room.setId("runtime-context-test");
		room.setOptionsMap(Map.of("instructions", SYSTEM));
		room.setMessages(new ArrayList<>());
		doReturn(List.of()).when(room).getAllToolsJsonForRoom(anyInt(), anyBoolean());
		return room;
	}

	private Insight insight() {
		var insight = mock(Insight.class);
		var user = mock(User.class, RETURNS_DEEP_STUBS);
		when(user.getPrimaryLoginToken().getId()).thenReturn("fixture-user");
		when(insight.getUser()).thenReturn(user);
		return insight;
	}

	private ResponseMessage calls(Room room, String... ids) {
		List<Map<String, Object>> calls = new ArrayList<>();
		for (String id : ids) {
			calls.add(Map.of("id", id, "name", "ReadFile", "arguments", Map.of("path", id + ".txt")));
		}
		var call = ResponseMessage.toolResponses(calls);
		call.setRoom(room);
		if (!room.getMessages().isEmpty()) {
			call.setParentMessageId(room.getMessages().getLast().getMessageId());
		}
		room.getMessages().add(call);
		return call;
	}

	private InputMessage results(Room room, ResponseMessage call, String... ids) {
		var builder = InputMessage.builder(room).withSystemPrompt(SYSTEM);
		for (String id : ids) {
			builder.withToolResult(id, "ReadFile", "output-" + id, Map.of("path", id + ".txt"), "success", false);
		}
		var input = builder.build();
		input.setParentMessageId(call.getMessageId());
		input.setVisible(false);
		room.getMessages().add(input);
		return input;
	}

	@Test
	void successiveRequestsKeepPriorPartsAndAppendStatusAfterAllToolResults() throws Exception {
		var room = room();
		var insight = insight();
		var engine = mock(IModelEngine.class);
		List<JSONArray> requests = new ArrayList<>();
		when(engine.askRoom(any(), eq(room), anyMap())).thenAnswer(call -> {
			requests.add(new JSONArray((String) ((Map<?, ?>) call.getArgument(2)).get("message_json")));
			return new AskStringModelEngineResponse("Next step", 1, 1);
		});
		var initial = InputMessage.builder(room).withSystemPrompt(SYSTEM)
				.withText("Create slides.\n\n[SEMOSS runtime status]\n40 remaining\n[/SEMOSS runtime status]",
						"Create slides.")
				.build();
		room.getMessages().add(initial);
		var firstCall = calls(room, "a", "b");
		var first = results(room, firstCall, "a", "b");
		try (var store = mockStatic(RoomMessageStore.class); var media = mockStatic(RoomUtils.class)) {
			store.when(() -> RoomMessageStore.currentMessageHistory(room)).thenCallRealMethod();
			store.when(() -> RoomMessageStore.providerContext(org.mockito.ArgumentMatchers.anyList())).thenCallRealMethod();
			room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), firstCall.getMessageId(), engine,
					insight, SYSTEM, "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]");
			String priorParts = new JSONArray(requests.getFirst().getJSONObject(requests.getFirst().length() - 1)
					.getJSONArray("parts").toString()).toString();
			var secondCall = calls(room, "c");
			var second = results(room, secondCall, "c");
			room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), secondCall.getMessageId(), engine,
					insight, SYSTEM,
					"[SEMOSS runtime status]\n38 remaining; repair rounds remaining: 3\n[/SEMOSS runtime status]");
			assertEquals(SYSTEM, first.getSystemPrompt());
			assertEquals(SYSTEM, second.getSystemPrompt());
			assertEquals(List.of(MessagePartType.SYSTEM, MessagePartType.TOOL_RESULT, MessagePartType.TOOL_RESULT,
					MessagePartType.TEXT), first.getParts().stream().map(MessagePart::getType).toList());
			assertEquals("Create slides.", initial.getInputUIPrompt());
			assertTrue(
					((TextMessagePart) second.getParts().getLast()).getText().contains("repair rounds remaining: 3"));
			assertEquals(priorParts,
					requests.get(1).getJSONObject(requests.getFirst().length() - 1).getJSONArray("parts").toString());
			for (int i = 0; i < requests.getFirst().length(); i++) {
				assertEquals(requests.getFirst().getJSONObject(i).getJSONArray("parts").toString(),
						requests.get(1).getJSONObject(i).getJSONArray("parts").toString());
			}
		}
		// Optional bridge fixture: the Python tests consume the actual Java
		// provider-history serialization.
		String output = System.getProperty("semoss.runtimeContextFixture");
		if (output != null) {
			Files.writeString(Path.of(output), new JSONObject().put("requests", new JSONArray(requests)).toString(2));
		}
	}

	@Test
	void focusedEditOmitsHistoricalCodeButKeepsStoredChatAndCompleteToolPairs() {
		var room = room();
		var old = InputMessage.builder(room).withText("Original brief").build();
		room.getMessages().add(old);
		var oldReply = ResponseMessage.text("OLD_GENERATOR_CODE"); oldReply.setRoom(room);
		oldReply.setParentMessageId(old.getMessageId()); room.getMessages().add(oldReply);
		var edit = InputMessage.builder(room).withText("Edit slide 3. Original/recent user requests included here.").build();
		edit.setParentMessageId(oldReply.getMessageId());
		edit.setOrnament(RoomMessageStore.PPTX_EDIT_CONTEXT_START, true);
		String first = RoomMessageStore.messageHistoryWithNewMessage(room, edit);
		assertFalse(first.contains("OLD_GENERATOR_CODE"));
		assertTrue(first.contains("Edit slide 3"));
		assertEquals(2, room.getMessages().size()); // Provider inspection did not delete any history.
		room.getMessages().add(edit);
		var call = calls(room, "inspect-slide"); results(room, call, "inspect-slide");
		String continuation = RoomMessageStore.currentMessageHistory(room);
		assertFalse(continuation.contains("OLD_GENERATOR_CODE"));
		assertTrue(continuation.contains("TOOL_CALL") && continuation.contains("TOOL_RESULT"));
		assertEquals(5, room.getMessages().size());
		assertEquals(oldReply.getMessageId(), edit.getParentMessageId());
		String persisted = prerna.engine.impl.model.message.MessageUtils.toJsonArray(room.getMessages());
		room.setMessages(prerna.engine.impl.model.message.MessageUtils.fromJsonArrayPreservingToolState(persisted, room));
		assertFalse(RoomMessageStore.currentMessageHistory(room).contains("OLD_GENERATOR_CODE"));
		var ordinary = InputMessage.builder(room).withText("A subsequent ordinary request").build();
		ordinary.setParentMessageId(room.getMessages().getLast().getMessageId());
		assertTrue(RoomMessageStore.messageHistoryWithNewMessage(room, ordinary).contains("OLD_GENERATOR_CODE"));
	}

	@Test
	void pendingParallelBatchDoesNotSendStatusOrCallModel() {
		var room = room();
		var engine = mock(IModelEngine.class);
		var call = calls(room, "a", "b");
		var input = results(room, call, "a");
		try (var store = mockStatic(RoomMessageStore.class)) {
			assertNull(room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(), call.getMessageId(),
					engine, insight(), SYSTEM, "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]"));
			assertFalse(input.hasTextPart());
			verify(engine, never()).askRoom(any(), any(), any());
		}
	}

	@Test
	void retryOfPendingContinuationDoesNotDuplicateOrRewriteStatus() {
		var room = room();
		var engine = mock(IModelEngine.class);
		var call = calls(room, "a");
		var input = results(room, call, "a");
		when(engine.askRoom(any(), any(), anyMap())).thenThrow(new IllegalStateException("offline provider failure"));
		String note = "[SEMOSS runtime status]\n39 remaining\n[/SEMOSS runtime status]";
		try (var store = mockStatic(RoomMessageStore.class)) {
			store.when(() -> RoomMessageStore.currentMessageHistory(room)).thenCallRealMethod();
			for (int i = 0; i < 2; i++) {
				assertThrows(IllegalStateException.class,
						() -> room.continueAfterToolExecutionResultsWithRuntimeContext(new HashMap<>(),
								call.getMessageId(), engine, insight(), SYSTEM, note));
			}
			assertEquals(1, input.getParts().stream().filter(p -> p instanceof TextMessagePart).count());
			assertEquals(SYSTEM, input.getSystemPrompt());
			assertEquals(note, ((TextMessagePart) input.getParts().getLast()).getText());
		}
	}
}
