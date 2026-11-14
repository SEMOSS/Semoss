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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomBranchFixtures;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/** Compaction keeps the room sendable, writes on its latest messages and stores nothing else. */
class CompactRoomMessagesReactorUnitTests {

	private static final String MODEL_ID = "model-1";

	private MockedStatic<RoomMessageStore> store;
	private MockedStatic<RoomUtils> roomUtils;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	private MockedStatic<SecurityEngineUtils> security;
	private MockedStatic<Utility> utility;

	private Insight insight;
	private Room room;
	private IModelEngine engine;
	private List<Room> askedRooms;
	private List<String> prompts;

	@BeforeEach
	void setUp() {
		insight = mock(Insight.class);
		User user = mock(User.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
		when(user.getPrimaryLoginToken().getId()).thenReturn("compact-user");
		when(insight.getUser()).thenReturn(user);

		room = new Room();
		room.setId("compact-room");
		room.setMessages(new ArrayList<>());
		room.setInsight(insight);
		room.setOptionsMap(new HashMap<>(Map.of("modelId", MODEL_ID)));

		engine = mock(IModelEngine.class);
		askedRooms = new ArrayList<>();
		prompts = new ArrayList<>();
		when(engine.askRoom(any(), any(), anyMap())).thenAnswer(call -> {
			InputMessage message = call.getArgument(0);
			askedRooms.add(call.getArgument(1));
			prompts.add(message.getInputText());
			return new AskStringModelEngineResponse("The user is building a report.", 10, 20);
		});

		store = RoomBranchFixtures.storeWithoutDatabase();
		roomUtils = mockStatic(RoomUtils.class);
		roomUtils.when(() -> RoomUtils.getOrLoadRoom(anyString(), any())).thenReturn(room);
		logs = mockStatic(ModelInferenceLogsUtils.class);
		security = mockStatic(SecurityEngineUtils.class);
		security.when(() -> SecurityEngineUtils.userCanViewEngine(any(User.class), anyString())).thenReturn(true);
		utility = mockStatic(Utility.class);
		utility.when(() -> Utility.getModel(MODEL_ID)).thenReturn(engine);
	}

	@AfterEach
	void tearDown() {
		utility.close();
		security.close();
		logs.close();
		roomUtils.close();
		store.close();
	}

	// ---- what can be compacted ----

	@Test
	void aParentThatIsNotInTheRoomIsRefused() {
		turns(3);
		int stored = room.getMessages().size();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> run("not-here", "SUMMARY"));
		assertTrue(e.getMessage().contains("not-here"));
		assertEquals(stored, room.getMessages().size());
	}

	@Test
	void anInputMessageCannotBeCompacted() {
		AbstractMessage q = add(input("q"), null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> run(q.getMessageId(), "TOOL_PRUNE"));
		assertTrue(e.getMessage().contains("input message"));
	}

	@Test
	void anUnansweredToolCallCannotBeCompacted() {
		AbstractMessage q = add(input("q"), null);
		AbstractMessage call = add(call("x"), q);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> run(call.getMessageId(), "TOOL_PRUNE"));
		assertTrue(e.getMessage().contains("unanswered tool calls"));
	}

	// ---- tool pruning ----

	@Test
	void toolPruningAddsAPairUnderTheLeafAndFlagsIt() {
		AbstractMessage q = add(input("q"), null);
		AbstractMessage call = add(call("a"), q);
		AbstractMessage results = add(results("a"), call);
		AbstractMessage answer = add(text("done"), results);

		Map<String, Object> result = only(run(answer.getMessageId(), "TOOL_PRUNE"));

		assertEquals(true, result.get("success"));
		assertEquals(6, room.getMessages().size());
		AbstractMessage pruneInput = room.getMessages().get(4);
		assertEquals(answer.getMessageId(), pruneInput.getParentMessageId());
		assertEquals(pruneInput.getMessageId(), room.getMessages().get(5).getParentMessageId());
		assertTrue(answer.getPruneToolsAbove());
	}

	@Test
	void toolPruningRefusesABranchWithAStoppedToolCallAndChangesNothing() {
		AbstractMessage q = add(input("q"), null);
		AbstractMessage stopped = add(call("x"), q);
		AbstractMessage q2 = add(input("carry on"), stopped);
		AbstractMessage answer = add(text("ok"), q2);

		Map<String, Object> result = only(run(answer.getMessageId(), "TOOL_PRUNE"));

		assertEquals(false, result.get("success"));
		assertTrue(String.valueOf(result.get("error")).contains("SUMMARY"));
		assertEquals(4, room.getMessages().size());
		assertFalse(answer.getPruneToolsAbove());
		store.verify(() -> RoomMessageStore.persist(any(Room.class), anyString()), never());
	}

	@Test
	void autoDetectSummarizesABranchToolPruningCannotFix() {
		// heavy tool use, which would pick tool pruning on a healthy branch
		AbstractMessage q0 = add(input("question 0"), null);
		q0.setTokensInMessage(100);
		AbstractMessage call = add(call("a"), q0);
		AbstractMessage results = add(results("a"), call);
		results.setTokensInMessage(50_000);
		AbstractMessage answer0 = add(text("answer 0"), results);
		// then a stopped tool call the user carried on past
		AbstractMessage q1 = add(input("question 1"), answer0);
		AbstractMessage stopped = add(call("x"), q1);
		AbstractMessage q2 = add(input("question 2"), stopped);
		AbstractMessage answer2 = add(text("answer 2"), q2);
		AbstractMessage q3 = add(input("question 3"), answer2);
		q3.setTokensInMessage(60_000);
		AbstractMessage answer3 = add(text("answer 3"), q3);
		when(engine.getContextWindow()).thenReturn(200_000);

		Map<String, Object> result = only(run(answer3.getMessageId(), null));

		assertEquals("SUMMARY", result.get("type"));
		assertEquals(true, result.get("success"));
		assertFalse(answer3.getPruneToolsAbove());
	}

	// ---- summaries ----

	@Test
	void theSummaryIsAskedOnAnInMemoryRoomAndNoOtherRoomIsStored() {
		AbstractMessage leaf = turns(3);

		Map<String, Object> result = only(run(leaf.getMessageId(), "SUMMARY"));

		assertEquals(true, result.get("success"));
		assertEquals(1, askedRooms.size());
		Room asked = askedRooms.get(0);
		assertNotSame(room, asked);
		assertEquals(room.getId(), asked.getId());
		assertTrue(asked.getMessages().isEmpty());
		roomUtils.verify(() -> RoomUtils.createRoomIfNotExists(any(), any(), any(), any()), never());
	}

	@Test
	void theSummaryStartsANewBranchAfterTheMessagesItSummarizes() {
		AbstractMessage leaf = turns(3);
		int stored = room.getMessages().size();

		only(run(leaf.getMessageId(), "SUMMARY"));

		assertEquals(stored + 2, room.getMessages().size());
		InputMessage compacted = (InputMessage) room.getMessages().get(stored);
		ResponseMessage summary = (ResponseMessage) room.getMessages().get(stored + 1);
		assertNull(compacted.getParentMessageId());
		assertEquals(leaf.getMessageId(), compacted.getSummaryLeafMessageId());
		assertEquals(compacted.getMessageId(), summary.getParentMessageId());
		assertTrue(summary.getContent().contains("The user is building a report."));
		// nothing the summary covers was removed
		assertSame(leaf, room.getMessages().get(stored - 1));
	}

	@Test
	void largeToolOutputIsCutInTheSummaryRequestAndInTheKeptMessages() {
		AbstractMessage q = add(input("read the log"), null);
		AbstractMessage call = add(call("big"), q);
		InputMessage results = InputMessage.builder(room)
				.withToolResult("big", "ReadFile", "x".repeat(50_000), null, "success", false).build();
		add(results, call);
		AbstractMessage leaf = add(text("read it"), results);
		for (int i = 0; i < 3; i++) {
			AbstractMessage next = add(input("q" + i), leaf);
			leaf = add(text("a" + i), next);
		}
		int stored = room.getMessages().size();

		only(run(leaf.getMessageId(), "SUMMARY"));

		String prompt = prompts.get(0);
		assertFalse(prompt.contains("x".repeat(CompactRoomMessagesReactor.MAX_TOOL_TEXT_CHARS + 1)));
		assertTrue(prompt.contains("more characters left out"));
		ResponseMessage summary = (ResponseMessage) room.getMessages().get(stored + 1);
		assertTrue(summary.getContent().length() < 20_000);
	}

	@Test
	void theSummaryRequestFitsASmallContextWindow() {
		AbstractMessage leaf = null;
		for (int i = 0; i < 40; i++) {
			AbstractMessage q = add(input("question " + i + " " + "words ".repeat(500)), leaf);
			leaf = add(text("answer " + i + " " + "words ".repeat(500)), q);
		}
		when(engine.getContextWindow()).thenReturn(16_000);

		only(run(leaf.getMessageId(), "SUMMARY"));

		// chars / 4 approximates tokens; the request leaves room for the reply
		assertTrue(prompts.get(0).length() / 4 < 16_000, "prompt ~" + prompts.get(0).length() / 4 + " tokens");
		assertTrue(prompts.get(0).contains("left out to fit the model's context"));
		// the goal at the start of the conversation is kept
		assertTrue(prompts.get(0).contains("question 0 "));
	}

	@Test
	void aSummaryThatCannotBeSavedLeavesTheRoomAsItWas() {
		AbstractMessage leaf = turns(3);
		int stored = room.getMessages().size();
		store.when(() -> RoomMessageStore.persist(any(Room.class), anyString()))
				.thenThrow(new IllegalStateException("Room is busy"));

		Map<String, Object> result = only(run(leaf.getMessageId(), "SUMMARY"));

		assertEquals(false, result.get("success"));
		assertEquals(stored, room.getMessages().size());
	}

	@Test
	void aSummaryWhoseBranchWasRemovedMeanwhileAddsNothing() {
		AbstractMessage leaf = turns(3);
		int stored = room.getMessages().size();
		// another request replaced the room's messages while the summary was written
		store.when(() -> RoomMessageStore.refreshFromLatestProjection(any(), any())).thenAnswer(call -> {
			room.getMessages().removeIf(message -> message == leaf);
			return null;
		});

		Map<String, Object> result = only(run(leaf.getMessageId(), "SUMMARY"));

		assertEquals(false, result.get("success"));
		assertTrue(String.valueOf(result.get("error")).contains("no longer in room"));
		assertEquals(stored - 1, room.getMessages().size());
	}

	// ---- transcript helpers ----

	@Test
	void aShortTranscriptIsLeftAlone() {
		assertEquals("User: hi", CompactRoomMessagesReactor.fitTranscript("User: hi", 100));
	}

	@Test
	void aLongTranscriptKeepsItsStartAndEndWithinTheBudget() {
		String transcript = "START" + "m".repeat(100_000) + "END";

		String fitted = CompactRoomMessagesReactor.fitTranscript(transcript, 10_000);

		assertTrue(fitted.length() <= 10_000);
		assertTrue(fitted.startsWith("START"));
		assertTrue(fitted.endsWith("END"));
		assertTrue(fitted.contains("left out to fit the model's context"));
	}

	@Test
	void anUnknownWindowPutsNoLimitOnTheTranscript() {
		when(engine.getContextWindow()).thenThrow(new IllegalStateException("unknown"));

		assertEquals(Integer.MAX_VALUE, CompactRoomMessagesReactor.summaryTranscriptCharBudget(engine, 2_000));
		assertEquals(Integer.MAX_VALUE, CompactRoomMessagesReactor.keptTranscriptCharBudget(engine));
	}

	// ---- fixtures ----

	private List<Map<String, Object>> run(String parentMessageId, String type) {
		CompactRoomMessagesReactor reactor = new CompactRoomMessagesReactor();
		reactor.In();
		reactor.setInsight(insight);
		addInput(reactor, ReactorKeysEnum.ROOM_ID.getKey(), room.getId());
		addInput(reactor, ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), parentMessageId);
		if (type != null) {
			addInput(reactor, "compactionTypes", type);
		}
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> results = (List<Map<String, Object>>) reactor.execute().getValue();
		return results;
	}

	private static void addInput(CompactRoomMessagesReactor reactor, String key, String value) {
		GenRowStruct row = new GenRowStruct();
		row.add(new NounMetadata(value, PixelDataType.CONST_STRING));
		reactor.getNounStore().addNoun(key, row);
	}

	private static Map<String, Object> only(List<Map<String, Object>> results) {
		assertEquals(1, results.size(), String.valueOf(results));
		return results.get(0);
	}

	/** {@code count} question and answer turns, the first with a tool call; returns the last answer. */
	private AbstractMessage turns(int count) {
		AbstractMessage q = add(input("question 0"), null);
		AbstractMessage call = add(call("t0"), q);
		AbstractMessage results = add(results("t0"), call);
		AbstractMessage leaf = add(text("answer 0"), results);
		for (int i = 1; i < count; i++) {
			AbstractMessage next = add(input("question " + i), leaf);
			leaf = add(text("answer " + i), next);
		}
		return leaf;
	}

	private InputMessage input(String text) {
		return RoomBranchFixtures.input(room, text);
	}

	private static ResponseMessage text(String text) {
		return RoomBranchFixtures.text(text);
	}

	private static ResponseMessage call(String id) {
		return RoomBranchFixtures.call(id);
	}

	private InputMessage results(String id) {
		return RoomBranchFixtures.results(room, id);
	}

	private <T extends AbstractMessage> T add(T message, AbstractMessage parent) {
		return RoomBranchFixtures.add(room, message, parent);
	}
}
