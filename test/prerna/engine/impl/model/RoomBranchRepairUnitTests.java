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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static prerna.engine.impl.model.RoomBranchFixtures.add;
import static prerna.engine.impl.model.RoomBranchFixtures.call;
import static prerna.engine.impl.model.RoomBranchFixtures.input;
import static prerna.engine.impl.model.RoomBranchFixtures.insight;
import static prerna.engine.impl.model.RoomBranchFixtures.results;
import static prerna.engine.impl.model.RoomBranchFixtures.room;
import static prerna.engine.impl.model.RoomBranchFixtures.storeWithoutDatabase;
import static prerna.engine.impl.model.RoomBranchFixtures.text;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;

/**
 * How a room recovers from a stopped turn, a crash mid-tool or a broken parent
 * link without losing stored messages.
 */
class RoomBranchRepairUnitTests {

	// ---- which part of a branch a provider accepts ----

	@Test
	void aBranchWhoseToolCallsAllHaveResultsIsSendableInFull() {
		Room room = room();
		List<AbstractMessage> branch = List.of(input(room, "q"), call("a"), results(room, "a"), text("done"));

		assertEquals(4, MessageUtils.sendablePrefixLength(branch));
	}

	@Test
	void aBranchEndingInAnUnansweredToolCallIsSendableUpToTheCall() {
		Room room = room();
		List<AbstractMessage> branch = List.of(input(room, "q"), text("a"), input(room, "q2"), call("a"));

		assertEquals(3, MessageUtils.sendablePrefixLength(branch));
	}

	@Test
	void anUnansweredCallInTheMiddleCutsEverythingAfterIt() {
		Room room = room();
		// the turn was stopped mid-tool and the user carried on
		List<AbstractMessage> branch = List.of(input(room, "q"), call("a"), input(room, "q2"), text("a2"));

		assertEquals(1, MessageUtils.sendablePrefixLength(branch));
	}

	@Test
	void aResultWithNoCallEndsTheSendablePart() {
		Room room = room();
		List<AbstractMessage> branch = List.of(input(room, "q"), text("a"), results(room, "ghost"), text("b"));

		assertEquals(2, MessageUtils.sendablePrefixLength(branch));
	}

	@Test
	void oneOfTwoParallelCallsAnsweredIsNotYetSendable() {
		Room room = room();
		List<AbstractMessage> branch = List.of(input(room, "q"), call("a", "b"), results(room, "a"));

		assertEquals(1, MessageUtils.sendablePrefixLength(branch));
	}

	@Test
	void aCallTheProviderRunsItselfNeedsNoResult() {
		Room room = room();
		ResponseMessage search = ResponseMessage.toolResponses(
				List.of(Map.of("id", "srvtoolu_1", "name", "web_search", "arguments", Map.of(), "server_tool", true)));
		List<AbstractMessage> branch = List.of(input(room, "q"), search);

		assertEquals(2, MessageUtils.sendablePrefixLength(branch));
	}

	// ---- where a new message attaches ----

	@Test
	void anEmptyRoomGivesARootMessage() {
		assertNull(RoomMessageStore.resolveInputParent(room(), null));
		assertNull(RoomMessageStore.resolveInputParent(room(), "anything"));
	}

	@Test
	void withNoParentAskedTheNewMessageFollowsTheLastMessage() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);

		assertEquals(a.getMessageId(), RoomMessageStore.resolveInputParent(room, null));
		assertEquals(a.getMessageId(), RoomMessageStore.resolveInputParent(room, " "));
	}

	@Test
	void anAskedParentOnAHealthyBranchIsKept() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);
		add(room, input(room, "q2"), a);

		assertEquals(a.getMessageId(), RoomMessageStore.resolveInputParent(room, a.getMessageId()));
	}

	@Test
	void afterAStoppedToolCallTheNewMessageGoesAfterTheLastSendableMessage() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);
		AbstractMessage q2 = add(room, input(room, "q2"), a);
		AbstractMessage stopped = add(room, call("x"), q2);

		assertEquals(q2.getMessageId(), RoomMessageStore.resolveInputParent(room, stopped.getMessageId()));
		assertEquals(q2.getMessageId(), RoomMessageStore.resolveInputParent(room, null));
	}

	@Test
	void aParentOnTheTailAfterAStoppedCallMovesAboveTheCall() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage stopped = add(room, call("x"), q);
		AbstractMessage q2 = add(room, input(room, "q2"), stopped);
		AbstractMessage a2 = add(room, text("a2"), q2);

		assertEquals(q.getMessageId(), RoomMessageStore.resolveInputParent(room, a2.getMessageId()));
	}

	@Test
	void aBrokenBranchDoesNotChangeAHealthyOneAndNothingIsRemoved() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);
		// an older branch with a stopped tool call, stored before the healthy reply
		AbstractMessage retry = add(room, input(room, "retry"), a);
		add(room, call("x"), retry);
		AbstractMessage q2 = add(room, input(room, "q2"), a);
		AbstractMessage a2 = add(room, text("a2"), q2);
		int stored = room.getMessages().size();

		assertEquals(a2.getMessageId(), RoomMessageStore.resolveInputParent(room, null));
		assertEquals(a2.getMessageId(), RoomMessageStore.resolveInputParent(room, a2.getMessageId()));
		assertEquals(stored, room.getMessages().size());
	}

	@Test
	void aBranchThatStartsWithAnUnpairedResultGivesARootMessage() {
		Room room = room();
		AbstractMessage orphan = add(room, results(room, "ghost"), null);
		AbstractMessage a = add(room, text("a"), orphan);

		assertNull(RoomMessageStore.resolveInputParent(room, a.getMessageId()));
	}

	@Test
	void anAskedParentThatIsNotInTheRoomIsRefused() {
		Room room = room();
		add(room, input(room, "q"), null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> RoomMessageStore.resolveInputParent(room, "not-here"));
		assertTrue(e.getMessage().contains("not-here"));
	}

	@Test
	void theBranchSentToTheModelAfterAStoppedCallIsAccepted() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		add(room, call("x"), q);
		InputMessage next = input(room, "carry on");
		next.setParentMessageId(RoomMessageStore.resolveInputParent(room, null));

		assertDoesNotThrow(() -> RoomMessageStore.messageHistoryWithNewMessage(room, next));
	}

	// ---- turns saved through the room ----

	@Test
	void aSavedTurnAfterAStoppedCallKeepsTheStoppedMessagesAndIsSendable() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage stopped = add(room, call("x"), q);
		IModelEngine engine = mock(IModelEngine.class);
		when(engine.keepInputOutput()).thenReturn(true);
		when(engine.keepsConversationHistory()).thenReturn(true);
		InputMessage next = input(room, "carry on");

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			ResponseMessage reply = room.commitPrebuiltTurn(next, engine, stopped.getMessageId(),
					List.of(Map.of("type", "TEXT", "text", "ok")), null, new ArrayList<>());

			assertEquals(q.getMessageId(), next.getParentMessageId());
			assertEquals(next.getMessageId(), reply.getParentMessageId());
			assertTrue(room.getMessages().contains(stopped));
			assertEquals(4, room.getMessages().size());
			assertDoesNotThrow(() -> RoomMessageStore.currentMessageHistory(room));
		}
	}

	@Test
	void aSavedTurnOnAnUnknownParentIsRefusedAndNothingIsAdded() {
		Room room = room();
		add(room, input(room, "q"), null);
		IModelEngine engine = mock(IModelEngine.class);
		when(engine.keepInputOutput()).thenReturn(true);
		when(engine.keepsConversationHistory()).thenReturn(true);

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			assertThrows(IllegalArgumentException.class, () -> room.commitPrebuiltTurn(input(room, "next"), engine,
					"not-here", List.of(Map.of("type", "TEXT", "text", "ok")), null, new ArrayList<>()));
			assertEquals(1, room.getMessages().size());
		}
	}

	// ---- broken parent links on load ----

	@Test
	void aMessageWhoseParentIsMissingBecomesARootWhenLoaded() {
		Room room = room();
		List<AbstractMessage> loaded = RoomMessageStore.loadFromPersistedJson(room,
				"[{\"io\":\"INPUT\",\"messageId\":\"root\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"child\",\"parentMessageId\":\"missing\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"grandchild\",\"parentMessageId\":\"child\",\"parts\":[]}]");

		assertNull(loaded.get(1).getParentMessageId());
		assertEquals("missing", loaded.get(1).getOrnament(RoomMessageStore.DETACHED_PARENT_ORNAMENT));
		// links that were fine stay as they were
		assertNull(loaded.get(0).getOrnament(RoomMessageStore.DETACHED_PARENT_ORNAMENT));
		assertEquals("child", loaded.get(2).getParentMessageId());
	}

	@Test
	void aMessageThatIsItsOwnParentBecomesARootWhenLoaded() {
		List<AbstractMessage> loaded = RoomMessageStore.loadFromPersistedJson(room(),
				"[{\"io\":\"INPUT\",\"messageId\":\"self\",\"parentMessageId\":\"self\",\"parts\":[]}]");

		assertNull(loaded.get(0).getParentMessageId());
		assertEquals("self", loaded.get(0).getOrnament(RoomMessageStore.DETACHED_PARENT_ORNAMENT));
	}

	@Test
	void aParentCycleIsBrokenOnceWhenLoaded() {
		List<AbstractMessage> loaded = RoomMessageStore.loadFromPersistedJson(room(),
				"[{\"io\":\"INPUT\",\"messageId\":\"a\",\"parentMessageId\":\"c\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"b\",\"parentMessageId\":\"a\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"c\",\"parentMessageId\":\"b\",\"parts\":[]}]");

		long roots = loaded.stream().filter(message -> message.getParentMessageId() == null).count();
		assertEquals(1, roots);
		// the three now form one branch from the new root
		assertEquals(3, MessageUtils.getMessageBranchFromParent(loaded, leafOf(loaded)).size());
	}

	@Test
	void aRoomLoadedWithBrokenLinksCanBeSentAndValidatedAgain() {
		Room room = room();
		room.setMessages(RoomMessageStore.loadFromPersistedJson(room,
				"[{\"io\":\"INPUT\",\"messageId\":\"child\",\"parentMessageId\":\"missing\",\"parts\":[]}]"));

		assertDoesNotThrow(() -> RoomMessageStore.providerMessageHistory(room, room.getMessages()));
	}

	@Test
	void aRoomWithHealthyLinksLoadsUnchanged() {
		List<AbstractMessage> loaded = RoomMessageStore.loadFromPersistedJson(room(),
				"[{\"io\":\"INPUT\",\"messageId\":\"a\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"b\",\"parentMessageId\":\"a\",\"parts\":[]}]");

		assertEquals("a", loaded.get(1).getParentMessageId());
		assertNull(loaded.get(1).getOrnament(RoomMessageStore.DETACHED_PARENT_ORNAMENT));
	}

	@Test
	void twoMessagesWithOneIdStillStopTheRoomLoading() {
		assertThrows(IllegalStateException.class,
				() -> RoomMessageStore.loadFromPersistedJson(room(), "[{\"io\":\"INPUT\",\"messageId\":\"a\",\"parts\":[]},"
						+ "{\"io\":\"INPUT\",\"messageId\":\"a\",\"parts\":[]}]"));
	}

	// ---- walking parent links ----

	@Test
	void walkingABranchStopsAtAParentCycleMadeInMemory() {
		Room room = room();
		AbstractMessage a = add(room, input(room, "a"), null);
		AbstractMessage b = add(room, text("b"), a);
		a.setParentMessageId(b.getMessageId());

		assertEquals(2, MessageUtils.getMessageBranchFromParent(room.getMessages(), b.getMessageId()).size());
		assertEquals(3, MessageUtils.getMessageBranchWithNewMessage(room.getMessages(), withParent(room, b)).size());
	}

	// ---- compaction's write ----

	@Test
	void aCompactionIsAddedToTheLatestMessagesAndKeepsAWriteMadeMeanwhile() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);
		InputMessage compacted = input(room, "Summarizing Conversation");
		ResponseMessage summary = text("summary");
		summary.setParentMessageId(compacted.getMessageId());
		AbstractMessage[] meanwhile = new AbstractMessage[1];

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			// another request saved a message while the summary was written
			store.when(() -> RoomMessageStore.refreshFromLatestProjection(any(), any())).thenAnswer(call -> {
				if (meanwhile[0] == null) {
					meanwhile[0] = add(room, input(room, "sent meanwhile"), a);
				}
				return null;
			});
			room.appendCompaction(a.getMessageId(), false, compacted, summary, insight());
		}

		assertTrue(room.getMessages().contains(meanwhile[0]));
		assertSame(compacted, room.getMessages().get(room.getMessages().size() - 2));
		assertSame(summary, room.getMessages().getLast());
		assertFalse(a.getPruneToolsAbove());
	}

	@Test
	void aToolPruneCompactionFlagsTheFreshCopyOfItsAnchor() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);
		InputMessage prune = input(room, "Pruning Tools For Future Messages");
		prune.setParentMessageId(a.getMessageId());
		ResponseMessage ack = text("ack");
		ack.setParentMessageId(prune.getMessageId());

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			room.appendCompaction(a.getMessageId(), true, prune, ack, insight());
		}

		assertTrue(a.getPruneToolsAbove());
		assertEquals(4, room.getMessages().size());
	}

	@Test
	void aCompactionWhoseAnchorIsGoneAddsNothing() {
		Room room = room();
		add(room, input(room, "q"), null);

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			assertThrows(IllegalStateException.class,
					() -> room.appendCompaction("gone", true, input(room, "x"), text("y"), insight()));
		}
		assertEquals(1, room.getMessages().size());
	}

	@Test
	void aCompactionThatCannotBeSavedIsRolledBack() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			store.when(() -> RoomMessageStore.persist(any(Room.class), anyString())).thenReturn(false);
			assertThrows(IllegalStateException.class,
					() -> room.appendCompaction(a.getMessageId(), true, input(room, "x"), text("y"), insight()));
		}
		assertEquals(2, room.getMessages().size());
		assertFalse(a.getPruneToolsAbove());
	}

	@Test
	void aCompactionWhoseSaveFailsIsRolledBack() {
		Room room = room();
		AbstractMessage q = add(room, input(room, "q"), null);
		AbstractMessage a = add(room, text("a"), q);

		try (MockedStatic<RoomMessageStore> store = storeWithoutDatabase()) {
			store.when(() -> RoomMessageStore.persist(any(Room.class), anyString()))
					.thenThrow(new IllegalStateException("Room is busy"));
			assertThrows(IllegalStateException.class,
					() -> room.appendCompaction(a.getMessageId(), true, input(room, "x"), text("y"), insight()));
		}
		assertEquals(2, room.getMessages().size());
		assertFalse(a.getPruneToolsAbove());
	}

	// ---- fixtures ----

	private static InputMessage withParent(Room room, AbstractMessage parent) {
		InputMessage next = input(room, "next");
		next.setParentMessageId(parent.getMessageId());
		return next;
	}

	/** The one message no other message names as its parent. */
	private static String leafOf(List<AbstractMessage> messages) {
		for (AbstractMessage candidate : messages) {
			if (messages.stream().noneMatch(m -> candidate.getMessageId().equals(m.getParentMessageId()))) {
				return candidate.getMessageId();
			}
		}
		throw new AssertionError("no leaf");
	}
}
