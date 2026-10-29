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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;
import prerna.collaboration.BrainMemoryUtils.Source;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.collaboration.BrainCompleteGoalReactor;
import prerna.reactor.collaboration.BrainEditTopicReactor;
import prerna.sablecc2.om.NounStore;
import prerna.util.Utility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/** Brain memory against an in-memory H2 Collaboration schema: migration, owner and assistant writes, topics. */
class BrainMemoryDbUnitTests {

	private static final Source CHAT = new Source(BrainMemoryUtils.FROM_CHAT, "th-1", "room-1", "msg-1", null, null);
	private static final Map<String, Object> ABOUT_PRIYA = Map.of("type", "person", "id", "p-priya");

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_memory_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					columns.add(column.getValue0() + " " + column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("ownerid");
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		person("p-priya", "Priya Shah", false);
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	// ---- migration ----

	@Test
	void migrationMovesNotesAndFactsOnceAndLeavesGoals() throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, KIND, TEXT, STATE, ORIGIN, "
				+ "CREATED_AT) VALUES (?, ?, 'n1', 't-acme', 'note', 'Needs three quotes', 'confirmed', 'you', "
				+ "CURRENT_TIMESTAMP)", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, KIND, TEXT, STATE, ORIGIN, "
				+ "CREATED_AT) VALUES (?, ?, 'n2', 't-acme', 'note', 'Maybe renews in March', 'draft', 'you', "
				+ "CURRENT_TIMESTAMP)", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, KIND, TEXT, STATE, ORIGIN, "
				+ "CREATED_AT) VALUES (?, ?, 'g1', 't-acme', 'goal', 'Close by Q4', 'open', 'you', "
				+ "CURRENT_TIMESTAMP)", ownerId, ownerType);
		sql("INSERT INTO WORK_THREAD_FACT (OWNER_ID, OWNER_TYPE, FACT_ID, THREAD_ID, TEXT, FROM_LABEL, STATUS, "
				+ "SOURCE_PERSON_ID, CREATED_AT) VALUES (?, ?, 'f1', 'th-1', 'Vendor agreed to 10% off', 'Priya', "
				+ "'confirmed', 'p-priya', CURRENT_TIMESTAMP)", ownerId, ownerType);

		BrainMemoryMigration.run();
		BrainMemoryMigration.run();

		assertEquals(3, count("BRAIN_MEMORY"));
		assertEquals(3, count("BRAIN_MEMORY_LINK"));
		assertEquals(1, count("BRAIN_TOPIC_NOTE"));
		assertEquals(0, count("WORK_THREAD_FACT"));
		List<Memory> memories = BrainMemoryUtils.load(ownerId, ownerType, BrainMemoryUtils.STATES);
		assertEquals(Set.of("Needs three quotes:active", "Maybe renews in March:suggested",
				"Vendor agreed to 10% off:active"),
				Set.copyOf(memories.stream().map(m -> m.text() + ":" + m.state()).toList()));
		Memory fact = memories.stream().filter(m -> m.text().startsWith("Vendor")).findFirst().orElseThrow();
		assertEquals(List.of(new Ref(BrainMemoryUtils.THREAD, "th-1")), fact.about());
		assertEquals("p-priya", fact.source().personId());
	}

	@Test
	void schemaUpgradeAddsGoalResolutionWithoutChangingExistingGoals() throws Exception {
		topic("t-acme", "Acme");
		Map<String, Object> goal = BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				"Close by Q4", "open");
		sql("ALTER TABLE BRAIN_TOPIC_NOTE DROP COLUMN RESOLUTION");
		IRDBMSEngine engine = SystemEngineRegistry.getCollaborationDb();
		var goalSchema = new CollaborationOwlCreator(engine.getQueryUtil()).getDBSchema().stream()
				.filter(table -> "BRAIN_TOPIC_NOTE".equals(table.getValue0())).toList();
		AbstractOwlCreator.syncSchema(engine, connection, goalSchema);
		AbstractOwlCreator.syncSchema(engine, connection, goalSchema);
		assertEquals(List.of(goal), BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme"));

		BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, null, null, null,
				(String) goal.get("noteId"), "Signed the renewal");
		Map<String, Object> completed = BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme").get(0);
		assertEquals("done", completed.get("status"));
		assertEquals("Signed the renewal", completed.get("resolution"));
		assertEquals(goal.get("text"), completed.get("text"));
		assertEquals(goal.get("date"), completed.get("date"));
		assertEquals(1, count("BRAIN_TOPIC_NOTE"));
	}

	// ---- owner ----

	@Test
	void ownerSavesEditsDeletesAndUndoesTheDelete() throws Exception {
		Map<String, Object> saved = BrainMemoryUtils.saveMemory(user,
				Map.of("kind", "preference", "text", "Sign emails as Rob"));
		String id = (String) saved.get("id");
		assertEquals("you", saved.get("origin"));
		assertEquals(true, saved.get("confirmed"));
		assertEquals(BrainMemoryUtils.ACTIVE, saved.get("state"));

		Map<String, Object> edited = BrainMemoryUtils.saveMemory(user,
				Map.of("id", id, "text", "Sign emails as Rob W.", "about", List.of(ABOUT_PRIYA)));
		assertEquals("Sign emails as Rob W.", edited.get("text"));
		assertEquals(1, ((List<?>) edited.get("about")).size());

		BrainMemoryUtils.deleteMemory(user, id);
		assertEquals(0, count("BRAIN_MEMORY"));
		assertEquals(0, count("BRAIN_MEMORY_LINK"));

		// the session Undo puts it back under the same id
		Map<String, Object> restored = BrainMemoryUtils.saveMemory(user, Map.of("id", id, "kind", "preference",
				"text", "Sign emails as Rob W.", "about", List.of(ABOUT_PRIYA)));
		assertEquals(id, restored.get("id"));
		assertEquals(1, count("BRAIN_MEMORY_LINK"));
		assertThrows(IllegalArgumentException.class,
				() -> BrainMemoryUtils.saveMemory(user, Map.of("text", "x", "about", List.of(Map.of("type",
						"person", "id", "nobody")))));
	}

	@Test
	void editingASuggestionAcceptsIt() throws Exception {
		Memory suggestion = suggestion("m-sugg", "Dana prefers calls for urgent issues", null);
		Map<String, Object> saved = BrainMemoryUtils.saveMemory(user,
				Map.of("id", suggestion.id(), "text", "Dana prefers a call for anything urgent"));
		assertEquals(BrainMemoryUtils.ACTIVE, saved.get("state"));
		assertEquals(true, saved.get("confirmed"));
	}

	// ---- assistant ----

	@Test
	void rememberSavesFindsTheSameAndReplacesItsOwn() throws Exception {
		Map<String, Object> first = BrainMemoryUtils.remember(user, Map.of("text",
				"Priya Shah approves the Acme budget", "kind", "fact", "about", List.of(ABOUT_PRIYA)), CHAT);
		assertEquals(BrainMemoryUtils.SAVED, first.get("status"));
		Map<String, Object> memory = memory(first);
		assertEquals(false, memory.get("confirmed"));
		assertEquals(BrainMemoryUtils.ASSISTANT, memory.get("origin"));
		assertEquals("Priya Shah", ((Map<?, ?>) ((List<?>) memory.get("about")).get(0)).get("name"));
		assertEquals("room-1", ((Map<?, ?>) memory.get("source")).get("roomId"));

		Map<String, Object> again = BrainMemoryUtils.remember(user, Map.of("text",
				"priya shah approves the Acme budget.", "kind", "fact", "about", List.of(ABOUT_PRIYA)), CHAT);
		assertEquals(BrainMemoryUtils.EXISTS, again.get("status"));
		assertEquals(1, count("BRAIN_MEMORY"));

		String oldId = (String) memory.get("id");
		Map<String, Object> replaced = BrainMemoryUtils.remember(user, Map.of("text",
				"Priya Shah approves Acme budgets up to 50k only", "kind", "fact", "replaces", oldId), CHAT);
		assertEquals(BrainMemoryUtils.UPDATED, replaced.get("status"));
		assertEquals(BrainMemoryUtils.SUPERSEDED, ((Map<?, ?>) replaced.get("replaced")).get("state"));
		assertEquals(oldId, memory(replaced).get("replacesId"));
		// the replacement keeps the old memory's links when none are given
		assertEquals(1, ((List<?>) memory(replaced).get("about")).size());

		// the chat card's Undo puts the old memory back
		String newId = (String) memory(replaced).get("id");
		Map<String, Object> undone = BrainMemoryUtils.resolveMemory(user, newId, BrainMemoryUtils.DISMISS);
		assertEquals(BrainMemoryUtils.DISMISSED, ((Map<?, ?>) undone.get("memory")).get("state"));
		assertEquals(BrainMemoryUtils.ACTIVE, ((Map<?, ?>) undone.get("restored")).get("state"));

		// saving what the owner just undid, in the same conversation, is refused; elsewhere it is new news
		Map<String, Object> sameRoom = BrainMemoryUtils.remember(user, Map.of("text",
				"Priya Shah approves Acme budgets up to 50k only", "kind", "fact"), CHAT);
		assertEquals(BrainMemoryUtils.UNDONE_HERE, sameRoom.get("status"));
		Map<String, Object> otherRoom = BrainMemoryUtils.remember(user,
				Map.of("text", "Priya Shah approves Acme budgets up to 50k only", "kind", "fact"),
				new Source(BrainMemoryUtils.FROM_CHAT, "th-2", "room-2", "msg-9", null, null));
		assertEquals(BrainMemoryUtils.SAVED, otherRoom.get("status"));
	}

	@Test
	void theAssistantNeverChangesWhatTheOwnerWrote() {
		String id = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Dana is my manager")).get("id");

		Map<String, Object> change = BrainMemoryUtils.remember(user,
				Map.of("text", "Dana is no longer my manager", "kind", "fact", "replaces", id), CHAT);
		assertEquals(BrainMemoryUtils.NEEDS_OWNER, change.get("status"));
		assertEquals("Dana is no longer my manager", ((Map<?, ?>) change.get("proposed")).get("text"));
		assertEquals("Dana is my manager", BrainMemoryUtils.find(ownerId, ownerType, id).text());

		Map<String, Object> forget = BrainMemoryUtils.forget(user, id);
		assertEquals(BrainMemoryUtils.NEEDS_OWNER, forget.get("status"));
		assertEquals(BrainMemoryUtils.ACTIVE, BrainMemoryUtils.find(ownerId, ownerType, id).state());
		assertThrows(IllegalArgumentException.class,
				() -> BrainMemoryUtils.resolveMemory(user, id, BrainMemoryUtils.DISMISS));
	}

	@Test
	void forgetHidesALearnedMemoryAndRestoreBringsItBack() {
		String id = (String) memory(BrainMemoryUtils.remember(user,
				Map.of("text", "Prefers agendas a day ahead", "kind", "preference"), CHAT)).get("id");
		assertEquals(BrainMemoryUtils.FORGOTTEN, BrainMemoryUtils.forget(user, id).get("status"));
		assertEquals(BrainMemoryUtils.DISMISSED, BrainMemoryUtils.find(ownerId, ownerType, id).state());
		BrainMemoryUtils.resolveMemory(user, id, BrainMemoryUtils.RESTORE);
		assertEquals(BrainMemoryUtils.ACTIVE, BrainMemoryUtils.find(ownerId, ownerType, id).state());
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.forget(user, "missing"));
		BrainMemoryUtils.resolveMemory(user, id, BrainMemoryUtils.CONFIRM);
		assertTrue(BrainMemoryUtils.find(ownerId, ownerType, id).confirmed());
		BrainMemoryUtils.resolveMemory(user, id, BrainMemoryUtils.UNCONFIRM);
		assertFalse(BrainMemoryUtils.find(ownerId, ownerType, id).confirmed());
		String owners = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Board meets monthly")).get("id");
		assertThrows(IllegalArgumentException.class,
				() -> BrainMemoryUtils.resolveMemory(user, owners, BrainMemoryUtils.UNCONFIRM));
	}

	@Test
	void acceptingASuggestionReplacesWhatItNamesAndDismissedOnesCanReopen() throws Exception {
		String ownerMemory = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Standup is at 9")).get("id");
		Memory suggestion = suggestion("m-sugg", "Standup moved to 9:30", ownerMemory);
		BrainMemoryUtils.resolveMemory(user, suggestion.id(), BrainMemoryUtils.ACCEPT);
		assertEquals(BrainMemoryUtils.ACTIVE, BrainMemoryUtils.find(ownerId, ownerType, suggestion.id()).state());
		assertTrue(BrainMemoryUtils.find(ownerId, ownerType, suggestion.id()).confirmed());
		assertEquals(BrainMemoryUtils.SUPERSEDED, BrainMemoryUtils.find(ownerId, ownerType, ownerMemory).state());

		Memory other = suggestion("m-other", "Prefers Teams to email", null);
		BrainMemoryUtils.resolveMemory(user, other.id(), BrainMemoryUtils.DISMISS);
		BrainMemoryUtils.resolveMemory(user, other.id(), BrainMemoryUtils.REOPEN);
		assertEquals(BrainMemoryUtils.SUGGESTED, BrainMemoryUtils.find(ownerId, ownerType, other.id()).state());
		// the owner's Undo of Keep: a suggestion again, and the memory it replaced is back
		Map<String, Object> reopened = BrainMemoryUtils.resolveMemory(user, suggestion.id(), BrainMemoryUtils.REOPEN);
		assertEquals(BrainMemoryUtils.SUGGESTED, ((Map<?, ?>) reopened.get("memory")).get("state"));
		assertEquals(false, ((Map<?, ?>) reopened.get("memory")).get("confirmed"));
		assertEquals(BrainMemoryUtils.ACTIVE, ((Map<?, ?>) reopened.get("restored")).get("state"));
		BrainMemoryUtils.resolveMemory(user, suggestion.id(), BrainMemoryUtils.ACCEPT);
		assertEquals(BrainMemoryUtils.SUPERSEDED, BrainMemoryUtils.find(ownerId, ownerType, ownerMemory).state());
		// deleting the accepted memory takes the superseded version with it
		BrainMemoryUtils.deleteMemory(user, suggestion.id());
		assertNull(BrainMemoryUtils.find(ownerId, ownerType, ownerMemory));
	}

	@Test
	void searchRanksAndLeavesOutNeverIngestPeople() throws Exception {
		person("p-mark", "Mark Liu", true);
		Map<String, Object> aboutMark = Map.of("type", "person", "id", "p-mark");
		BrainMemoryUtils.remember(user, Map.of("text", "Approves every budget over 50k", "kind", "fact", "about",
				List.of(ABOUT_PRIYA)), CHAT);
		// the assistant may not keep anything about someone the owner excluded
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.remember(user,
				Map.of("text", "Prefers calls to email", "kind", "preference", "about", List.of(aboutMark)), CHAT));
		// the owner may, but it is never searched or recalled while Mark is excluded
		BrainMemoryUtils.saveMemory(user, Map.of("text", "Prefers calls to email", "about", List.of(aboutMark)));

		Map<String, Object> budget = BrainMemoryUtils.search(user, "Priya budgets", null, null);
		assertEquals(1, budget.get("total"));
		Map<String, Object> calls = BrainMemoryUtils.search(user, "calls", null, null);
		assertEquals(0, calls.get("total"));
		assertNotNull(calls.get("note"));
		assertEquals(1, BrainMemoryUtils.search(user, "", List.of(new Ref("person", "p-priya")), 5).get("total"));
	}

	// ---- topics ----

	@Test
	void deletingATopicTakesItsOnlyMemoriesAndUndoPutsThemBack() throws Exception {
		topic("t-acme", "Acme");
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				"Get the room code for Florida", "open").get("noteId");
		BrainTopicUtils.completeGoal(user, "t-acme", goalId, "Florida room code is 444553");
		List<Map<String, Object>> goals = BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme");
		String onlyTopic = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Procurement needs three quotes",
				"about", List.of(Map.of("type", "topic", "id", "t-acme")))).get("id");
		String shared = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Priya approves Acme spend",
				"about", List.of(Map.of("type", "topic", "id", "t-acme"), ABOUT_PRIYA))).get("id");

		String changeId = (String) BrainTopicUtils.deleteTopic(user, "t-acme").get("changeId");
		assertTrue(BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme").isEmpty());
		assertNull(BrainMemoryUtils.find(ownerId, ownerType, onlyTopic));
		assertEquals(List.of(new Ref("person", "p-priya")), BrainMemoryUtils.find(ownerId, ownerType, shared).about());

		BrainTopicChangeUtils.undo(user, changeId);
		assertEquals(goals, BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme"));
		assertEquals(List.of(new Ref("topic", "t-acme")), BrainMemoryUtils.find(ownerId, ownerType, onlyTopic).about());
		assertEquals(new TreeSet<>(List.of("person:p-priya", "topic:t-acme")),
				refs(BrainMemoryUtils.find(ownerId, ownerType, shared)));
	}

	@Test
	void mergingTopicsMovesMemoryLinksAndUndoSplitsThemAgain() throws Exception {
		topic("t-a", "Acme renewal");
		topic("t-b", "Acme");
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-a", null, BrainTopicUtils.GOAL,
				"Get the room code for Florida", "open").get("noteId");
		BrainTopicUtils.completeGoal(user, "t-a", goalId, "Florida room code is 444553");
		List<Map<String, Object>> goals = BrainTopicUtils.getGoals(ownerId, ownerType, "t-a");
		String onSource = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Renewal is due in March",
				"about", List.of(Map.of("type", "topic", "id", "t-a")))).get("id");
		String onBoth = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Legal reviews every Acme contract",
				"about", List.of(Map.of("type", "topic", "id", "t-a"), Map.of("type", "topic", "id", "t-b"))))
				.get("id");

		String changeId = (String) BrainTopicUtils.mergeTopics(user, "t-a", "t-b").get("changeId");
		assertEquals(goals, BrainTopicUtils.getGoals(ownerId, ownerType, "t-b"));
		assertEquals(List.of(new Ref("topic", "t-b")), BrainMemoryUtils.find(ownerId, ownerType, onSource).about());
		assertEquals(List.of(new Ref("topic", "t-b")), BrainMemoryUtils.find(ownerId, ownerType, onBoth).about());

		BrainTopicChangeUtils.undo(user, changeId);
		assertEquals(goals, BrainTopicUtils.getGoals(ownerId, ownerType, "t-a"));
		assertTrue(BrainTopicUtils.getGoals(ownerId, ownerType, "t-b").isEmpty());
		assertEquals(List.of(new Ref("topic", "t-a")), BrainMemoryUtils.find(ownerId, ownerType, onSource).about());
		assertEquals(new TreeSet<>(List.of("topic:t-a", "topic:t-b")),
				refs(BrainMemoryUtils.find(ownerId, ownerType, onBoth)));
	}

	// ---- recall ----

	@Test
	void recallUsesTheThreadsPeopleTopicsAndAccountsAndLeavesOutWhoTheOwnerExcluded() throws Exception {
		person("p-dana", "Dana Lee", false);
		person("p-mark", "Mark Liu", true);
		sql("INSERT INTO BRAIN_ACCOUNT (OWNER_ID, OWNER_TYPE, ACCOUNT_ID, NAME) VALUES (?, ?, 'a-acme', 'Acme Corp')",
				ownerId, ownerType);
		sql("UPDATE BRAIN_PERSON SET ACCOUNT_ID = 'a-acme' WHERE PERSON_ID = 'p-priya'");
		topic("t-acme", "Acme");
		topic("t-other", "Other");
		sql("UPDATE BRAIN_TOPIC SET ACCOUNT_ID = 'a-acme' WHERE TOPIC_ID = 't-acme'");
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE, SUBJECT) VALUES (?, ?, 'th-1', "
				+ "'outlook', 'Q4 budget')", ownerId, ownerType);
		participant("p-priya", true);
		participant("p-dana", false);
		participant("p-mark", true);
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE) VALUES (?, ?, 'th-1', "
				+ "'t-acme', 'you')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE) VALUES (?, ?, 'th-1', "
				+ "'t-other', 'suggested')", ownerId, ownerType);

		String preference = save("Sign emails as Rob", "preference");
		String aboutThread = save("The vendor agreed to 10% off", null, Map.of("type", "thread", "id", "th-1"));
		String aboutPriya = save("Priya approves budgets", null, ABOUT_PRIYA);
		String aboutAcme = save("Acme needs three quotes", null, Map.of("type", "topic", "id", "t-acme"));
		String aboutAccount = save("Acme Corp pays at net 60", null, Map.of("type", "account", "id", "a-acme"));
		save("Dana books travel", null, Map.of("type", "person", "id", "p-dana"));
		save("Mark prefers calls", null, Map.of("type", "person", "id", "p-mark"));
		save("Other is on hold", null, Map.of("type", "topic", "id", "t-other"));
		save("Wrong thread", null, Map.of("type", "thread", "id", "th-1")); // replaced below
		sql("UPDATE BRAIN_MEMORY_LINK SET REF_ID = 'th-2' WHERE MEMORY_ID = (SELECT MEMORY_ID FROM BRAIN_MEMORY "
				+ "WHERE TEXT = 'Wrong thread')");

		List<String> ids = ids(BrainMemoryRecall.recall(ownerId, ownerType, "th-1", null, 4000));
		assertEquals(Set.of(preference, aboutThread, aboutPriya, aboutAcme, aboutAccount), Set.copyOf(ids));
		assertEquals(List.of(preference, aboutThread, aboutPriya), ids.subList(0, 3));

		// a channel rule keeps the whole thread out: its own memories and its people go, its topics stay
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, CHANNEL, CREATED_AT) VALUES (?, ?, 'r1', "
				+ "'exclude_channel', 'outlook', CURRENT_TIMESTAMP)", ownerId, ownerType);
		assertEquals(Set.of(preference, aboutAcme, aboutAccount),
				Set.copyOf(ids(BrainMemoryRecall.recall(ownerId, ownerType, "th-1", null, 4000))));

		// a /new session only gets what applies everywhere
		assertEquals(List.of(preference), ids(BrainMemoryRecall.recall(ownerId, ownerType, "session:abc", null, 4000)));

		Map<String, Object> shown = BrainMemoryRecall.recallMemories(user, "th-1", null);
		assertEquals(true, shown.get("enabled"));
		assertEquals(3, ((List<?>) shown.get("items")).size());
		assertTrue(((String) shown.get("prompt")).contains("[m:" + preference + "] Sign emails as Rob"));
		// the owner's chat opened from th-1
		Room room = mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of("source", Map.of("threadId", "th-1")));
		String block = BrainMemoryRecall.promptBlock(user, room);
		assertTrue(block.contains("### What you remember for this thread"));
		// only the thread's chat gets its topic memories; a room not read as one would fall back to the session's
		assertTrue(block.contains("[m:" + aboutAcme + "] Acme needs three quotes"));

		// memory off: no block for the prompt and nothing recalled
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, MEMORY_USE) "
				+ "VALUES (?, ?, 85, 40, 1, FALSE)", ownerId, ownerType);
		assertNull(BrainMemoryRecall.promptBlock(user, room));
		assertEquals(false, BrainMemoryRecall.recallMemories(user, "th-1", null).get("enabled"));
		assertThrows(IllegalArgumentException.class, () -> BrainMemoryUtils.requireAssistantMemory(user));
	}

	@SafeVarargs
	private String save(String text, String kind, Map<String, Object>... about) {
		Map<String, Object> memory = new java.util.HashMap<>();
		memory.put("text", text);
		if (kind != null) {
			memory.put("kind", kind);
		}
		memory.put("about", List.of(about));
		return (String) BrainMemoryUtils.saveMemory(user, memory).get("id");
	}

	private void participant(String personId, boolean included) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED) VALUES (?, ?, "
				+ "'th-1', ?, ?)", ownerId, ownerType, personId, included);
	}

	private static List<String> ids(BrainMemoryRecall.Recall recall) {
		return recall.lines().stream().map(line -> line.memory().id()).toList();
	}

	// ---- the review of finished chats ----

	@Test
	void reviewSuggestsWhatTheOwnerSaidAndMovesItsWatermark() throws Exception {
		person("p-dana", "Dana Lee", false);
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE, SUBJECT) VALUES (?, ?, 'th-1', "
				+ "'outlook', 'Q4 budget')", ownerId, ownerType);
		participant("p-dana", true);
		Room room = Mockito.mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		InputMessage said = InputMessage.text(room, "[SEMOSS_WORK_CONTEXT_V1]\n{}\n[/SEMOSS_WORK_CONTEXT_V1]\n\n"
				+ "FYI Dana approves every Acme budget now.");
		said.setMessageId("msg-1");
		ResponseMessage answered = ResponseMessage.builder().withText("Noted.").build();
		answered.setMessageId("msg-2");
		List<AbstractMessage> messages = new ArrayList<>(List.of(said, answered));
		when(room.getMessages()).thenReturn(messages);
		user.getRoomHash().put("room-1", room);
		IModelEngine model = Mockito.mock(IModelEngine.class);
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenReturn(new AskStringModelEngineResponse(
				"{\"memories\": [{\"text\": \"Dana approves every Acme budget.\", \"kind\": \"fact\", "
						+ "\"about\": [\"p1\"], \"replaces\": \"\", \"evidence\": \"Dana approves every Acme budget\"}]}",
				0, 0));
		try (MockedStatic<BrainTopicModel> topicModel = mockStatic(BrainTopicModel.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class, Mockito.CALLS_REAL_METHODS)) {
			topicModel.when(() -> BrainTopicModel.engine(user)).thenReturn("llm-1");
			utility.when(() -> Utility.getModel("llm-1")).thenReturn(model);

			List<Memory> created = BrainMemoryReview.review(user, ownerId, ownerType, "room-1", "th-1");
			assertEquals(1, created.size());
			Memory suggestion = BrainMemoryUtils.find(ownerId, ownerType, created.get(0).id());
			assertEquals(BrainMemoryUtils.SUGGESTED, suggestion.state());
			assertEquals(WorkThreadInsights.BRAIN, suggestion.origin());
			assertEquals(List.of(new Ref("person", "p-dana")), suggestion.about());
			assertEquals("room-1", suggestion.source().roomId());
			assertEquals("msg-1", suggestion.source().ref());
			assertEquals("msg-2", value("SELECT LAST_MESSAGE_ID FROM BRAIN_MEMORY_SCAN WHERE ROOM_ID = 'room-1'"));

			// nothing new since: no second call, and the same answer would not be suggested twice anyway
			assertTrue(BrainMemoryReview.review(user, ownerId, ownerType, "room-1", "th-1").isEmpty());
			ResponseMessage again = ResponseMessage.builder().withText("Anything else?").build();
			again.setMessageId("msg-3");
			messages.add(again);
			assertTrue(BrainMemoryReview.review(user, ownerId, ownerType, "room-1", "th-1").isEmpty());
			verify(model, times(1)).ask(anyString(), anyString(), any(), anyMap());
			assertEquals("msg-3", value("SELECT LAST_MESSAGE_ID FROM BRAIN_MEMORY_SCAN WHERE ROOM_ID = 'room-1'"));

			// learning turned off: the chat is not read at all
			InputMessage more = InputMessage.text(room, "Also, Dana signs off on every Acme hire.");
			more.setMessageId("msg-4");
			messages.add(more);
			sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, MEMORY_LEARN) "
					+ "VALUES (?, ?, 85, 40, 1, FALSE)", ownerId, ownerType);
			assertTrue(BrainMemoryReview.review(user, ownerId, ownerType, "room-1", "th-1").isEmpty());
			verify(model, times(1)).ask(anyString(), anyString(), any(), anyMap());
		}
	}

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void editTopicKeepsItsNotesAsMemoriesAboutTheTopic() throws Exception {
		topic("t-acme", "Acme");
		topic("t-other", "Other");
		BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, "Close by Q4",
				"Procurement needs three quotes", null, null, null);
		assertEquals(1, count("BRAIN_TOPIC_NOTE"));
		Map<String, Object> topic = BrainTopicUtils.getTopic(user, null, "Acme");
		List<Map<String, Object>> notes = (List<Map<String, Object>>) topic.get("notes");
		assertEquals(1, notes.size());
		assertEquals("Procurement needs three quotes", notes.get(0).get("text"));
		assertEquals(true, notes.get(0).get("confirmed"));
		String noteId = (String) notes.get(0).get("id");
		assertEquals(Set.of("topic:t-acme"), refs(BrainMemoryUtils.find(ownerId, ownerType, noteId)));
		String goalId = (String) ((List<Map<String, Object>>) topic.get("goals")).get(0).get("noteId");

		// with memory off the assistant does not see them
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, MEMORY_USE) "
				+ "VALUES (?, ?, 85, 40, 1, FALSE)", ownerId, ownerType);
		assertEquals(List.of(), BrainTopicUtils.getTopic(user, "t-acme", null).get("notes"));

		// a note of another topic is not this topic's to delete; this topic's note and goal are
		String elsewhere = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Other needs a quote",
				"about", List.of(Map.of("type", "topic", "id", "t-other")))).get("id");
		assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, "t-acme", null, null, null,
				null, null, null, null, null, elsewhere, null, null));
		BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, null, null, noteId, null, null);
		BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, null, null, goalId, null, null);
		assertNull(BrainMemoryUtils.find(ownerId, ownerType, noteId));
		assertNotNull(BrainMemoryUtils.find(ownerId, ownerType, elsewhere));
		assertEquals(0, count("BRAIN_TOPIC_NOTE"));
	}

	@Test
	void editTopicCompletesAGoalAndKeepsItsHistoryOnRepeatedCalls() throws Exception {
		topic("t-acme", "Acme");
		String text = "  Close by Q4  ";
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				text, "open").get("noteId");
		sql("UPDATE BRAIN_TOPIC_NOTE SET ORIGIN = 'brain', SOURCE_REF = 'thread:th-1' WHERE NOTE_ID = ?", goalId);
		Map<String, Object> expected = new LinkedHashMap<>(BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme").get(0));
		expected.put("status", "done");

		BrainEditTopicReactor reactor = editTopicReactor(Map.of("topicId", "t-acme", "completeGoalId", goalId));
		for (int attempt = 0; attempt < 2; attempt++) {
			Map<?, ?> result = (Map<?, ?>) reactor.execute().getValue();
			Map<?, ?> updatedTopic = (Map<?, ?>) result.get("topic");
			assertEquals(List.of(Map.of("noteId", goalId, "text", text, "status", "done")), updatedTopic.get("goals"));
			assertEquals(List.of(expected), BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme"));
			assertEquals(1, count("BRAIN_TOPIC_NOTE"));
		}
	}

	@Test
	void goalResolutionIsReturnedAndSurvivesRetriesEditsAndReopening() throws Exception {
		topic("t-acme", "Acme");
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				"Close by Q4", "open").get("noteId");
		String resolution = "Signed the renewal.\nLegal approved the final terms.";
		Map<?, ?> result = (Map<?, ?>) editTopicReactor(Map.of("topicId", "t-acme", "completeGoalId", goalId,
				"goalResolution", "  " + resolution + "  ")).execute().getValue();
		Map<?, ?> updatedTopic = (Map<?, ?>) result.get("topic");
		assertEquals(List.of(Map.of("noteId", goalId, "text", "Close by Q4", "status", "done", "resolution",
				resolution)), updatedTopic.get("goals"));

		for (String omittedOrBlank : new String[] { null, "", " \t " }) {
			BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, null, null, null,
					goalId, omittedOrBlank);
			Map<String, Object> saved = BrainTopicUtils.saveTopicNote(user, "t-acme", goalId, BrainTopicUtils.GOAL,
					"Close by Q4, including the addendum", "open");
			assertEquals("open", saved.get("status"));
			assertEquals(resolution, saved.get("resolution"));
			BrainAgentEdits.editTopic(user, "t-acme", null, null, null, null, null, null, null, null, null,
					goalId, omittedOrBlank);
			Map<String, Object> completed = BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme").get(0);
			assertEquals("done", completed.get("status"));
			assertEquals("Close by Q4, including the addendum", completed.get("text"));
			assertEquals(resolution, completed.get("resolution"));
			assertEquals(1, count("BRAIN_TOPIC_NOTE"));
		}
	}

	@Test
	void resolutionWithoutAGoalIsRejectedBeforeOtherTopicChanges() throws Exception {
		topic("t-acme", "Acme");
		for (String missingGoalId : new String[] { null, "", "  " }) {
			assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, "t-acme", null,
					"Renamed", null, null, null, null, "New goal", null, null, missingGoalId, "Signed the renewal"));
		}
		assertEquals("Acme", BrainTopicUtils.getTopic(user, "t-acme", null).get("name"));
		assertEquals(0, count("BRAIN_TOPIC_NOTE"));
	}

	@Test
	void completingAGoalRejectsIdsOutsideTheOwnersTopicGoals() throws Exception {
		topic("t-acme", "Acme");
		topic("t-other", "Other");
		BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL, "Close by Q4", "open");
		String otherGoalId = (String) BrainTopicUtils.saveTopicNote(user, "t-other", null, BrainTopicUtils.GOAL,
				"Other goal", "open").get("noteId");
		sql("INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, KIND, TEXT, STATE) "
				+ "VALUES (?, ?, 'legacy-note', 't-acme', 'note', 'A note', 'confirmed')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, KIND, TEXT, STATE) "
				+ "VALUES ('another-owner', ?, 'foreign-goal', 't-acme', 'goal', 'Their goal', 'open')", ownerType);
		String memoryId = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Needs three quotes",
				"about", List.of(Map.of("type", "topic", "id", "t-acme")))).get("id");
		List<Map<String, Object>> goals = BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme");

		for (String id : List.of("missing", otherGoalId, "legacy-note", memoryId, "foreign-goal")) {
			assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, "t-acme", null,
					null, null, null, null, null, null, null, null, id, "Completed the renewal"), id);
		}
		assertEquals(goals, BrainTopicUtils.getGoals(ownerId, ownerType, "t-acme"));
		assertEquals(4, count("BRAIN_TOPIC_NOTE"));
		assertEquals(0L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_NOTE WHERE STATE = 'done'"));
		assertEquals(0L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_NOTE WHERE RESOLUTION IS NOT NULL"));
		assertEquals("confirmed", value("SELECT STATE FROM BRAIN_TOPIC_NOTE WHERE NOTE_ID = 'legacy-note'"));
		assertEquals(BrainMemoryUtils.ACTIVE, BrainMemoryUtils.find(ownerId, ownerType, memoryId).state());
	}

	@Test
	void editTopicExposesOptionalGoalCompletionWithOwnerApproval() {
		Map<String, Object> tool = CollaborationAgentTools.definitions().stream()
				.filter(definition -> "EditTopic".equals(definition.get("name"))).findFirst().orElseThrow();
		Map<?, ?> schema = (Map<?, ?>) tool.get("inputSchema");
		Map<?, ?> properties = (Map<?, ?>) schema.get("properties");
		assertEquals("string", ((Map<?, ?>) properties.get("completeGoalId")).get("type"));
		assertFalse(((List<?>) schema.get("required")).contains("completeGoalId"));
		assertEquals("string", ((Map<?, ?>) properties.get("goalResolution")).get("type"));
		assertFalse(((List<?>) schema.get("required")).contains("goalResolution"));
		assertEquals(MCPUtility.MCPExecution.ASK.getValue(),
				((Map<?, ?>) tool.get("_meta")).get(MCPUtility.SMSS_MCP_EXECUTION));
	}

	@Test
	void assistantCompletesCurrentTopicGoalWithEvidenceAndPreservesItsText() throws Exception {
		topic("t-acme", "Rule of Nines");
		String goal = "Get booking details for nevada";
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				goal, "open").get("noteId");
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'room-1', 't-acme', 'linked', 'you')", ownerId, ownerType);
		String resolution = "Booking details confirmed: American flight 4884, arriving 11/01/2026 at Dulles.";
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(ownerId, List.of("room-1")))
					.thenReturn(List.of(Map.of("ROOM_ID", "room-1", "PROJECT_ID",
							CollaborationUtils.COLLABORATION_PROJECT_ID, "OPTIONS", "{}")));
			BrainCompleteGoalReactor reactor = new BrainCompleteGoalReactor() {
				@Override
				protected User getUser() {
					return user;
				}
			};
			Insight insight = mock(Insight.class);
			when(insight.getRoomId()).thenReturn("room-1");
			reactor.setInsight(insight);
			NounStore store = new NounStore("complete-goal");
			Map.of("topicId", "t-acme", "goalId", goalId, "resolution", resolution)
					.forEach((key, value) -> store.makeNoun(key).addLiteral(value));
			reactor.setNounStore(store);
			for (int attempt = 0; attempt < 2; attempt++) {
				Map<?, ?> result = (Map<?, ?>) reactor.execute().getValue();
				assertEquals(List.of("goal completed"), result.get("changes"));
				assertEquals(List.of(Map.of("noteId", goalId, "text", goal, "status", "done", "resolution", resolution)),
						((Map<?, ?>) result.get("topic")).get("goals"));
			}
		}
		assertEquals(1, count("BRAIN_TOPIC_NOTE"));
	}

	@Test
	void automaticCompletionRequiresEvidenceAndAnOwnedChatTopic() throws Exception {
		topic("t-acme", "Acme");
		topic("t-other", "Other");
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "t-acme", null, BrainTopicUtils.GOAL,
				"Get booking details", "open").get("noteId");
		String otherGoal = (String) BrainTopicUtils.saveTopicNote(user, "t-other", null, BrainTopicUtils.GOAL,
				"Another goal", "open").get("noteId");
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'room-1', 't-acme', 'linked', 'you')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'child-room', 't-acme', 'linked', 'you')", ownerId, ownerType);
		for (String missingEvidence : new String[] { null, "", "  " }) {
			assertThrows(IllegalArgumentException.class,
					() -> BrainAgentEdits.completeGoal(user, "room-1", "t-acme", goalId, missingEvidence));
		}
		try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(ownerId, List.of("room-1")))
					.thenReturn(List.of(Map.of("ROOM_ID", "room-1", "PROJECT_ID",
							CollaborationUtils.COLLABORATION_PROJECT_ID, "OPTIONS", "{}")));
			assertThrows(IllegalArgumentException.class,
					() -> BrainAgentEdits.completeGoal(user, "room-1", "t-other", otherGoal, "Got the details"));
			assertThrows(IllegalArgumentException.class,
					() -> BrainAgentEdits.completeGoal(user, "room-1", "t-acme", otherGoal, "Got the details"));
			assertThrows(IllegalArgumentException.class,
					() -> BrainAgentEdits.completeGoal(user, "not-my-room", "t-acme", goalId, "Got the details"));
			logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(ownerId, List.of("child-room")))
					.thenReturn(List.of(Map.of("ROOM_ID", "child-room", "PROJECT_ID",
							CollaborationUtils.COLLABORATION_PROJECT_ID, "OPTIONS",
							"{\"subagent_parent_room_id\":\"room-1\"}")));
			assertTrue(assertThrows(IllegalArgumentException.class,
					() -> BrainAgentEdits.completeGoal(user, "child-room", "t-acme", goalId, "Got the details"))
					.getMessage().contains("subagent"));
		}
		assertEquals(0L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_NOTE WHERE STATE = 'done'"));
	}

	@Test
	void automaticGoalToolIsOnlyOfferedToTheOwnersRootAssistant() {
		Room room = new Room();
		room.setProjectId(CollaborationUtils.COLLABORATION_PROJECT_ID);
		room.setOptionsMap(new LinkedHashMap<>());
		Map<String, Object> tool = CollaborationAgentTools.definitions(room, user, true).stream()
				.filter(definition -> "CompleteGoal".equals(definition.get("name"))).findFirst().orElseThrow();
		assertEquals(MCPUtility.MCPExecution.AUTO.getValue(),
				((Map<?, ?>) tool.get("_meta")).get(MCPUtility.SMSS_MCP_EXECUTION));
		assertEquals(Set.of("topicId", "goalId", "resolution"),
				Set.copyOf((List<?>) ((Map<?, ?>) tool.get("inputSchema")).get("required")));
		assertTrue(CollaborationAgentTools.definitions().stream().noneMatch(t -> "CompleteGoal".equals(t.get("name"))));
		assertTrue(CollaborationAgentTools.definitions(room, user, false).stream()
				.noneMatch(t -> "CompleteGoal".equals(t.get("name"))));
		room.setOptionsMap(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "delegation-1"));
		assertTrue(CollaborationAgentTools.definitions(room, user, true).stream()
				.noneMatch(t -> "CompleteGoal".equals(t.get("name"))));
	}

	@Test
	void showItemIsOnlyOfferedToTheOwnersRootChat() {
		Room room = new Room();
		room.setProjectId(CollaborationUtils.COLLABORATION_PROJECT_ID);
		room.setOptionsMap(new LinkedHashMap<>());
		Map<String, Object> tool = CollaborationAgentTools.definitions(room, user, true).stream()
				.filter(definition -> "ShowItem".equals(definition.get("name"))).findFirst().orElseThrow();
		assertEquals(MCPUtility.MCPExecution.AUTO.getValue(),
				((Map<?, ?>) tool.get("_meta")).get(MCPUtility.SMSS_MCP_EXECUTION));
		assertEquals(Set.of("emailRef", "threadId", "chatId", "path"),
				((Map<?, ?>) ((Map<?, ?>) tool.get("inputSchema")).get("properties")).keySet());
		assertTrue(CollaborationAgentTools.definitions().stream().noneMatch(t -> "ShowItem".equals(t.get("name"))));
		assertTrue(CollaborationAgentTools.definitions(room, user, false).stream()
				.noneMatch(t -> "ShowItem".equals(t.get("name"))));
		room.setOptionsMap(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "delegation-1"));
		assertTrue(CollaborationAgentTools.definitions(room, user, true).stream()
				.noneMatch(t -> "ShowItem".equals(t.get("name"))));
	}

	// ---- settings and reset ----

	@Test
	void memorySettingsReadNullAsOn() throws Exception {
		assertTrue(BrainProfileUtils.usesMemory(ownerId, ownerType));
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, MEMORY_LEARN) "
				+ "VALUES (?, ?, 85, 40, 1, FALSE)", ownerId, ownerType);
		assertTrue(BrainProfileUtils.usesMemory(ownerId, ownerType));
		assertFalse(BrainProfileUtils.learnsMemory(ownerId, ownerType));
		sql("UPDATE BRAIN_SETTINGS SET MEMORY_USE = FALSE, MEMORY_LEARN = TRUE WHERE OWNER_ID = ?", ownerId);
		assertFalse(BrainProfileUtils.usesMemory(ownerId, ownerType));
		// no learning while memory itself is off
		assertFalse(BrainProfileUtils.learnsMemory(ownerId, ownerType));
	}

	@Test
	void resetMyDataErasesMemories() throws Exception {
		BrainMemoryUtils.saveMemory(user, Map.of("text", "Prefers short emails", "about", List.of(ABOUT_PRIYA)));
		sql("INSERT INTO BRAIN_MEMORY_SCAN (OWNER_ID, OWNER_TYPE, ROOM_ID, LAST_MESSAGE_ID) VALUES (?, ?, 'r1', 'x')",
				ownerId, ownerType);
		BrainResetUtils.resetMyData(user);
		assertEquals(0, count("BRAIN_MEMORY"));
		assertEquals(0, count("BRAIN_MEMORY_LINK"));
		assertEquals(0, count("BRAIN_MEMORY_SCAN"));
	}

	// ---- helpers ----

	private BrainEditTopicReactor editTopicReactor(Map<String, String> arguments) {
		BrainEditTopicReactor reactor = new BrainEditTopicReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		NounStore store = new NounStore("edit-topic");
		arguments.forEach((key, value) -> store.makeNoun(key).addLiteral(value));
		reactor.setNounStore(store);
		return reactor;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> memory(Map<String, Object> result) {
		return (Map<String, Object>) result.get("memory");
	}

	private static Set<String> refs(Memory memory) {
		Set<String> refs = new TreeSet<>();
		for (Ref ref : memory.about()) {
			refs.add(ref.type() + ":" + ref.id());
		}
		return refs;
	}

	private Memory suggestion(String id, String text, String replaces) throws Exception {
		java.sql.Timestamp now = CollaborationDbUtils.now();
		Memory memory = new Memory(id, BrainMemoryUtils.FACT, text, BrainMemoryUtils.SUGGESTED, WorkThreadInsights.BRAIN,
				false, false, replaces, null,
				new Source(BrainMemoryUtils.FROM_CHAT_REVIEW, "th-1", "room-1", "msg-2", null, null), now, now, null,
				List.of());
		connection.setAutoCommit(false);
		BrainMemoryUtils.insert(connection, ownerId, ownerType, memory);
		connection.commit();
		connection.setAutoCommit(true);
		return memory;
	}

	private void person(String personId, String name, boolean neverIngest) throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, DISPLAY_NAME, NEVER_INGEST) VALUES (?, ?, ?, ?, ?)",
				ownerId, ownerType, personId, name, neverIngest);
	}

	private void topic(String topicId, String name) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS, CREATED_AT, UPDATED_AT) "
				+ "VALUES (?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", ownerId, ownerType, topicId,
				name);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private int count(String table) throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
			rs.next();
			return rs.getInt(1);
		}
	}
}
