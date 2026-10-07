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
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
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
		String onlyTopic = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Procurement needs three quotes",
				"about", List.of(Map.of("type", "topic", "id", "t-acme")))).get("id");
		String shared = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Priya approves Acme spend",
				"about", List.of(Map.of("type", "topic", "id", "t-acme"), ABOUT_PRIYA))).get("id");

		String changeId = (String) BrainTopicUtils.deleteTopic(user, "t-acme").get("changeId");
		assertNull(BrainMemoryUtils.find(ownerId, ownerType, onlyTopic));
		assertEquals(List.of(new Ref("person", "p-priya")), BrainMemoryUtils.find(ownerId, ownerType, shared).about());

		BrainTopicChangeUtils.undo(user, changeId);
		assertEquals(List.of(new Ref("topic", "t-acme")), BrainMemoryUtils.find(ownerId, ownerType, onlyTopic).about());
		assertEquals(new TreeSet<>(List.of("person:p-priya", "topic:t-acme")),
				refs(BrainMemoryUtils.find(ownerId, ownerType, shared)));
	}

	@Test
	void mergingTopicsMovesMemoryLinksAndUndoSplitsThemAgain() throws Exception {
		topic("t-a", "Acme renewal");
		topic("t-b", "Acme");
		String onSource = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Renewal is due in March",
				"about", List.of(Map.of("type", "topic", "id", "t-a")))).get("id");
		String onBoth = (String) BrainMemoryUtils.saveMemory(user, Map.of("text", "Legal reviews every Acme contract",
				"about", List.of(Map.of("type", "topic", "id", "t-a"), Map.of("type", "topic", "id", "t-b"))))
				.get("id");

		String changeId = (String) BrainTopicUtils.mergeTopics(user, "t-a", "t-b").get("changeId");
		assertEquals(List.of(new Ref("topic", "t-b")), BrainMemoryUtils.find(ownerId, ownerType, onSource).about());
		assertEquals(List.of(new Ref("topic", "t-b")), BrainMemoryUtils.find(ownerId, ownerType, onBoth).about());

		BrainTopicChangeUtils.undo(user, changeId);
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

		List<String> ids = ids(BrainMemoryRecall.recall(ownerId, ownerType, "th-1", 4000));
		assertEquals(Set.of(preference, aboutThread, aboutPriya, aboutAcme, aboutAccount), Set.copyOf(ids));
		assertEquals(List.of(preference, aboutThread, aboutPriya), ids.subList(0, 3));

		// a channel rule keeps the whole thread out: its own memories and its people go, its topics stay
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, CHANNEL, CREATED_AT) VALUES (?, ?, 'r1', "
				+ "'exclude_channel', 'outlook', CURRENT_TIMESTAMP)", ownerId, ownerType);
		assertEquals(Set.of(preference, aboutAcme, aboutAccount),
				Set.copyOf(ids(BrainMemoryRecall.recall(ownerId, ownerType, "th-1", 4000))));

		// a /new session only gets what applies everywhere
		assertEquals(List.of(preference), ids(BrainMemoryRecall.recall(ownerId, ownerType, "session:abc", 4000)));

		Map<String, Object> shown = BrainMemoryRecall.recallMemories(user, "th-1");
		assertEquals(true, shown.get("enabled"));
		assertEquals(3, ((List<?>) shown.get("items")).size());
		assertTrue(((String) shown.get("prompt")).contains("[m:" + preference + "] Sign emails as Rob"));
		assertTrue(BrainMemoryRecall.promptBlock(user, "th-1").contains("### What you remember for this thread"));

		// memory off: no block for the prompt and nothing recalled
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, MEMORY_USE) "
				+ "VALUES (?, ?, 85, 40, 1, FALSE)", ownerId, ownerType);
		assertNull(BrainMemoryRecall.promptBlock(user, "th-1"));
		assertEquals(false, BrainMemoryRecall.recallMemories(user, "th-1").get("enabled"));
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
