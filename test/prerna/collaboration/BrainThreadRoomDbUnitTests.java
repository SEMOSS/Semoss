package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.collaboration.BrainThreadMessages.Shown;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/** A chat's email threads against an in-memory H2 Collaboration schema: seeding, links, new counts, lookups. */
class BrainThreadRoomDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	// the user's open rooms in the inference database, by id
	private final Map<String, Map<String, Object>> rooms = new LinkedHashMap<>();
	private User user;
	private String userId;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_thread_room_" + UUID.randomUUID());
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
		// a new owner per test: older chats are seeded once per owner on a server
		userId = "owner-" + UUID.randomUUID();
		AccessToken token = new AccessToken();
		token.setId(userId);
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(eq(userId), anyList())).thenAnswer(call -> {
			List<Map<String, Object>> found = new ArrayList<>();
			for (Object roomId : (List<?>) call.getArgument(1)) {
				if (rooms.containsKey(roomId)) {
					found.add(rooms.get(roomId));
				}
			}
			return found;
		});
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomOptions(userId, CollaborationUtils.COLLABORATION_PROJECT_ID))
				.thenAnswer(call -> new ArrayList<>(rooms.values()));

		thread("th-1", "2026-10-01 12:00:00");
		email("th-1", "g1", "2026-10-01 10:00:00", "p-kyle");
		email("th-1", "g2", "2026-10-01 11:00:00", "p-dana");
		email("th-1", "g3", "2026-10-01 12:00:00", "p-kyle");
		// opened from th-1 when it had g1 and g2
		room("room-1", "2026-10-02 09:00:00", source("th-1", "g1", "g2"));
	}

	@AfterEach
	void tearDown() throws Exception {
		logs.close();
		registry.close();
		connection.close();
	}

	@Test
	void aChatOpenedFromAThreadIncludesItUpToWhatItsImportHeld() throws Exception {
		List<Map<String, Object>> threads = threads(BrainThreadRoomUtils.listRoomThreads(user, "room-1", true));
		assertEquals(1, threads.size());
		Map<String, Object> thread = threads.get(0);
		assertEquals("th-1", thread.get("threadId"));
		assertEquals(BrainThreadRoomUtils.THREAD, thread.get("origin"));
		assertEquals(false, thread.get("added"));
		assertEquals(1, thread.get("newCount"));
		assertEquals(List.of("g3", "g2", "g1"), thread.get("emailIds"));
		assertEquals("g2", value("SELECT SEEN_REF FROM BRAIN_THREAD_ROOM"));

		BrainThreadRoomUtils.listRoomThreads(user, "room-1", false);
		assertEquals(1, count("BRAIN_THREAD_ROOM"));
	}

	@Test
	void hiddenAndExcludedEmailsAreNotNewAndCannotBeAnswered() throws Exception {
		sql("UPDATE BRAIN_MESSAGE SET DECISION = ? WHERE GRAPH_ID = 'g3'", BrainRulesGate.NEVER);
		// from someone the owner left out of this thread
		email("th-1", "g4", "2026-10-01 13:00:00", "p-left-out");
		sql("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED) "
				+ "VALUES (?, ?, 'th-1', 'p-left-out', FALSE)", ownerId, ownerType);
		// from someone a rule made after the import keeps out everywhere
		email("th-1", "g5", "2026-10-01 14:00:00", "p-blocked");
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, PERSON_ID, CREATED_AT) "
				+ "VALUES (?, ?, 'r-1', 'exclude_everywhere', 'p-blocked', CURRENT_TIMESTAMP)", ownerId, ownerType);
		email("th-1", "g6", "2026-10-01 15:00:00", "p-kyle");

		Map<String, Object> thread = threads(BrainThreadRoomUtils.listRoomThreads(user, "room-1", true)).get(0);
		assertEquals(1, thread.get("newCount"));
		assertEquals(List.of("g6", "g2", "g1"), thread.get("emailIds"));
	}

	@Test
	void theOwnerAddsAndRemovesThreads() throws Exception {
		thread("th-2", "2026-10-03 08:00:00");
		email("th-2", "h1", "2026-10-03 07:00:00", "p-dana");
		email("th-2", "h2", "2026-10-03 08:00:00", "p-kyle");

		List<Map<String, Object>> threads = threads(BrainThreadRoomUtils.linkRoomThread(user, "room-1", "th-2", false));
		assertEquals(List.of("th-2", "th-1"), ids(threads));
		assertEquals(BrainProfileUtils.YOU, threads.get(0).get("origin"));
		assertEquals(true, threads.get(0).get("added"));
		assertEquals(2, threads.get(0).get("newCount"));

		// the thread the chat was opened from stays out once removed
		assertEquals(List.of("th-2"), ids(threads(BrainThreadRoomUtils.linkRoomThread(user, "room-1", "th-1", true))));
		assertEquals(List.of("th-2"), ids(threads(BrainThreadRoomUtils.listRoomThreads(user, "room-1", false))));

		// added back, it picks up where the chat left it
		threads = threads(BrainThreadRoomUtils.linkRoomThread(user, "room-1", "th-1", false));
		assertEquals(List.of("th-2", "th-1"), ids(threads));
		assertEquals(BrainThreadRoomUtils.THREAD, threads.get(1).get("origin"));
		assertEquals(1, threads.get(1).get("newCount"));
		assertEquals(2, count("BRAIN_THREAD_ROOM"));
	}

	@Test
	void removingTheOpenedThreadBeforeAnyReadSticks() throws Exception {
		assertTrue(threads(BrainThreadRoomUtils.linkRoomThread(user, "room-1", "th-1", true)).isEmpty());
		assertTrue(threads(BrainThreadRoomUtils.listRoomThreads(user, "room-1", false)).isEmpty());
		assertEquals(1, count("BRAIN_THREAD_ROOM"));
	}

	@Test
	void aChatOpenedFromAnOutlookEmailFollowsNoThread() throws Exception {
		room("room-2", "2026-10-02 09:00:00", source("outlook-abc", "x1"));
		assertTrue(threads(BrainThreadRoomUtils.listRoomThreads(user, "room-2", false)).isEmpty());
		assertEquals(0, count("BRAIN_THREAD_ROOM"));
	}

	@Test
	void onlyTheOwnersOwnChatsAndThreadsAreUsed() {
		room("room-3", "2026-10-02 09:00:00", delegated(source("th-1", "g1")));
		for (String roomId : List.of("room-3", "someone-elses-room")) {
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> BrainThreadRoomUtils.linkRoomThread(user, roomId, "th-1", false));
			assertEquals("Chat not found", error.getMessage());
		}
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> BrainThreadRoomUtils.linkRoomThread(user, "room-1", "th-missing", false));
		assertEquals("Thread not found", error.getMessage());
	}

	@Test
	void aThreadFindsItsChatsIncludingOnesOpenedBeforeLinksExisted() throws Exception {
		room("room-2", "2026-10-02 10:00:00", source("th-1", "g1"));
		room("room-3", "2026-10-02 11:00:00", delegated(source("th-1", "g1")));
		// a home chat the owner adds th-1 to
		room("room-4", "2026-10-02 08:00:00", Map.of());
		BrainThreadRoomUtils.linkRoomThread(user, "room-4", "th-1", false);

		List<Map<String, Object>> chats = items(BrainThreadRoomUtils.listThreadRooms(user, "th-1"));
		assertEquals(List.of("room-2", "room-1", "room-4"), chats.stream().map(chat -> chat.get("roomId")).toList());
		assertEquals(List.of("thread", "thread", "you"), chats.stream().map(chat -> chat.get("origin")).toList());

		// a closed chat drops out, and older chats are seeded only once
		rooms.remove("room-2");
		assertEquals(2, items(BrainThreadRoomUtils.listThreadRooms(user, "th-1")).size());
		logs.verify(() -> ModelInferenceLogsUtils.getActiveRoomOptions(userId,
				CollaborationUtils.COLLABORATION_PROJECT_ID), times(1));
	}

	@Test
	void newCountComparesTimesNotText() {
		// Instant.toString drops a zero fraction, so as text "12:00:00Z" sorts after "12:00:00.500Z"
		List<Shown> shown = List.of(new Shown("g3", "2026-10-01T12:00:00.500Z"),
				new Shown("g2", "2026-10-01T12:00:00Z"));
		assertEquals(1, BrainThreadRoomUtils.newCount(shown, "2026-10-01T12:00:00Z"));
		assertEquals(2, BrainThreadRoomUtils.newCount(shown, null));
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> threads(Map<String, Object> list) {
		return (List<Map<String, Object>>) list.get("threads");
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> items(Map<String, Object> page) {
		return (List<Map<String, Object>>) page.get("items");
	}

	private static List<Object> ids(List<Map<String, Object>> threads) {
		return threads.stream().map(thread -> thread.get("threadId")).toList();
	}

	// room options of a chat opened from a thread whose import held these emails
	private static Map<String, Object> source(String threadId, String... graphIds) {
		List<Map<String, Object>> messages = new ArrayList<>();
		for (String graphId : graphIds) {
			messages.add(Map.of("id", graphId, "at", "2026-10-01T10:00:00Z"));
		}
		return Map.of(CollaborationUtils.ROOM_OPTION_SOURCE,
				Map.of("version", 1, "threadId", threadId, "messages", messages));
	}

	private static Map<String, Object> delegated(Map<String, Object> options) {
		Map<String, Object> out = new HashMap<>(options);
		out.put(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "act-1");
		return out;
	}

	private void room(String roomId, String updatedAt, Map<String, Object> options) {
		Map<String, Object> row = new HashMap<>();
		row.put("ROOM_ID", roomId);
		row.put("ROOM_NAME", "Chat " + roomId);
		row.put("PROJECT_ID", CollaborationUtils.COLLABORATION_PROJECT_ID);
		row.put("OPTIONS", CollaborationDbUtils.toJson(options));
		row.put("UPDATED_AT", Timestamp.valueOf(updatedAt));
		rooms.put(roomId, row);
	}

	private void thread(String threadId, String lastMessageAt) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, SUBJECT, MESSAGE_COUNT, "
				+ "LAST_MESSAGE_AT, CREATED_AT) VALUES (?, ?, ?, ?, 'email', ?, 0, ?, CURRENT_TIMESTAMP)", ownerId,
				ownerType, threadId, "email:conv-" + threadId, "About " + threadId, Timestamp.valueOf(lastMessageAt));
	}

	private void email(String threadId, String graphId, String receivedAt, String personId) throws Exception {
		sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, GRAPH_ID, SENDER_PERSON_ID, "
				+ "FOLDER, RECEIVED_AT, DECISION) VALUES (?, ?, ?, ?, ?, ?, 'inbox', ?, 'include')", ownerId,
				ownerType, "key-" + graphId, threadId, graphId, personId, Timestamp.valueOf(receivedAt));
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

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}
}
