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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Re-sorting after a topic changes, against an in-memory H2 Collaboration schema: which conversations a preview
 * scores, which scores become rows, and what move, also_link and reject leave behind.
 */
class BrainTopicMatchesDbUnitTests {

	private static final String T = "topic-new";
	private static final String A = "topic-a";
	private static final String B = "topic-b";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_matches_" + UUID.randomUUID());
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
		token.setId("owner-" + UUID.randomUUID());
		token.setName("Owner");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		for (String topic : List.of(T, A, B)) {
			sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) VALUES (?, ?, ?, ?, 'active')",
					ownerId, ownerType, topic, topic);
		}
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	// ---- preview ----

	@Test
	void thePreviewScoresOnlyConversationsTheTopicCouldStillGain() throws Exception {
		thread("newest", "2026-10-20 10:00:00", false, false);
		thread("muted", "2026-10-19 10:00:00", true, false);
		thread("automated", "2026-10-18 10:00:00", false, true);
		thread("already", "2026-10-17 10:00:00", false, false);
		link("already", T, "confirmed", true, "jev-v1", "brain");
		thread("refused", "2026-10-16 10:00:00", false, false);
		BrainThreadTopicDecisions.remember(ownerId, ownerType, "refused", T, true);
		thread("older", "2026-10-15 10:00:00", false, false);
		link("older", A, "you", true, null, "you");

		assertEquals(List.of("newest", "older"), BrainTopicMatches.candidates(ownerId, ownerType, T, 200));
		assertEquals(List.of("newest"), BrainTopicMatches.candidates(ownerId, ownerType, T, 1));
	}

	@Test
	void aClearBestFitOnBrainGuessesSuggestsAMove() {
		Map<String, Object> row = BrainTopicMatches.match(T, scores(T, 0.9, A, 0.05, "other", 0.05), Set.of(),
				List.of(guess(A, true)), 85, 40);
		assertEquals(90, row.get("confidence"));
		assertEquals(BrainTopicMatches.BEST, row.get("fit"));
		assertEquals(List.of(BrainTopicMatches.MOVE, BrainTopicMatches.ALSO_LINK), row.get("ops"));
		assertEquals(BrainTopicMatches.MOVE, row.get("op"));
	}

	@Test
	void theOwnersOwnLinkOnlyEverGetsAnAdditionalLink() {
		Map<String, Object> row = BrainTopicMatches.match(T, scores(T, 0.9, A, 0.05, "other", 0.05), Set.of(),
				List.of(new BrainTopicMatches.Link(A, "you", true, true)), 85, 40);
		assertEquals(List.of(BrainTopicMatches.ALSO_LINK), row.get("ops"));
		assertEquals(BrainTopicMatches.ALSO_LINK, row.get("op"));
		Map<String, Object> unlinked = BrainTopicMatches.match(T, scores(T, 0.9, "other", 0.1), Set.of(), List.of(),
				85, 40);
		assertEquals(List.of(BrainTopicMatches.ALSO_LINK), unlinked.get("ops"));
	}

	@Test
	void aConversationSplitBetweenItsTopicAndTheNewOneIsSharedNotMoved() {
		Map<String, Object> row = BrainTopicMatches.match(T, scores(A, 0.5, T, 0.45, "other", 0.05), Set.of(),
				List.of(guess(A, true)), 85, 40);
		assertEquals(BrainTopicMatches.SHARED, row.get("fit"));
		assertEquals(BrainTopicMatches.ALSO_LINK, row.get("op"));
		// the same split with no link to the rival is too unsure to show
		assertNull(BrainTopicMatches.match(T, scores(A, 0.5, T, 0.45, "other", 0.05), Set.of(), List.of(), 85, 40));
		// a best fit under the ask band is left alone, as the classifier would
		assertNull(BrainTopicMatches.match(T, scores(T, 0.55, "other", 0.45), Set.of(), List.of(), 85, 40));
	}

	@Test
	void aTopicTheOwnerTurnedDownIsNoRival() {
		Map<String, Double> scores = scores(A, 0.45, T, 0.42, B, 0.13);
		assertNull(BrainTopicMatches.match(T, scores, Set.of(), List.of(guess(B, true)), 85, 40));
		Map<String, Object> row = BrainTopicMatches.match(T, scores, Set.of(A), List.of(guess(B, true)), 85, 40);
		assertEquals(BrainTopicMatches.SHARED, row.get("fit"));
		// turned down or filed under the topic while the preview ran
		assertNull(BrainTopicMatches.match(T, scores(T, 0.9, "other", 0.1), Set.of(T), List.of(), 85, 40));
		assertNull(BrainTopicMatches.match(T, scores(T, 0.9, "other", 0.1), Set.of(), List.of(guess(T, true)), 85, 40));
	}

	@Test
	void dryRunResultsBecomeRowsBestFirst() throws Exception {
		thread("fits", "2026-10-20 10:00:00", false, false);
		thread("fits-less", "2026-10-19 10:00:00", false, false);
		thread("failed", "2026-10-18 10:00:00", false, false);
		thread("automated", "2026-10-17 10:00:00", false, false);
		thread("elsewhere", "2026-10-16 10:00:00", false, false);
		Map<String, Object> summary = Map.of("results", List.of(
				result("fits-less", "needs_me", scores(T, 0.75, "other", 0.25), null),
				result("fits", "fyi", scores(T, 0.95, "other", 0.05), null),
				result("failed", null, null, "model timed out"),
				result("automated", "automated", scores(T, 0.95, "other", 0.05), null),
				result("elsewhere", "fyi", scores(A, 0.9, T, 0.1), null)));

		List<Map<String, Object>> rows = BrainTopicMatches.matches(ownerId, ownerType, T, summary);
		assertEquals(List.of("fits", "fits-less"), rows.stream().map(row -> row.get("threadId")).toList());
		assertEquals("Subject fits", rows.get(0).get("subject"));
		assertEquals(95, rows.get(0).get("confidence"));
	}

	// ---- apply ----

	@Test
	void aMoveReplacesBrainsGuessesAndKeepsWorkAndChats() throws Exception {
		thread("x", "2026-10-20 10:00:00", false, false);
		link("x", A, "confirmed", true, "jev-v1", "brain");
		link("x", B, "suggested", false, "jev-v1", "brain");
		sql("INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, ORIGIN, STATUS, SNOOZE_UNTIL, "
				+ "ASSIGNEE_PERSON_ID, ROOM_ID, LINK_TOPIC_ID) VALUES (?, ?, 'card', 'x', 'brain', 'snoozed', ?, "
				+ "'person-1', 'room-1', ?)", ownerId, ownerType, Timestamp.valueOf("2026-11-01 09:00:00"), A);
		sql("INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, ORIGIN, STATUS, LINK_TOPIC_ID) "
				+ "VALUES (?, ?, 'owner-topic', 'x', 'brain', 'open', ?)", ownerId, ownerType, A);
		sql("INSERT INTO WORK_ITEM_HISTORY (OWNER_ID, OWNER_TYPE, HISTORY_ID, ITEM_ID, CHANGED_BY, FIELD, OLD_VALUE, "
				+ "NEW_VALUE) VALUES (?, ?, 'h1', 'owner-topic', 'you', 'linkTopicId', NULL, ?)", ownerId, ownerType, A);
		sql("INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, ORIGIN, STATUS) "
				+ "VALUES (?, ?, 'owner-card', 'x', 'you', 'open')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_THREAD_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, THREAD_ID, STATE) "
				+ "VALUES (?, ?, 'room-1', 'x', 'linked')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'room-1', ?, 'linked', 'you')", ownerId, ownerType, A);
		sql("INSERT INTO BRAIN_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, KIND, REF_TYPE, REF_ID, STATUS) "
				+ "VALUES (?, ?, 'choice', 'topic_choice', 'thread', 'x', 'open')", ownerId, ownerType);

		Map<String, Object> result = apply(change("x", BrainTopicMatches.MOVE));

		assertEquals(1, result.get("applied"));
		assertEquals(List.of(List.of(T, "you", true)), links("x"));
		assertEquals(Set.of(A, B), rejections("x"));
		assertEquals(T, value("SELECT LINK_TOPIC_ID FROM WORK_ITEM WHERE ITEM_ID = 'card'"));
		assertEquals("snoozed", value("SELECT STATUS FROM WORK_ITEM WHERE ITEM_ID = 'card'"));
		assertEquals("person-1", value("SELECT ASSIGNEE_PERSON_ID FROM WORK_ITEM WHERE ITEM_ID = 'card'"));
		assertEquals("room-1", value("SELECT ROOM_ID FROM WORK_ITEM WHERE ITEM_ID = 'card'"));
		assertEquals(A, value("SELECT LINK_TOPIC_ID FROM WORK_ITEM WHERE ITEM_ID = 'owner-topic'"));
		assertNull(value("SELECT LINK_TOPIC_ID FROM WORK_ITEM WHERE ITEM_ID = 'owner-card'"));
		assertEquals("linked", value("SELECT STATE FROM BRAIN_THREAD_ROOM WHERE ROOM_ID = 'room-1'"));
		assertEquals(A, value("SELECT TOPIC_ID FROM BRAIN_TOPIC_ROOM WHERE ROOM_ID = 'room-1'"));
		assertEquals("dismissed", value("SELECT STATUS FROM BRAIN_REVIEW WHERE REVIEW_ID = 'choice'"));

		// applying the same choice again changes nothing
		assertEquals(1, apply(change("x", BrainTopicMatches.MOVE)).get("applied"));
		assertEquals(List.of(List.of(T, "you", true)), links("x"));
	}

	@Test
	void aMoveNeverRemovesALinkTheOwnerOrAFolderMade() throws Exception {
		thread("mine", "2026-10-20 10:00:00", false, false);
		link("mine", A, "you", true, null, "you");
		thread("folder", "2026-10-19 10:00:00", false, false);
		link("folder", A, "confirmed", true, BrainMailFolders.FILED, "brain");

		Map<String, Object> result = apply(change("mine", BrainTopicMatches.MOVE),
				change("folder", BrainTopicMatches.MOVE));

		assertEquals(0, result.get("applied"));
		for (Object row : (List<?>) result.get("results")) {
			assertEquals("skipped", ((Map<?, ?>) row).get("status"));
			assertTrue(String.valueOf(((Map<?, ?>) row).get("reason")).contains(BrainTopicMatches.ALSO_LINK));
		}
		assertEquals(List.of(List.of(A, "you", true)), links("mine"));
		assertEquals(List.of(List.of(A, "confirmed", true)), links("folder"));
		assertEquals(Set.of(), rejections("mine"));
	}

	@Test
	void anAdditionalLinkLeavesTheOwnersPrimaryInPlace() throws Exception {
		thread("mine", "2026-10-20 10:00:00", false, false);
		link("mine", A, "you", true, null, "you");
		thread("bare", "2026-10-19 10:00:00", false, false);

		apply(change("mine", BrainTopicMatches.ALSO_LINK), change("bare", BrainTopicMatches.ALSO_LINK));

		assertEquals(List.of(List.of(A, "you", true), List.of(T, "you", false)), links("mine"));
		// the first link of a conversation is its primary
		assertEquals(List.of(List.of(T, "you", true)), links("bare"));
	}

	@Test
	void aRejectionIsRememberedAndOnlyDropsABrainGuess() throws Exception {
		thread("guessed", "2026-10-20 10:00:00", false, false);
		link("guessed", T, "confirmed", true, "jev-v1", "brain");
		link("guessed", B, "confirmed", false, "jev-v1", "brain");
		thread("plain", "2026-10-19 10:00:00", false, false);
		thread("mine", "2026-10-18 10:00:00", false, false);
		link("mine", T, "you", true, null, "you");

		Map<String, Object> result = apply(change("guessed", BrainTopicMatches.REJECT),
				change("plain", BrainTopicMatches.REJECT), change("mine", BrainTopicMatches.REJECT));

		assertEquals(2, result.get("applied"));
		assertEquals(List.of(List.of(B, "confirmed", true)), links("guessed"));
		assertEquals(Set.of(T), rejections("guessed"));
		assertEquals(Set.of(T), rejections("plain"));
		assertEquals(List.of(List.of(T, "you", true)), links("mine"));
		assertEquals(Set.of(), rejections("mine"));
		// and the next preview leaves them out
		assertEquals(List.of(), BrainTopicMatches.candidates(ownerId, ownerType, T, 200));
	}

	@Test
	void badChangesFailBeforeAnythingIsWrittenAndGoneThreadsAreSkipped() throws Exception {
		thread("x", "2026-10-20 10:00:00", false, false);
		assertThrows(IllegalArgumentException.class,
				() -> apply(change("x", BrainTopicMatches.ALSO_LINK), change("x", "delete")));
		assertEquals(List.of(), links("x"));

		Map<String, Object> result = apply(change("gone", BrainTopicMatches.ALSO_LINK));
		assertEquals("skipped", ((Map<?, ?>) ((List<?>) result.get("results")).get(0)).get("status"));

		sql("UPDATE BRAIN_TOPIC SET STATUS = 'archived' WHERE TOPIC_ID = ?", T);
		assertThrows(IllegalArgumentException.class, () -> apply(change("x", BrainTopicMatches.ALSO_LINK)));
	}

	// ---- helpers ----

	@SafeVarargs
	private Map<String, Object> apply(Map<String, Object>... changes) {
		return BrainTopicMatches.apply(user, T, List.of(changes));
	}

	private static Map<String, Object> change(String threadId, String op) {
		Map<String, Object> change = new LinkedHashMap<>();
		change.put("threadId", threadId);
		change.put("op", op);
		return change;
	}

	private static BrainTopicMatches.Link guess(String topicId, boolean primary) {
		return new BrainTopicMatches.Link(topicId, "confirmed", primary, false);
	}

	private static Map<String, Double> scores(Object... pairs) {
		Map<String, Double> scores = new LinkedHashMap<>();
		for (int i = 0; i < pairs.length; i += 2) {
			scores.put((String) pairs[i], (Double) pairs[i + 1]);
		}
		return scores;
	}

	private static Map<String, Object> result(String threadId, String work, Map<String, Double> topics, String error) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("work", work);
		result.put("scores", topics == null ? null : Map.of("topics", topics));
		result.put("error", error);
		return result;
	}

	private void thread(String threadId, String lastMessageAt, boolean muted, boolean automated) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE, SUBJECT, MUTED, AUTOMATED, "
				+ "LAST_MESSAGE_AT) VALUES (?, ?, ?, 'email', ?, ?, ?, ?)", ownerId, ownerType, threadId,
				"Subject " + threadId, muted, automated, Timestamp.valueOf(lastMessageAt));
	}

	private void link(String threadId, String topicId, String source, boolean primary, String version,
			String changedBy) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, "
				+ "IS_PRIMARY, CLASSIFIER_VERSION, CHANGED_BY) VALUES (?, ?, ?, ?, ?, 90, ?, ?, ?)", ownerId,
				ownerType, threadId, topicId, source, primary, version, changedBy);
	}

	// [topic, source, primary] per link, primary first
	private List<List<Object>> links(String threadId) throws Exception {
		List<List<Object>> links = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement("SELECT TOPIC_ID, SOURCE, IS_PRIMARY "
				+ "FROM BRAIN_THREAD_TOPIC WHERE THREAD_ID = ? ORDER BY IS_PRIMARY DESC, TOPIC_ID")) {
			statement.setString(1, threadId);
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					links.add(List.of(rs.getString(1), rs.getString(2), rs.getBoolean(3)));
				}
			}
		}
		return links;
	}

	private Set<String> rejections(String threadId) {
		return BrainThreadTopicDecisions.rejected(ownerId, ownerType, threadId);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}
}
