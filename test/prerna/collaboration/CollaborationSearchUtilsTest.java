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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/** Runs real JDBC queries against a deterministic fake Collaboration database. */
class CollaborationSearchUtilsTest {
	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<User> users;
	private final User user = mock(User.class);

	@BeforeEach
	void setup() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:search-" + UUID.randomUUID());
		var queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		try (var statement = connection.createStatement()) {
			for (var table : new CollaborationOwlCreator(queryUtil).getDBSchema()) {
				String columns = String.join(", ", table.getValue1().stream().map(c -> c.getValue0() + " " + c.getValue1()).toList());
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + columns + ")");
			}
		}
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString())).thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		users = mockStatic(User.class);
		users.when(() -> User.getPrimaryUserIdAndTypePair(user)).thenReturn(Pair.with("owner", "NATIVE"));
		for (int i = 1; i <= 40; i++) {
			String name = String.format(Locale.ROOT, "SearchCase %02d", i);
			seed("thread", "t-" + i, name, "owner", "NATIVE");
			seed("person", "p-" + i, name, "owner", "NATIVE");
			seed("topic", "g-" + i, name, "owner", "NATIVE");
		}
		seed("thread", "other-owner", "SearchCase private", "someone-else", "NATIVE");
		seed("person", "other-type", "SearchCase private", "owner", "LDAP");
	}

	@AfterEach
	void teardown() throws Exception {
		if (users != null) users.close();
		if (registry != null) registry.close();
		if (connection != null) connection.close();
	}

	private void seed(String kind, String id, String name, String owner, String ownerType) throws Exception {
		String table = switch (kind) { case "thread" -> "BRAIN_THREAD"; case "person" -> "BRAIN_PERSON"; default -> "BRAIN_TOPIC"; };
		String idColumn = switch (kind) { case "thread" -> "THREAD_ID"; case "person" -> "PERSON_ID"; default -> "TOPIC_ID"; };
		String nameColumn = switch (kind) { case "thread" -> "SUBJECT"; case "person" -> "DISPLAY_NAME"; default -> "NAME"; };
		try (PreparedStatement ps = connection.prepareStatement("INSERT INTO " + table + " (OWNER_ID, OWNER_TYPE, " + idColumn + ", " + nameColumn + ") VALUES (?, ?, ?, ?)")) {
			ps.setString(1, owner); ps.setString(2, ownerType); ps.setString(3, id); ps.setString(4, name); ps.executeUpdate();
		}
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> items(Map<String, Object> page) { return (List<Map<String, Object>>) page.get("items"); }

	@Test
	void normalizedPartialMatchesCoverAllTypesAndPagesWithoutOwnerLeaks() {
		var first = CollaborationSearchUtils.search(user, "  sEaRcHcA  ", 30, 0);
		assertEquals(120, first.get("total"));
		assertEquals(30, items(first).size());
		assertEquals(new HashSet<>(List.of("thread", "person", "topic")), new HashSet<>(items(first).stream().map(row -> row.get("kind")).toList()));
		var keys = new HashSet<String>();
		for (int offset = 0; offset < 120; offset += 30) {
			for (var row : items(CollaborationSearchUtils.search(user, "searchcase", 30, offset))) {
				assertTrue(keys.add(row.get("kind") + ":" + row.get("id")), "Duplicate across pages");
				assertFalse(String.valueOf(row.get("id")).startsWith("other-"));
			}
		}
		assertEquals(120, keys.size());
		assertTrue(items(CollaborationSearchUtils.search(user, "searchcase", 30, 120)).isEmpty());
	}

	@Test
	void literalCharactersEmailAndEmptyQueries() throws Exception {
		seed("thread", "literal", "Budget 100%_! O'Brien", "owner", "NATIVE");
		for (String query : List.of("%", "_", "!", "O'BRIEN")) {
			assertEquals("literal", items(CollaborationSearchUtils.search(user, query, 30, 0)).get(0).get("id"));
			assertEquals(1, CollaborationSearchUtils.search(user, query, 30, 0).get("total"));
		}
		try (var statement = connection.createStatement()) {
			statement.execute("UPDATE BRAIN_PERSON SET EMAIL_NORM = 'unique@search.example' WHERE PERSON_ID = 'p-1'");
		}
		assertEquals("p-1", items(CollaborationSearchUtils.search(user, " UNIQUE@SEARCH ", 30, 0)).get(0).get("id"));
		for (String query : List.of(" ", "missing-result")) assertEquals(0, CollaborationSearchUtils.search(user, query, 30, 0).get("total"));
		assertEquals(0, CollaborationSearchUtils.search(user, null, 30, 0).get("total"));
	}

	@Test
	void recordsBeyondStartupLimitsRemainSearchable() throws Exception {
		for (int i = 41; i <= 5001; i++) {
			seed("thread", "t-" + i, i == 5001 ? "Beyond startup target" : "Filler " + i, "owner", "NATIVE");
			seed("person", "p-" + i, i == 5001 ? "Beyond startup target" : "Filler " + i, "owner", "NATIVE");
		}
		for (int i = 41; i <= 1001; i++) seed("topic", "g-" + i, i == 1001 ? "Beyond startup target" : "Filler " + i, "owner", "NATIVE");
		assertEquals(3, CollaborationSearchUtils.search(user, "beyond startup", 30, 0).get("total"));
	}

	@Test
	void detailsReadOnlyOwnedRecordsAndThreadMetadata() {
		assertEquals("t-40", items(Map.of("items", CollaborationSearchUtils.getRecord(user, "thread", "t-40").get("threads"))).get(0).get("id"));
		assertEquals("p-40", items(Map.of("items", CollaborationSearchUtils.getRecord(user, "person", "p-40").get("people"))).get(0).get("id"));
		assertEquals("g-40", items(Map.of("items", CollaborationSearchUtils.getRecord(user, "topic", "g-40").get("topics"))).get(0).get("id"));
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.getRecord(user, "thread", "other-owner"));
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.getRecord(user, "person", "other-type"));
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.getRecord(user, "thread", "deleted"));
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.getRecord(user, "unknown", "t-1"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void threadDetailsIncludeSavedContextAndSourceIdentity() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("UPDATE BRAIN_THREAD SET SOURCE = 'teams', THREAD_KEY = 'teams:chat-native-id', GOAL = 'Saved goal' WHERE THREAD_ID = 't-1'");
			statement.execute("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, IS_PRIMARY) VALUES ('owner', 'NATIVE', 't-1', 'g-1', 'you', TRUE)");
			statement.execute("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, INCLUDED, ROLES_JSON) VALUES ('owner', 'NATIVE', 't-1', 'p-1', TRUE, '[\"member\"]')");
			statement.execute("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, THREAD_ID, MESSAGE_KEY, GRAPH_ID) VALUES ('owner', 'NATIVE', 't-1', 'msg', 'native-message')");
		}
		var record = CollaborationSearchUtils.getRecord(user, "thread", "t-1");
		var thread = items(Map.of("items", record.get("threads"))).get(0);
		assertEquals("chat-native-id", thread.get("conversationId"));
		assertEquals("native-message", thread.get("latestMessageId"));
		assertEquals("g-1", items(Map.of("items", record.get("topics"))).get(0).get("id"));
		assertEquals("p-1", items(Map.of("items", record.get("people"))).get(0).get("id"));
		assertEquals("Saved goal", items((Map<String, Object>) record.get("workspaces")).get(0).get("goal"));
	}

	@Test
	void generatedLocalMailboxCanBeReadByTheRealFixtureSource() throws Exception {
		String path = System.getProperty("collaborationSearchFixture");
		org.junit.jupiter.api.Assumptions.assumeTrue(path != null, "Optional generated local fixture smoke test");
		var source = BrainFixtureHeaderSource.of(path);
		var since = java.time.Instant.now().minus(java.time.Duration.ofDays(7));
		var inbox = source.list(user, BrainMailHeaderSource.INBOX, since, 5000);
		var sent = source.list(user, BrainMailHeaderSource.SENT, since, 5000);
		assertTrue(inbox.size() > 30);
		assertFalse(sent.isEmpty(), "Sent folder must use the importer's sentitems suffix");
		assertEquals("Alex Morgan", source.me(user).get("displayName"));
		var subjects = java.util.stream.Stream.concat(inbox.stream(), sent.stream()).map(row -> String.valueOf(row.get("subject"))).toList();
		assertTrue(subjects.stream().anyMatch(subject -> subject.contains("100%_! O'Brien")));
		assertTrue(subjects.stream().anyMatch(subject -> subject.contains("\u00c9lodie")));
	}

	@Test
	void validatesPagingAndAuthentication() {
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.search(null, "search", 30, 0));
		for (int limit : List.of(-1, 0, 101)) assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.search(user, "search", limit, 0));
		assertThrows(IllegalArgumentException.class, () -> CollaborationSearchUtils.search(user, "search", 30, -1));
	}
}
