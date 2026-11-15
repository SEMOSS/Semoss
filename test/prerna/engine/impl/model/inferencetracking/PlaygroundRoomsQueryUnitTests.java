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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomFilter;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomGroup;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.PlaygroundRoomSort;
import prerna.util.QueryExecutionUtility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Runs the playground room list query on an in-memory H2 database: grouping,
 * pinned rooms first, sorting, paging, search and the agent name.
 */
class PlaygroundRoomsQueryUnitTests {

	private static final String USER = "user-1";
	private static final String PROJECT = "playground";

	private Connection connection;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:playground_rooms_" + UUID.randomUUID());
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE ROOM (ROOM_ID VARCHAR(50), ROOM_NAME VARCHAR(255), USER_ID VARCHAR(255), "
					+ "PROJECT_ID VARCHAR(255), IS_ACTIVE BOOLEAN, DATE_CREATED TIMESTAMP, UPDATED_AT TIMESTAMP, "
					+ "PINNED BOOLEAN, WORKSPACE_ID VARCHAR(255), PARENT_ROOM_ID VARCHAR(50), MESSAGES CLOB)");
			// the real column is a BLOB, which message search reads as text
			statement.execute("CREATE TABLE MESSAGE (MESSAGE_ID VARCHAR(50), ROOM_ID VARCHAR(50), MESSAGE_DATA BLOB)");
			statement.execute("CREATE TABLE WORKSPACE (WORKSPACE_ID VARCHAR(255), NAME VARCHAR(255))");
			statement.execute("INSERT INTO WORKSPACE VALUES ('ws-b', 'Beta agent'), ('ws-a', 'alpha agent')");
		}
		// id, name, created day, updated day (null = never), pinned, workspace
		room("r1", "Delta", 1, 9, false, "ws-b");
		room("r2", "alpha", 2, null, true, "ws-a");
		room("r3", "Charlie", 3, 4, false, null);
		room("r4", "bravo", 4, 5, true, null);
		room("r5", "Echo", 5, 6, false, "ws-a");

		// never listed: another user's, inactive, without messages, another project,
		// unnamed, and a subagent's child room
		insertRoom("x-user", "Other", "user-2", PROJECT, true, 1, null, false, null, null, true);
		insertRoom("x-inactive", "Gone", USER, PROJECT, false, 1, null, false, null, null, true);
		insertRoom("x-empty", "Empty", USER, PROJECT, true, 1, null, false, null, null, false);
		insertRoom("x-project", "Elsewhere", USER, "other", true, 1, null, false, null, null, true);
		insertRoom("x-unnamed", "", USER, PROJECT, true, 1, null, false, null, null, true);
		insertRoom("x-child", "Child", USER, PROJECT, true, 1, null, false, null, "r1", true);
	}

	@AfterEach
	void tearDown() throws Exception {
		connection.close();
	}

	@Test
	void ungroupedListsPinnedFirstThenLastActivity() throws Exception {
		List<Map<String, Object>> rows = list(PlaygroundRoomSort.DATE_UPDATED, "DESC", PlaygroundRoomGroup.NONE);
		// pinned r4 (day 5) and r2 (never updated, created day 2), then r1 (9), r5 (6), r3 (4)
		assertEquals(List.of("r4", "r2", "r1", "r5", "r3"), ids(rows));
	}

	@Test
	void sortsByCreationAscending() throws Exception {
		List<Map<String, Object>> rows = list(PlaygroundRoomSort.DATE_CREATED, "ASC", PlaygroundRoomGroup.NONE);
		assertEquals(List.of("r2", "r4", "r1", "r3", "r5"), ids(rows));
	}

	@Test
	void sortsByNameIgnoringCase() throws Exception {
		List<Map<String, Object>> rows = list(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE);
		assertEquals(List.of("r2", "r4", "r3", "r1", "r5"), ids(rows));
		rows = list(PlaygroundRoomSort.NAME, "DESC", PlaygroundRoomGroup.NONE);
		assertEquals(List.of("r4", "r2", "r5", "r1", "r3"), ids(rows));
	}

	@Test
	void groupsByAgentNameWithRoomsWithoutAnAgentLast() throws Exception {
		List<Map<String, Object>> rows = list(PlaygroundRoomSort.DATE_UPDATED, "DESC", PlaygroundRoomGroup.AGENT);
		// alpha agent: pinned r2 then r5; Beta agent: r1; no agent: pinned r4 then r3
		assertEquals(List.of("r2", "r5", "r1", "r4", "r3"), ids(rows));
		assertEquals("alpha agent", rows.get(0).get("WORKSPACE_NAME"));
		assertEquals(null, rows.get(3).get("WORKSPACE_NAME"));
	}

	@Test
	void pagesNeverOverlapOrSkip() throws Exception {
		List<Map<String, Object>> all = list(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.AGENT);
		List<Map<String, Object>> first = run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.AGENT, 2, -1,
				null, null, null));
		List<Map<String, Object>> second = run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.AGENT, 2, 2,
				null, null, null));
		List<Map<String, Object>> third = run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.AGENT, 2, 4,
				null, null, null));
		assertEquals(ids(all).subList(0, 2), ids(first));
		assertEquals(ids(all).subList(2, 4), ids(second));
		assertEquals(ids(all).subList(4, 5), ids(third));
	}

	@Test
	void filtersBySearchPinnedAndRoomIds() throws Exception {
		assertEquals(List.of("r2", "r3"), ids(run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE, -1,
				-1, "HA", null, null))));
		assertEquals(List.of("r2", "r4"), ids(run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE, -1,
				-1, null, true, null))));
		assertEquals(List.of("r3", "r1", "r5"), ids(run(query(PlaygroundRoomSort.NAME, "ASC",
				PlaygroundRoomGroup.NONE, -1, -1, null, false, null))));
		assertEquals(List.of("r3", "r5"), ids(run(query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE, -1,
				-1, null, null, List.of("r5", "r3", "x-user")))));
	}

	@Test
	void searchesNamesAndMessagesWithNameMatchesFirst() throws Exception {
		// r4: the conversation says it
		conversation("r4", "[{\"role\":\"user\",\"content\":\"the QUARTERLY plan is ready\"}]");
		// r5: saved before conversations were kept on the room; only its log says it
		message("r5", "see the quarterly numbers");
		// r1: only a raw model call (a system prompt, say) mentions it, not the
		// conversation the user sees
		message("r1", "system: summarise quarterly reports when asked");
		conversation("r1", "[{\"role\":\"user\",\"content\":\"hello\"}]");
		room("r6", "Quarterly review", 6, 7, false, null);
		PlaygroundRoomFilter filter = new PlaygroundRoomFilter("quarterly", true, null, null, null, false, false);
		List<Map<String, Object>> rows = run(
				query(PlaygroundRoomSort.DATE_UPDATED, "DESC", PlaygroundRoomGroup.NONE, -1, -1, filter));
		// the name match first, then message matches (pinned r4 before r5)
		assertEquals(List.of("r6", "r4", "r5"), ids(rows));
		assertEquals(Boolean.TRUE, rows.get(0).get("NAME_MATCH"));
		assertEquals(Boolean.FALSE, rows.get(1).get("NAME_MATCH"));

		// without searchMessages, only the name is searched
		PlaygroundRoomFilter namesOnly = new PlaygroundRoomFilter("quarterly", false, null, null, null, false, false);
		assertEquals(List.of("r6"),
				ids(run(query(PlaygroundRoomSort.DATE_UPDATED, "DESC", PlaygroundRoomGroup.NONE, -1, -1, namesOnly))));
	}

	@Test
	void filtersByAgent() throws Exception {
		PlaygroundRoomFilter filter = new PlaygroundRoomFilter(null, false, null, null, "ws-a", false, false);
		List<Map<String, Object>> rows = run(
				query(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE, -1, -1, filter));
		assertEquals(List.of("r2", "r5"), ids(rows));
		assertEquals("alpha agent", rows.get(1).get("WORKSPACE_NAME"));
	}

	@Test
	void fallsBackToCreationWhenNeverUpdated() throws Exception {
		Map<String, Object> r2 = list(PlaygroundRoomSort.NAME, "ASC", PlaygroundRoomGroup.NONE).get(0);
		assertEquals(r2.get("DATE_CREATED"), r2.get("DATE_UPDATED"));
	}

	@Test
	void rejectsUnknownSortAndGroupKeys() {
		assertThrows(IllegalArgumentException.class, () -> PlaygroundRoomSort.fromKey("ROOM_NAME; DROP TABLE ROOM"));
		assertThrows(IllegalArgumentException.class, () -> PlaygroundRoomGroup.fromKey("model"));
		assertEquals(PlaygroundRoomSort.DATE_UPDATED, PlaygroundRoomSort.fromKey("dateUpdated"));
		assertEquals(PlaygroundRoomGroup.AGENT, PlaygroundRoomGroup.fromKey("AGENT"));
		assertThrows(IllegalArgumentException.class, () -> PlaygroundRoomGroup.fromKey("status"));
	}

	private List<Map<String, Object>> list(PlaygroundRoomSort sortBy, String sortDir, PlaygroundRoomGroup groupBy)
			throws Exception {
		return run(query(sortBy, sortDir, groupBy, -1, -1, null, null, null));
	}

	private static QueryExecutionUtility.ParameterizedQuery query(PlaygroundRoomSort sortBy, String sortDir,
			PlaygroundRoomGroup groupBy, long limit, long offset, String search, Boolean pinned, List<String> roomIds) {
		return query(sortBy, sortDir, groupBy, limit, offset,
				new PlaygroundRoomFilter(search, false, pinned, roomIds, null, false, false));
	}

	private static QueryExecutionUtility.ParameterizedQuery query(PlaygroundRoomSort sortBy, String sortDir,
			PlaygroundRoomGroup groupBy, long limit, long offset, PlaygroundRoomFilter filter) {
		return ModelInferenceLogsUtils.playgroundRoomsQuery(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB), USER,
				PROJECT, limit, offset, sortBy, sortDir, groupBy, filter);
	}

	private List<Map<String, Object>> run(QueryExecutionUtility.ParameterizedQuery query) throws Exception {
		return QueryExecutionUtility.flushRsToMap(connection, query, 0);
	}

	private static List<Object> ids(List<Map<String, Object>> rows) {
		return rows.stream().map(row -> row.get("ROOM_ID")).toList();
	}

	private void room(String id, String name, int createdDay, Integer updatedDay, boolean pinned, String workspace)
			throws Exception {
		insertRoom(id, name, USER, PROJECT, true, createdDay, updatedDay, pinned, workspace, null, true);
	}

	private void insertRoom(String id, String name, String user, String project, boolean active, int createdDay,
			Integer updatedDay, boolean pinned, String workspace, String parent, boolean hasMessage) throws Exception {
		try (PreparedStatement ps = connection.prepareStatement("INSERT INTO ROOM (ROOM_ID, ROOM_NAME, USER_ID, PROJECT_ID, IS_ACTIVE, DATE_CREATED, UPDATED_AT, PINNED, WORKSPACE_ID, PARENT_ROOM_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, id);
			ps.setString(2, name);
			ps.setString(3, user);
			ps.setString(4, project);
			ps.setBoolean(5, active);
			ps.setTimestamp(6, day(createdDay));
			ps.setTimestamp(7, updatedDay == null ? null : day(updatedDay));
			ps.setBoolean(8, pinned);
			ps.setString(9, workspace);
			ps.setString(10, parent);
			ps.executeUpdate();
		}
		if (hasMessage) {
			message(id, "hi from " + name);
		}
	}

	private void message(String roomId, String text) throws Exception {
		try (PreparedStatement ps = connection.prepareStatement("INSERT INTO MESSAGE VALUES (?, ?, ?)")) {
			ps.setString(1, UUID.randomUUID().toString());
			ps.setString(2, roomId);
			ps.setBytes(3, text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			ps.executeUpdate();
		}
	}

	private void conversation(String roomId, String json) throws Exception {
		try (PreparedStatement ps = connection.prepareStatement("UPDATE ROOM SET MESSAGES = ? WHERE ROOM_ID = ?")) {
			ps.setString(1, json);
			ps.setString(2, roomId);
			ps.executeUpdate();
		}
	}

	private static Timestamp day(int day) {
		return Timestamp.valueOf(String.format("2026-01-%02d 12:00:00", day));
	}
}
