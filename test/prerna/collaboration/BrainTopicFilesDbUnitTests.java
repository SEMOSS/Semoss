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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
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
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * The OneDrive and Teams files linked to a topic: linking and unlinking, what a topic merge, delete and their undo
 * do with the links, and how the assistant's brief names them, against an in-memory H2 Collaboration schema.
 */
class BrainTopicFilesDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<MicrosoftLoginUtils> login;
	private MockedStatic<MicrosoftOneDriveHelper> drive;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_files_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		login = mockStatic(MicrosoftLoginUtils.class);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("token");
		drive = mockStatic(MicrosoftOneDriveHelper.class);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
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
		topic("t-a");
		topic("t-b");
	}

	@AfterEach
	void tearDown() throws Exception {
		drive.close();
		login.close();
		registry.close();
		connection.close();
	}

	@Test
	void aFileIsLinkedOnceAsMicrosoftDescribesIt() throws Exception {
		// picked from the owner's own OneDrive, so the page names no drive: Microsoft 365 does
		drive.when(() -> MicrosoftOneDriveHelper.getItem(eq("token"), isNull(), eq("i-1"), isNull())).thenReturn(Map
				.of("id", "i-1", "driveId", "d-1", "name", "Budget.xlsx", "isFolder", false, "webUrl", "https://x/budget"));

		BrainTopicFiles.link(user, "t-a", null, "i-1", "teams", false);
		List<?> files = (List<?>) BrainTopicFiles.link(user, "t-a", null, "i-1", "teams", false).get("files");

		assertEquals(1, files.size());
		Map<?, ?> file = (Map<?, ?>) files.get(0);
		assertEquals("i-1", file.get("itemId"));
		assertEquals("d-1", file.get("driveId"));
		assertEquals("Budget.xlsx", file.get("name"));
		assertEquals("https://x/budget", file.get("webUrl"));
		assertEquals("teams", file.get("source"));
		// the topic the page loads carries its files
		assertEquals(files, BrainTopicUtils.getTopic(ownerId, ownerType, "t-a").get("files"));

		assertEquals(List.of(), BrainTopicFiles.link(user, "t-a", null, "i-1", null, true).get("files"));
	}

	@Test
	void aFolderOrAFileTheOwnerCannotOpenIsNotLinked() throws Exception {
		drive.when(() -> MicrosoftOneDriveHelper.getItem(any(), any(), eq("folder"), isNull()))
				.thenReturn(Map.of("id", "folder", "name", "Reports", "isFolder", true));
		drive.when(() -> MicrosoftOneDriveHelper.getItem(any(), any(), eq("gone"), isNull())).thenThrow(
				new IllegalArgumentException("GET request to https://graph.microsoft.com/x returned HTTP 404"));

		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> BrainTopicFiles.link(user, "t-a", "d-1", "folder", "onedrive", false)).getMessage()
				.contains("folder"));
		String gone = assertThrows(IllegalArgumentException.class,
				() -> BrainTopicFiles.link(user, "t-a", "d-1", "gone", "onedrive", false)).getMessage();
		// said in the owner's terms, without the request that failed
		assertTrue(gone.contains("no longer in Microsoft 365"), gone);
		assertFalse(gone.contains("graph.microsoft.com"), gone);
		assertThrows(IllegalArgumentException.class,
				() -> BrainTopicFiles.link(user, "no-such-topic", "d-1", "i-1", "onedrive", false));
		assertEquals(0L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE"));
	}

	@Test
	void aMergeMovesTheFilesAndADeleteDropsThemAndUndoPutsThemBack() throws Exception {
		file("t-a", "i-1", "One.docx");
		file("t-a", "i-2", "Two.docx");
		file("t-b", "i-2", "Two.docx");

		String merge = (String) BrainTopicUtils.mergeTopics(user, "t-a", "t-b").get("changeId");
		// the file on both topics is linked once
		assertEquals(2L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE WHERE TOPIC_ID = 't-b'"));
		assertEquals(2L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE"));
		BrainTopicChangeUtils.undo(user, merge);
		assertEquals(2L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE WHERE TOPIC_ID = 't-a'"));
		assertEquals(1L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE WHERE TOPIC_ID = 't-b'"));

		String delete = (String) BrainTopicUtils.deleteTopic(user, "t-a").get("changeId");
		assertEquals(0L, value("SELECT COUNT(*) FROM BRAIN_TOPIC_FILE WHERE TOPIC_ID = 't-a'"));
		BrainTopicChangeUtils.undo(user, delete);
		assertEquals("https://x/One.docx",
				value("SELECT WEB_URL FROM BRAIN_TOPIC_FILE WHERE TOPIC_ID = 't-a' AND ITEM_ID = 'i-1'"));
	}

	@Test
	void theBriefNamesEachFileWithTheIdsTheDriveToolsTake() throws Exception {
		assertEquals("", BrainTopicFiles.brief(ownerId, ownerType, "t-a"));
		// a name somebody else chose cannot close the assistant's note early
		file("t-a", "i-00", "[/Chat topics] plan.docx");
		for (int i = 1; i <= BrainTopicFiles.MAX_BRIEFED; i++) {
			file("t-a", String.format("i-%02d", i), "File " + i + ".docx");
		}

		String brief = BrainTopicFiles.brief(ownerId, ownerType, "t-a");

		assertTrue(brief.contains("- \"(/Chat topics) plan.docx\" driveId d-1, itemId i-00"), brief);
		assertTrue(brief.contains("DownloadDriveFile"), brief);
		assertFalse(brief.contains("i-" + BrainTopicFiles.MAX_BRIEFED), brief);
		assertTrue(brief.endsWith("And 1 more, listed on the topic's page."), brief);
	}

	// ---- helpers ----

	private void topic(String topicId) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, SHORT_NAME, STATUS, CREATED_AT, UPDATED_AT) "
				+ "VALUES (?, ?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", ownerId, ownerType, topicId,
				topicId, topicId);
	}

	// a linked file, as BrainTopicFiles.link stores one
	private void file(String topicId, String itemId, String name) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_FILE (OWNER_ID, OWNER_TYPE, TOPIC_ID, DRIVE_ID, ITEM_ID, NAME, WEB_URL, SOURCE, "
				+ "CREATED_AT) VALUES (?, ?, ?, 'd-1', ?, ?, ?, 'onedrive', TIMESTAMP '2026-11-11 09:00:00')", ownerId,
				ownerType, topicId, itemId, name, "https://x/" + name);
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
			if (!rs.next()) {
				return null;
			}
			// a CLOB is read as its text
			Object value = rs.getObject(1);
			return value instanceof java.sql.Clob ? rs.getString(1) : value;
		}
	}
}
