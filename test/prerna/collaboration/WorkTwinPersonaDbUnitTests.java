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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * A person's persona against an in-memory H2 Collaboration schema: what is kept, which twin's prompt gets it, what
 * their own assistant is given, and what a data reset leaves.
 */
class WorkTwinPersonaDbUnitTests {

	private static final String TWIN = "twin-1";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_persona_" + UUID.randomUUID());
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
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
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
	}

	@AfterEach
	void tearDown() throws Exception {
		CollaborationDbUtils.initialized = false;
		registry.close();
		connection.close();
	}

	@Test
	void aSectionIsKeptAsPlainTextWithinItsSize() {
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));

		Map<String, Object> saved = WorkTwinPersona.save(user,
				Map.of("voice", "  ## Digital Twin\r\nShort and direct.\n\n\n\n# No small talk.  ", "short", "x".repeat(900)));
		assertEquals(true, saved.get("exists"));
		Map<?, ?> sections = (Map<?, ?>) saved.get("sections");
		// no line can pass for a heading of the prompt, and blank runs close up
		assertEquals("Digital Twin\nShort and direct.\n\nNo small talk.", sections.get("voice"));
		assertEquals(400, ((String) sections.get("short")).length());
		assertEquals("", sections.get("limits"));
		assertEquals(WorkTwinPersona.SECTIONS.keySet(), sections.keySet());

		// a section left out stays, one passed blank goes
		Map<String, Object> changes = new LinkedHashMap<>();
		changes.put("short", " ");
		changes.put("limits", "Pay and HR matters: send people to Dana Smith.");
		sections = (Map<?, ?>) WorkTwinPersona.save(user, changes).get("sections");
		assertEquals("", sections.get("short"));
		assertEquals("Digital Twin\nShort and direct.\n\nNo small talk.", sections.get("voice"));
		assertEquals("Pay and HR matters: send people to Dana Smith.", sections.get("limits"));
	}

	@Test
	void aPersonaNeedsAKnownSectionWithText() {
		assertThrows(IllegalArgumentException.class, () -> WorkTwinPersona.save(user, Map.of()));
		assertThrows(IllegalArgumentException.class, () -> WorkTwinPersona.save(user, Map.of("secret", "x")));
		assertThrows(IllegalArgumentException.class, () -> WorkTwinPersona.save(user, Map.of("voice", "  ")));
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));

		// the last section cannot be emptied by a save: that is what delete is for
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		assertThrows(IllegalArgumentException.class, () -> WorkTwinPersona.save(user, Map.of("voice", "")));
		assertEquals(true, WorkTwinPersona.get(user).get("exists"));
	}

	@Test
	void aTwinGetsThePersonaUntilItIsSwitchedOffForIt() throws Exception {
		assertNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));

		WorkTwinPersona.save(user, Map.of("voice", "Direct.", "limits", "Budgets: ask Dana Smith."));
		String block = WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes");
		assertNotNull(block);
		assertTrue(block.startsWith("## Persona of Robin Reyes\n"), block);
		// in reading order, under titles the server wrote
		assertTrue(block.indexOf("### Voice\nDirect.") < block.indexOf("### Leave to Robin Reyes\nBudgets: ask Dana Smith."),
				block);
		// someone else's persona is never read
		assertNull(WorkTwinPersona.block("someone-else", ownerType, TWIN, "Robin Reyes"));
		assertNull(WorkTwinPersona.block(ownerId, "OTHER", TWIN, "Robin Reyes"));

		assertEquals(Map.of("project", TWIN, "enabled", false), WorkTwinPersona.setTwin(user, TWIN, false));
		WorkTwinPersona.setTwin(user, TWIN, false);
		assertEquals(List.of(TWIN), WorkTwinPersona.get(user).get("offTwins"));
		assertNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, "twin-2", "Robin Reyes"));

		WorkTwinPersona.setTwin(user, TWIN, true);
		assertEquals(List.of(), WorkTwinPersona.get(user).get("offTwins"));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
		assertEquals(1, count("BRAIN_PERSONA"));
	}

	@Test
	void aTwinCanBeSwitchedOffBeforeThereIsAPersona() {
		WorkTwinPersona.setTwin(user, TWIN, false);
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		assertNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
	}

	@Test
	void aFullPersonaKeepsWhatTheTwinMustLeaveAloneAndCutsExamplesFirst() {
		Map<String, Object> full = new LinkedHashMap<>();
		for (Map.Entry<String, Integer> section : WorkTwinPersona.SECTIONS.entrySet()) {
			// lines of 50 characters, so a cut lands on a whole line
			full.put(section.getKey(), (section.getKey() + " ").repeat(60).substring(0, 49).concat("\n").repeat(40));
		}
		WorkTwinPersona.save(user, full);
		String block = WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes");
		String body = block.substring(block.indexOf("### "));
		int text = 0;
		for (String line : body.split("\n")) {
			text += line.startsWith("### ") || line.isEmpty() ? 0 : line.length() + 1;
		}
		assertTrue(text <= WorkTwinPersona.MAX_PROMPT, String.valueOf(text));
		for (String key : List.of("limits", "voice", "short", "stands", "decide", "people", "review")) {
			int cap = WorkTwinPersona.SECTIONS.get(key);
			assertEquals(cap / 50, lines(body, key), key);
		}
		// the examples are cut to the whole lines there is room for, never dropped mid-line
		int examples = lines(body, "examples");
		assertTrue(examples > 0 && examples < 1500 / 50, String.valueOf(examples));
	}

	@Test
	void deletingKeepsTheTwinsItWasSwitchedOffFor() throws Exception {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		WorkTwinPersona.setTwin(user, TWIN, false);
		sql("UPDATE BRAIN_PERSONA SET INTERVIEW_JSON = '{\"notes\":[\"writes short\"]}' WHERE OWNER_ID = ?", ownerId);

		Map<String, Object> gone = WorkTwinPersona.delete(user);
		assertEquals(false, gone.get("exists"));
		assertEquals(List.of(TWIN), gone.get("offTwins"));
		assertNull(text("INTERVIEW_JSON"));

		// written again, it does not come back on for that twin
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		assertNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, "twin-2", "Robin Reyes"));
	}

	@Test
	void aRowThatCannotBeReadGivesNoPersonaAndCanBeWrittenAgain() throws Exception {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		sql("UPDATE BRAIN_PERSONA SET BRIEF_JSON = '{not json' WHERE OWNER_ID = ?", ownerId);
		assertNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));
		WorkTwinPersona.save(user, Map.of("voice", "Direct again."));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
	}

	@Test
	void aRunGetsThePersonaOnlyOfACreatorOnRecordWhoCanStillChangeTheTwin() {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		try (MockedStatic<SecurityProjectUtils> security = mockStatic(SecurityProjectUtils.class)) {
			security.when(() -> SecurityProjectUtils.getProjectCreator(TWIN)).thenReturn(Pair.with(ownerId, ownerType));
			security.when(() -> SecurityProjectUtils.getUserProjectPermission(ownerId, TWIN)).thenReturn(1);

			// Collaboration is off: nothing is read at all
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
			security.verifyNoInteractions();

			CollaborationDbUtils.initialized = true;
			assertTrue(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes").contains("### Voice\nDirect."));
			assertNull(WorkTwinPersona.promptBlock(null, "Robin Reyes"));

			// an editor still gets it, someone who can only read the twin or has no access does not
			security.when(() -> SecurityProjectUtils.getUserProjectPermission(ownerId, TWIN)).thenReturn(2);
			assertNotNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
			security.when(() -> SecurityProjectUtils.getUserProjectPermission(ownerId, TWIN)).thenReturn(3);
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
			security.when(() -> SecurityProjectUtils.getUserProjectPermission(ownerId, TWIN)).thenReturn(null);
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
			security.when(() -> SecurityProjectUtils.getUserProjectPermission(ownerId, TWIN)).thenReturn(1);

			// an id without its login type could be another person's
			security.when(() -> SecurityProjectUtils.getProjectCreator(TWIN)).thenReturn(Pair.with(ownerId, null));
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
			security.when(() -> SecurityProjectUtils.getProjectCreator(TWIN)).thenReturn(null);
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));

			// a lookup that fails is a run without a persona, never a failed run
			security.when(() -> SecurityProjectUtils.getProjectCreator(TWIN)).thenThrow(new IllegalStateException("down"));
			assertNull(WorkTwinPersona.promptBlock(TWIN, "Robin Reyes"));
		}
	}

	@Test
	void aDataResetKeepsThePersonaAndClearsItsInterview() throws Exception {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		WorkTwinPersona.setTwin(user, TWIN, false);
		sql("UPDATE BRAIN_PERSONA SET INTERVIEW_JSON = '{\"notes\":[\"writes short\"]}' WHERE OWNER_ID = ?", ownerId);
		try (MockedStatic<WorkRoutineRoom> rooms = mockStatic(WorkRoutineRoom.class);
				MockedStatic<BrainMailWebhook> webhook = mockStatic(BrainMailWebhook.class)) {
			BrainResetUtils.resetMyData(user);
		}
		assertNull(text("INTERVIEW_JSON"));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, "twin-2", "Robin Reyes"));
		assertEquals(List.of(TWIN), WorkTwinPersona.get(user).get("offTwins"));
	}

	@Test
	void theOwnersAssistantGetsTheSectionsADraftReadsUntilItIsSwitchedOff() throws Exception {
		CollaborationDbUtils.initialized = true;
		// no persona yet: nothing to write with, and the switch reads as on
		assertNull(WorkTwinPersona.forAssistant(user));
		assertEquals(true, WorkTwinPersona.get(user).get("assistant"));

		Map<String, Object> all = new LinkedHashMap<>();
		for (String key : WorkTwinPersona.SECTIONS.keySet()) {
			all.put(key, "About " + key + ".");
		}
		WorkTwinPersona.save(user, all);
		String text = WorkTwinPersona.forAssistant(user);
		assertNotNull(text);
		// what a draft reads, in reading order, under titles that call them "the owner"
		int at = -1;
		for (String section : List.of("### In short\nAbout short.", "### Voice\nAbout voice.",
				"### Where the owner stands\nAbout stands.", "### With different people\nAbout people.",
				"### Leave to the owner\nAbout limits.", "### Sounds like the owner\nAbout examples.")) {
			int next = text.indexOf(section);
			assertTrue(next > at, section + " in " + text);
			at = next;
		}
		// how they decide and what they check stay with their twins
		assertFalse(text.contains("About decide."), text);
		assertFalse(text.contains("About review."), text);
		// it is the text of a tool's result, never a twin's section of a prompt
		assertFalse(text.contains("## Persona of"), text);
		assertFalse(text.contains("Digital Twin"), text);

		assertEquals(Map.of("enabled", false), WorkTwinPersona.setAssistant(user, false));
		WorkTwinPersona.setAssistant(user, false);
		assertEquals(false, WorkTwinPersona.get(user).get("assistant"));
		assertNull(WorkTwinPersona.forAssistant(user));
		// their twins still answer with it
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));

		assertEquals(Map.of("enabled", true), WorkTwinPersona.setAssistant(user, true));
		assertEquals(true, WorkTwinPersona.get(user).get("assistant"));
		assertNotNull(WorkTwinPersona.forAssistant(user));
		assertEquals(1, count("BRAIN_PERSONA"));
	}

	@Test
	void theAssistantCanBeSwitchedOffBeforeThereIsAPersona() throws Exception {
		CollaborationDbUtils.initialized = true;
		// on is what someone with no row has, so switching it on writes nothing
		WorkTwinPersona.setAssistant(user, true);
		assertEquals(0, count("BRAIN_PERSONA"));
		WorkTwinPersona.setAssistant(user, false);
		assertEquals(false, WorkTwinPersona.get(user).get("exists"));
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		assertNull(WorkTwinPersona.forAssistant(user));
		assertNotNull(WorkTwinPersona.block(ownerId, ownerType, TWIN, "Robin Reyes"));
	}

	@Test
	void theAssistantWritesWithoutAPersonaWhenThereIsNoneItCanRead() throws Exception {
		WorkTwinPersona.save(user, Map.of("decide", "Quality wins."));
		// Collaboration is off: nothing is read
		assertNull(WorkTwinPersona.forAssistant(user));
		CollaborationDbUtils.initialized = true;
		assertNull(WorkTwinPersona.forAssistant(null));
		// only a section a draft does not read holds text
		assertNull(WorkTwinPersona.forAssistant(user));
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		assertTrue(WorkTwinPersona.forAssistant(user).endsWith("### Voice\nDirect."));

		// it is the signed-in person's own: nobody else is given it
		AccessToken token = new AccessToken();
		token.setId("someone-else");
		token.setProvider(AuthProvider.NATIVE);
		User other = new User();
		other.setAccessToken(token);
		assertNull(WorkTwinPersona.forAssistant(other));

		// a row that cannot be read is a draft without a persona, never a failed draft
		sql("UPDATE BRAIN_PERSONA SET BRIEF_JSON = '{not json' WHERE OWNER_ID = ?", ownerId);
		assertNull(WorkTwinPersona.forAssistant(user));
		sql("DROP TABLE BRAIN_PERSONA");
		assertNull(WorkTwinPersona.forAssistant(user));
	}

	@Test
	void theAssistantsSwitchSurvivesADeleteAndADataReset() throws Exception {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		WorkTwinPersona.setAssistant(user, false);
		assertEquals(false, WorkTwinPersona.delete(user).get("assistant"));

		// written again, it does not come back on for their assistant
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		try (MockedStatic<WorkRoutineRoom> rooms = mockStatic(WorkRoutineRoom.class);
				MockedStatic<BrainMailWebhook> webhook = mockStatic(BrainMailWebhook.class)) {
			BrainResetUtils.resetMyData(user);
		}
		assertEquals(false, WorkTwinPersona.get(user).get("assistant"));
		CollaborationDbUtils.initialized = true;
		assertNull(WorkTwinPersona.forAssistant(user));
		WorkTwinPersona.setAssistant(user, true);
		assertNotNull(WorkTwinPersona.forAssistant(user));
	}

	// how many 50-character lines of a section's own text the block holds
	private static int lines(String body, String key) {
		int count = 0;
		for (String line : body.split("\n")) {
			count += line.startsWith(key + " ") ? 1 : 0;
		}
		return count;
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

	private String text(String column) throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT " + column + " FROM BRAIN_PERSONA")) {
			rs.next();
			return rs.getString(1);
		}
	}
}
