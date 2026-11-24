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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.imageio.ImageIO;

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
 * A person's own Digital Twin against an in-memory H2 Collaboration schema: the name it starts with, what a save
 * keeps, what its prompt is given, and what a data reset leaves.
 */
class WorkMainTwinDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_main_twin_" + UUID.randomUUID());
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
		user = user("ownerid", "Owner");
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		CollaborationDbUtils.initialized = true;
	}

	@AfterEach
	void tearDown() throws Exception {
		CollaborationDbUtils.initialized = false;
		registry.close();
		connection.close();
	}

	@Test
	void aTwinStartsUnderItsOwnersFirstName() throws Exception {
		assertEquals("Robin's Digital Twin", WorkMainTwin.defaultName("Reyes, Robin"));
		assertEquals("Robin's Digital Twin", WorkMainTwin.defaultName("Reyes, Robin M."));
		assertEquals("Robin's Digital Twin", WorkMainTwin.defaultName("  Robin\nReyes "));
		assertEquals("Robin's Digital Twin", WorkMainTwin.defaultName("Robin"));
		// no word to name it by
		assertNull(WorkMainTwin.defaultName(null));
		assertNull(WorkMainTwin.defaultName("  "));
		assertNull(WorkMainTwin.defaultName("Reyes,"));

		// with no profile yet, the name they signed in with; reading it writes nothing
		assertEquals(Map.of("name", "Owner's Digital Twin"), WorkMainTwin.get(user));
		assertEquals("Owner's Digital Twin", WorkMainTwin.name(user));
		assertEquals(0, count("BRAIN_PERSONA"));
		assertEquals(0, count("BRAIN_PROFILE"));

		// the name on their Collaboration profile comes first
		sql("INSERT INTO BRAIN_PROFILE (OWNER_ID, OWNER_TYPE, DISPLAY_NAME) VALUES (?, ?, ?)", ownerId, ownerType,
				"Reyes, Robin");
		assertEquals("Robin's Digital Twin", WorkMainTwin.get(user).get("name"));
		assertEquals("Robin's Digital Twin", WorkMainTwin.name(user));

		// someone with no name at all still has a twin; its prompt is given no name
		User nameless = user("nameless", null);
		assertEquals(Map.of("name", "Your Digital Twin"), WorkMainTwin.get(nameless));
		assertNull(WorkMainTwin.name(nameless));
	}

	@Test
	void aSavedNameIsOneLineAndTheDefaultIsNeverStored() throws Exception {
		assertEquals("Ops Twin #1", WorkMainTwin.save(user, "  Ops\r\nTwin\u2028 #1\u0007 ", null).get("name"));
		assertEquals("Ops Twin #1", storedName());
		assertEquals("Ops Twin #1", WorkMainTwin.name(user));
		assertEquals(80, ((String) WorkMainTwin.save(user, "x".repeat(200), null).get("name")).length());

		// empty goes back to the default
		assertEquals("Owner's Digital Twin", WorkMainTwin.save(user, "", null).get("name"));
		assertNull(storedName());

		// the default itself is kept as not set, so it goes on following the person's own name
		WorkMainTwin.save(user, "Named", null);
		assertEquals("Owner's Digital Twin", WorkMainTwin.save(user, " Owner's  Digital Twin ", null).get("name"));
		assertNull(storedName());
		sql("INSERT INTO BRAIN_PROFILE (OWNER_ID, OWNER_TYPE, DISPLAY_NAME) VALUES (?, ?, ?)", ownerId, ownerType,
				"Reyes, Robin");
		assertEquals("Robin's Digital Twin", WorkMainTwin.get(user).get("name"));
		assertEquals(1, count("BRAIN_PERSONA"));
	}

	@Test
	void aPictureIsKeptAsASmallPngUntilItIsRemoved() throws Exception {
		Map<String, Object> saved = WorkMainTwin.save(user, null, picture(800, 400));
		// a save of the picture alone leaves the name as it was
		assertEquals("Owner's Digital Twin", saved.get("name"));
		byte[] png = Base64.getDecoder().decode((String) saved.get("image"));
		assertEquals((byte) 0x89, png[0]);
		assertEquals('P', png[1]);
		BufferedImage kept = ImageIO.read(new ByteArrayInputStream(png));
		assertEquals(192, kept.getWidth());
		assertEquals(96, kept.getHeight());
		// a small picture is not made larger
		kept = ImageIO.read(new ByteArrayInputStream(
				Base64.getDecoder().decode((String) WorkMainTwin.save(user, null, picture(100, 50)).get("image"))));
		assertEquals(100, kept.getWidth());

		// a save of the name alone leaves the picture
		assertTrue(WorkMainTwin.save(user, "Ops Twin", null).containsKey("image"));
		// and an empty picture removes it, leaving the name
		assertEquals(Map.of("name", "Ops Twin"), WorkMainTwin.save(user, null, ""));
		assertEquals(1, count("BRAIN_PERSONA"));
	}

	@Test
	void aPictureThatCannotBeUsedIsRefusedAndNothingIsWritten() throws Exception {
		assertRefused("That picture could not be read. Choose another one.", "New name", "not base64 !!");
		assertRefused("That picture could not be read. Choose another one.", "New name",
				Base64.getEncoder().encodeToString("plain text, not a picture".getBytes()));
		assertRefused("That picture is too large. Choose a smaller one.", "New name",
				"A".repeat(WorkMainTwin.MAX_IMAGE_CHARS + 1));
		assertRefused("Give a name or a picture to save.", null, null);
		// the name that came with a refused picture was not saved either
		assertEquals(0, count("BRAIN_PERSONA"));
		assertEquals("Owner's Digital Twin", WorkMainTwin.get(user).get("name"));
	}

	@Test
	void theTwinAndThePersonaLeaveEachOtherAloneAndADataResetKeepsBoth() throws Exception {
		WorkTwinPersona.save(user, Map.of("voice", "Direct."));
		WorkMainTwin.save(user, "Ops Twin", picture(64, 64));
		assertEquals("Direct.", ((Map<?, ?>) WorkTwinPersona.get(user).get("sections")).get("voice"));
		assertEquals(1, count("BRAIN_PERSONA"));

		// deleting the persona is not deleting the twin
		WorkTwinPersona.delete(user);
		Map<String, Object> twin = WorkMainTwin.get(user);
		assertEquals("Ops Twin", twin.get("name"));
		assertNotNull(twin.get("image"));

		try (MockedStatic<WorkRoutineRoom> rooms = mockStatic(WorkRoutineRoom.class);
				MockedStatic<BrainMailWebhook> webhook = mockStatic(BrainMailWebhook.class)) {
			BrainResetUtils.resetMyData(user);
		}
		assertEquals(twin, WorkMainTwin.get(user));

		// it is the signed-in person's own: nobody else reads it
		assertEquals(Map.of("name", "Dana's Digital Twin"), WorkMainTwin.get(user("someone-else", "Dana Smith")));
	}

	@Test
	void theTwinsPromptSaysWhoItIsAndHowItsOwnerThinks() throws Exception {
		String unnamedRule = "When the owner asks who you are, say you are their Digital Twin.";
		String prompt = WorkMainTwin.promptBlock(user);
		assertTrue(prompt.startsWith("## Who you are\nYou are the owner's Digital Twin"), prompt);
		assertTrue(prompt.contains("\n- Name: Owner's Digital Twin\n"), prompt);
		assertFalse(prompt.contains(unnamedRule), prompt);
		// no persona: who it is, and nothing about how they think
		assertFalse(prompt.contains("## How the owner thinks"), prompt);

		WorkMainTwin.save(user, "Ops Twin", null);
		WorkTwinPersona.save(user, Map.of("decide", "Quality wins.", "voice", "Direct."));
		prompt = WorkMainTwin.promptBlock(user);
		assertTrue(prompt.contains("\n- Name: Ops Twin\n"), prompt);
		assertTrue(prompt.indexOf("## Who you are") < prompt.indexOf("\n\n## How the owner thinks\n"), prompt);
		assertTrue(prompt.endsWith("### How the owner decides\nQuality wins."), prompt);
		// how they sound is not part of how it thinks
		assertFalse(prompt.contains("Direct."), prompt);

		// the switch for their own twin takes the persona out and leaves who it is
		WorkTwinPersona.setAssistant(user, false);
		prompt = WorkMainTwin.promptBlock(user);
		assertTrue(prompt.contains("\n- Name: Ops Twin\n"), prompt);
		assertFalse(prompt.contains("## How the owner thinks"), prompt);

		// what cannot be read is left out, never a failed run
		assertNull(WorkMainTwin.name(null));
		sql("DROP TABLE BRAIN_PERSONA");
		assertNull(WorkMainTwin.name(user));
		prompt = WorkMainTwin.promptBlock(user);
		assertTrue(prompt.startsWith("## Who you are\n"), prompt);
		assertFalse(prompt.contains("- Name:"), prompt);
		assertTrue(prompt.contains(unnamedRule), prompt);
		// Collaboration is off: nothing is read
		CollaborationDbUtils.initialized = false;
		assertNull(WorkMainTwin.name(user));
	}

	@Test
	void theirSettingsCarryTheTwinAndAFailedReadLeavesItOut() throws Exception {
		try (MockedStatic<CollaborationUtils> collaboration = mockStatic(CollaborationUtils.class);
				MockedStatic<WorkTwinAvatar> avatar = mockStatic(WorkTwinAvatar.class)) {
			assertEquals(Map.of("name", "Owner's Digital Twin"), BrainProfileUtils.getSettings(user).get("mainTwin"));

			sql("INSERT INTO BRAIN_PERSONA (OWNER_ID, OWNER_TYPE, TWIN_NAME, TWIN_IMAGE) VALUES (?, ?, ?, ?)", ownerId,
					ownerType, "Ops Twin", "aGVsbG8=");
			assertEquals(Map.of("name", "Ops Twin", "image", "aGVsbG8="),
					BrainProfileUtils.getSettings(user).get("mainTwin"));

			// the web's first load reads these settings: a twin that cannot be read never fails them
			sql("DROP TABLE BRAIN_PERSONA");
			Map<String, Object> settings = BrainProfileUtils.getSettings(user);
			assertFalse(settings.containsKey("mainTwin"), settings.toString());
			assertTrue(settings.containsKey("version") && settings.containsKey("avatarGeneration"), settings.toString());
		}
	}

	private void assertRefused(String why, String name, String image) {
		assertEquals(why,
				assertThrows(IllegalArgumentException.class, () -> WorkMainTwin.save(user, name, image)).getMessage());
	}

	private static User user(String id, String name) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setName(name);
		token.setProvider(AuthProvider.NATIVE);
		User user = new User();
		user.setAccessToken(token);
		return user;
	}

	// a picture of that size, as the web sends one
	private static String picture(int width, int height) throws Exception {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		ImageIO.write(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB), "png", bytes);
		return Base64.getEncoder().encodeToString(bytes.toByteArray());
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

	private String storedName() throws Exception {
		try (Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT TWIN_NAME FROM BRAIN_PERSONA")) {
			rs.next();
			return rs.getString(1);
		}
	}
}
