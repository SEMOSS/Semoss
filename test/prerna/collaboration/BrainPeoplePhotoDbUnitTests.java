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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
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
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Which Microsoft photo an avatar asks Graph for, against an in-memory H2 Collaboration schema: the owner's own, a
 * directory colleague's by id, an address not looked up yet, and nothing for the people Graph has no photo of.
 */
class BrainPeoplePhotoDbUnitTests {

	private static final byte[] JPEG = { (byte) 0xFF, (byte) 0xD8 };

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private User user;
	private String ownerId;
	private String ownerType;
	// the Graph paths photos were read from
	private final List<String> paths = new ArrayList<>();
	private final BrainMailHeaderSource graph = new BrainMailHeaderSource() {
		@Override
		public Map<String, Object> me(User user) {
			return Map.of();
		}

		@Override
		public Map<String, Object> manager(User user) {
			return null;
		}

		@Override
		public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
			return List.of();
		}

		@Override
		public byte[] photo(User user, String path) {
			paths.add(path);
			return JPEG;
		}
	};

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_photo_" + UUID.randomUUID());
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
		// signed in natively with Microsoft linked alongside, so the owner is the native login
		AccessToken nativeLogin = new AccessToken();
		nativeLogin.setId("owner-" + UUID.randomUUID());
		nativeLogin.setProvider(AuthProvider.NATIVE);
		AccessToken microsoft = new AccessToken();
		microsoft.setId("ms-owner");
		microsoft.setEmail("Owner@Test.com");
		microsoft.setProvider(AuthProvider.MICROSOFT);
		user = new User();
		user.setAccessToken(nativeLogin);
		user.setAccessToken(microsoft);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();

		// the owner's own person holds their mailbox address; their Microsoft login is not in the Brain
		person("p-self", "me@test.com", "self", null, false);
		sql("INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, KIND, VALUE_NORM) "
				+ "VALUES (?, ?, 'p-self', 'email', 'owner.alias@test.com')", ownerId, ownerType);
		person("p-ana", "ana@test.com", "colleague", "dir-ana", true);
		person("p-gone", "gone@test.com", null, null, true);
		person("p-news", "news@test.com", BrainSenderTyping.AUTOMATED, null, false);
		person("p-list", "team@test.com", BrainSenderTyping.AUTOMATED, "dir-team-list", true);
		person("p-client", "client@other.com", "external", null, false);
		person("p-new", "new@test.com", "colleague", null, false);
	}

	@AfterEach
	void tearDown() throws Exception {
		registry.close();
		connection.close();
	}

	@Test
	void readsThePhotoTheBrainCanName() throws Exception {
		// the Microsoft login's own address in any case, and an alias of the owner's own person
		assertEquals("/me/photos/96x96/$value", pathFor(" OWNER@test.com "));
		assertEquals("/me/photos/96x96/$value", pathFor("owner.alias@test.com"));
		// by directory id, which still works when an address is not the sign-in name
		assertEquals("/users/dir-ana/photos/96x96/$value", pathFor("ana@test.com"));
		// a colleague by domain not looked up yet, and an address the Brain has never seen
		assertEquals("/users/new@test.com/photos/96x96/$value", pathFor("new@test.com"));
		assertEquals("/users/someone@test.com/photos/96x96/$value", pathFor("someone@test.com"));
		assertArrayEquals(JPEG, BrainPeopleUtils.photo(user, "ana@test.com", graph));
	}

	@Test
	void asksGraphNothingWhenItHasNoPhotoToGive() throws Exception {
		// the directory was asked and has no one
		assertNull(pathFor("gone@test.com"));
		// outside the organization
		assertNull(pathFor("client@other.com"));
		// a bulk sender, and a list the directory matched
		assertNull(pathFor("news@test.com"));
		assertNull(pathFor("team@test.com"));
		// only plain addresses reach a Graph path
		assertNull(pathFor("ana@test.com/../../me/messages"));
		assertNull(pathFor("someone@test.com?$select=id"));
		assertNull(pathFor(""));
		assertNull(pathFor(null));
		assertNull(BrainPeopleUtils.photo(user, "client@other.com", graph));
	}

	// the Graph path one avatar reads its photo from, or null when it asks Graph nothing
	private String pathFor(String address) throws Exception {
		paths.clear();
		BrainPeopleUtils.photo(user, address, graph);
		return paths.isEmpty() ? null : paths.get(0);
	}

	private void person(String id, String email, String relationship, String directoryId, boolean checked)
			throws Exception {
		sql("INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, DISPLAY_NAME, RELATIONSHIP, "
				+ "DIRECTORY_ID, DIRECTORY_CHECKED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, id, email, id,
				relationship, directoryId, checked ? Timestamp.from(Instant.parse("2026-10-01T13:00:00Z")) : null);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
