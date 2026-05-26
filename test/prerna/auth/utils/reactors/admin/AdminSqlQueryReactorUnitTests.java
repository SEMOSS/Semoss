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
package prerna.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminSqlQueryReactorUnitTests {

	private AdminSqlQueryReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@BeforeEach
	void setup() {
		reactor = new AdminSqlQueryReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void testKeysToGet() {
		assertEquals(4, reactor.keysToGet.length);
		assertEquals(ReactorKeysEnum.QUERY_KEY.getKey(), reactor.keysToGet[0]);
		assertEquals(ReactorKeysEnum.DATABASE.getKey(), reactor.keysToGet[1]);
	}

	@Test
	void testNonAdminThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	void testEmptyQueryThrowsException() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("")).thenReturn("");

			assertThrows(SemossPixelException.class, reactor::execute);
		}
	}

	@Test
	void testNullDatabaseIdThrowsException() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT 1");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT 1")).thenReturn("SELECT 1");

			assertThrows(IllegalArgumentException.class, reactor::execute);
		}
	}

	@Test
	void testSelectQuerySuccess() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT * FROM users");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT * FROM users")).thenReturn("SELECT * FROM users");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}

	@Test
	void testShowTablesQuerySuccess() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SHOW TABLES");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SHOW TABLES")).thenReturn("SHOW TABLES");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}

	@Test
	void testDatabaseNotFoundThrowsException() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT 1");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "bad_db");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT 1")).thenReturn("SELECT 1");
			util.when(() -> Utility.getDatabase("bad_db")).thenReturn(null);

			assertThrows(SemossPixelException.class, reactor::execute);
		}
	}

	@Test
	void testGetReactorDescription() {
		assertNotNull(reactor.getReactorDescription());
	}

	@Test
	void testInsertQuerySuccess() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "INSERT INTO users VALUES(1, 'test')");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");

		when(user.getLogins()).thenReturn(Arrays.asList(AuthProvider.NATIVE));
		AccessToken token = mock(AccessToken.class);
		when(user.getAccessToken(AuthProvider.NATIVE)).thenReturn(token);
		when(token.getId()).thenReturn("test-user-id");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("INSERT INTO users VALUES(1, 'test')"))
					.thenReturn("INSERT INTO users VALUES(1, 'test')");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);
			when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.BOOLEAN, result.getNounType());
		}
	}

	@Test
	void testSelectQueryWithCustomLimit() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT * FROM users");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), "100");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT * FROM users")).thenReturn("SELECT * FROM users");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}

	@Test
	void testSelectQueryWithInvalidLimit() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT * FROM users");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), "not_a_number");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT * FROM users")).thenReturn("SELECT * FROM users");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}

	@Test
	void testSelectQueryWithNegativeLimit() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT * FROM users");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), "-5");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT * FROM users")).thenReturn("SELECT * FROM users");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}

	@Test
	void testSelectQueryWithOverMaxLimit() {
		reactor.keyValue.put(ReactorKeysEnum.QUERY_KEY.getKey(), "SELECT * FROM users");
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "db123");
		reactor.keyValue.put(ReactorKeysEnum.LIMIT.getKey(), "10000");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			util.when(() -> Utility.decodeURIComponent("SELECT * FROM users")).thenReturn("SELECT * FROM users");

			IDatabaseEngine engine = mock(IDatabaseEngine.class);
			util.when(() -> Utility.getDatabase("db123")).thenReturn(engine);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.FORMATTED_DATA_SET, result.getNounType());
		}
	}
}
