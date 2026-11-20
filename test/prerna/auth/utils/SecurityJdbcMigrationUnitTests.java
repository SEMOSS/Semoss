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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class SecurityJdbcMigrationUnitTests {

	private Connection connection;
	private IRDBMSEngine engine;
	private MockedStatic<SystemEngineRegistry> registry;

	@BeforeEach
	void setup() throws Exception {
		connection = spy(DriverManager.getConnection("jdbc:h2:mem:auth_jdbc_" + UUID.randomUUID()));
		engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(engine);
	}

	@AfterEach
	void cleanup() throws Exception {
		registry.close();
		connection.close();
	}

	@Test
	void metadataReplacementFailureRestoresOriginalRowsAndKeepsVoidFallback() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE INSIGHTMETA (PROJECTID VARCHAR, INSIGHTID VARCHAR, METAKEY VARCHAR, "
					+ "METAVALUE VARCHAR CHECK (METAVALUE <> 'reject'), METAORDER INT)");
			statement.execute("INSERT INTO INSIGHTMETA VALUES ('project','insight','tag','original',0)");
		}
		assertDoesNotThrow(() -> SecurityInsightUtils.updateInsightTags("project", "insight", List.of("ok", "reject")));
		try (var statement = connection.prepareStatement("SELECT METAVALUE FROM INSIGHTMETA");
				var rs = statement.executeQuery()) {
			assertTrue(rs.next());
			assertEquals("original", rs.getString(1));
			assertFalse(rs.next());
		}
		verify(connection).rollback();
		verify(engine, never()).getPreparedStatement(anyString());
		assertTrue(connection.getAutoCommit());
		assertFalse(connection.isClosed());
	}

	@Test
	void successfulMetadataReplacementCommitsInManualMode() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE INSIGHTMETA (PROJECTID VARCHAR, INSIGHTID VARCHAR, METAKEY VARCHAR, "
					+ "METAVALUE VARCHAR, METAORDER INT)");
		}
		connection.setAutoCommit(false);
		SecurityInsightUtils.updateInsightTags("project", "insight", List.of("  spaced  ", ""));
		connection.rollback();
		try (var statement = connection.prepareStatement("SELECT METAVALUE FROM INSIGHTMETA ORDER BY METAORDER");
				var rs = statement.executeQuery()) {
			assertTrue(rs.next());
			assertEquals("  spaced  ", rs.getString(1));
			assertTrue(rs.next());
			assertEquals("", rs.getString(1));
			assertFalse(rs.next());
		}
		verify(connection).commit();
		assertFalse(connection.getAutoCommit());
	}

	@Test
	void tokenFailureKeepsExistingReturnContractAfterRollback() throws Exception {
		// No TOKEN table: execution fails in manual transaction mode.
		connection.setAutoCommit(false);
		clearInvocations(connection);
		Object[] token = SecurityTokenUtils.generateToken("127.0.0.1", "client");
		assertNotNull(token[0]);
		assertEquals("127.0.0.1", token[1]);
		assertEquals("client", token[2]);
		verify(connection).rollback();
		verify(connection, never()).commit();
	}

	@Test
	void projectTemplateZeroRowsPreservesIntentionalError() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE PROJECT (PROJECTID VARCHAR, IS_TEMPLATE BOOLEAN)");
		}
		connection.setAutoCommit(false);
		clearInvocations(connection);
		var error = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.setProjectTemplate("absent", true));
		assertEquals("Project does not exist", error.getMessage());
		verify(connection).rollback();
		verify(connection, never()).commit();
	}

	@Test
	void scalarReadCompletesManualTransactionAndPreservesEmptyResult() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE PROJECT (PROJECTID VARCHAR, PORTALPUBLISHED TIMESTAMP)");
		}
		connection.setAutoCommit(false);
		assertNull(SecurityProjectUtils.getPortalPublishedTimestamp("absent"));
		verify(connection).rollback();
		assertFalse(connection.isClosed());
	}

	@Test
	void readFailurePreservesPublicNullFallback() throws Exception {
		connection.setAutoCommit(false);
		assertNull(SecurityProjectUtils.getPortalPublishedTimestamp("absent"));
		verify(connection).rollback();
	}

	@Test
	void shareTokenValidationRunsBeforeAcquisition() throws Exception {
		var error = assertThrows(IllegalArgumentException.class,
				() -> SecurityShareSessionUtils.createShareToken(null, "session", "route"));
		assertEquals("Cannot share a session for a user who is not logged in", error.getMessage());
		verify(engine, never()).getConnection();
	}

	@ParameterizedTest
	@CsvSource({ "native,false", "native,true", "api,false", "api,true", "access,false", "access,true" })
	void legacyCredentialMigrationCommitsOrRollsBackWithoutRejectingValidAuthentication(String kind,
			boolean rejectUpdate) throws Exception {
		String secret = "test-only-secret";
		String legacySalt = org.mindrot.jbcrypt.BCrypt.gensalt(4);
		String legacyHash = AbstractSecurityUtils.hash(secret, legacySalt);
		boolean accessKey = kind.equals("access");
		String table = accessKey ? "SMSS_USER_ACCESS_KEYS" : "SMSS_USER";
		String saltColumn = accessKey ? "SECRETSALT" : "SALT";
		String hashColumn = accessKey ? "SECRETKEY" : "PASSWORD";
		String type = kind.equals("api") ? AuthProvider.API_USER.toString() : AuthProvider.NATIVE.getLabel();
		try (var statement = connection.createStatement()) {
			statement.execute(accessKey
					? "CREATE TABLE SMSS_USER_ACCESS_KEYS (ACCESSKEY VARCHAR, SECRETKEY VARCHAR, SECRETSALT VARCHAR)"
					: "CREATE TABLE SMSS_USER (ID VARCHAR, TYPE VARCHAR, PASSWORD VARCHAR, SALT VARCHAR)");
		}
		try (var ps = connection.prepareStatement(accessKey ? "INSERT INTO SMSS_USER_ACCESS_KEYS VALUES (?, ?, ?)"
				: "INSERT INTO SMSS_USER (ID, PASSWORD, SALT, TYPE) VALUES (?, ?, ?, ?)")) {
			ps.setString(1, "credential");
			ps.setString(2, legacyHash);
			ps.setString(3, legacySalt);
			if (!accessKey) {
				ps.setString(4, type);
			}
			ps.executeUpdate();
		}
		if (rejectUpdate) {
			try (var statement = connection.createStatement()) {
				statement.execute("ALTER TABLE " + table + " ADD CHECK (" + saltColumn + " = '" + legacySalt + "')");
			}
		}
		connection.setAutoCommit(false);
		clearInvocations(connection);
		var row = mock(IHeadersDataRow.class);
		var wrapper = mock(IRawSelectWrapper.class);
		var manager = mock(WrapperManager.class);
		when(wrapper.hasNext()).thenReturn(true);
		when(wrapper.next()).thenReturn(row);
		when(wrapper.getHeaders()).thenReturn(new String[] { "ID", "NAME", "USERNAME", "EMAIL", "TYPE", "ADMIN",
				"PASSWORD", "SALT", "PHONE", "PHONEEXTENSION", "COUNTRYCODE" });
		when(row.getValues()).thenReturn(kind.equals("native")
				? new Object[] { "credential", "Name", "login", "email", type, false, legacyHash, legacySalt, null,
						null, null }
				: accessKey ? new Object[] { legacyHash, legacySalt, "user", type, "Name", "login", "email" }
						: new Object[] { legacyHash, legacySalt });
		when(manager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(wrapper);
		try (var managers = mockStatic(WrapperManager.class); var updates = mockStatic(SecurityUpdateUtils.class)) {
			managers.when(WrapperManager::getInstance).thenReturn(manager);
			if (kind.equals("native")) {
				assertTrue(SecurityNativeUserUtils.logIn("login", secret));
			} else if (accessKey) {
				assertEquals("user",
						SecurityUserAccessKeyUtils.validateKeysAndReturnToken("credential", secret).getId());
			} else {
				assertTrue(SecurityAPIUserUtils.validCredentials("credential", secret));
			}
		}
		try (var ps = connection.prepareStatement("SELECT " + saltColumn + ", " + hashColumn + " FROM " + table);
				var rs = ps.executeQuery()) {
			assertTrue(rs.next());
			String actualSalt = rs.getString(1);
			String actualHash = rs.getString(2);
			if (rejectUpdate) {
				assertEquals(legacySalt, actualSalt);
				assertEquals(legacyHash, actualHash);
				verify(connection, never()).commit();
				verify(connection, atLeastOnce()).rollback();
			} else {
				assertFalse(AbstractSecurityUtils.isLegacySalt(actualSalt));
				assertTrue(AbstractSecurityUtils.credentialMatches(secret, actualHash, actualSalt));
				verify(connection).commit();
			}
		}
		assertFalse(connection.getAutoCommit());
		assertFalse(connection.isClosed());
	}

	@Test
	void expiredTokenCleanupCommitsWhileRetainingFreshTokens() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE TOKEN (VALUE VARCHAR, DATEADDED TIMESTAMP)");
			statement.execute(
					"INSERT INTO TOKEN VALUES ('old', TIMESTAMP '2000-01-01 00:00:00'), ('fresh', TIMESTAMP '2999-01-01 00:00:00')");
		}
		connection.setAutoCommit(false);
		clearInvocations(connection);
		SecurityTokenUtils.clearExpiredTokens(60);
		connection.rollback();
		try (var ps = connection.prepareStatement("SELECT VALUE FROM TOKEN"); var rs = ps.executeQuery()) {
			assertTrue(rs.next());
			assertEquals("fresh", rs.getString(1));
			assertFalse(rs.next());
		}
		verify(connection).commit();
	}

	@Test
	void userMetadataReplacementIsAtomicAndPreservesLiteralNullAndEmptyValues() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute(
					"CREATE TABLE USERMETA (USERID VARCHAR, TYPE VARCHAR, METAKEY VARCHAR, METAVALUE VARCHAR CHECK (METAVALUE <> 'reject'), METAORDER INT)");
			statement.execute("INSERT INTO USERMETA VALUES ('u', 'NATIVE', 'tag', 'original', 0)");
		}
		var token = new AccessToken();
		token.setId("u");
		token.setProvider(AuthProvider.NATIVE);
		SecurityUserUtils.updateUserMetadata(token, "tag", List.of("ok", "reject"));
		try (var ps = connection.prepareStatement("SELECT METAVALUE FROM USERMETA"); var rs = ps.executeQuery()) {
			assertTrue(rs.next());
			assertEquals("original", rs.getString(1));
			assertFalse(rs.next());
		}
		verify(connection).rollback();
		SecurityUserUtils.updateUserMetadata(token, "tag", Arrays.asList("", null, "  spaced  "));
		try (var ps = connection.prepareStatement("SELECT METAVALUE FROM USERMETA ORDER BY METAORDER");
				var rs = ps.executeQuery()) {
			for (String expected : List.of("", "null", "  spaced  ")) {
				assertTrue(rs.next());
				assertEquals(expected, rs.getString(1));
			}
			assertFalse(rs.next());
		}
	}

	@Test
	void tokenLabelsRetainCallerTrimmingAndEmptyToNullStorage() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute(
					"CREATE TABLE SMSS_USER_ACCESS_KEYS (USERID VARCHAR, TYPE VARCHAR, ACCESSKEY VARCHAR, SECRETKEY VARCHAR, SECRETSALT VARCHAR, DATECREATED TIMESTAMP, LASTUSED TIMESTAMP, TOKENNAME VARCHAR, TOKENDESCRIPTION VARCHAR)");
		}
		var token = new AccessToken();
		token.setId("u");
		token.setProvider(AuthProvider.NATIVE);
		String[] inputs = { null, "", " \t ", "  named  ", "世界" };
		String[] returned = { null, "", "", "named", "世界" };
		String[] stored = { null, null, null, "named", "世界" };
		for (int i = 0; i < inputs.length; i++) {
			var result = SecurityUserAccessKeyUtils.createUserAccessToken(token, inputs[i], inputs[i]);
			assertEquals(returned[i], result.get("TOKENNAME"));
			assertEquals(returned[i], result.get("TOKENDESCRIPTION"));
			try (var ps = connection.prepareStatement(
					"SELECT TOKENNAME, TOKENDESCRIPTION FROM SMSS_USER_ACCESS_KEYS WHERE ACCESSKEY=?")) {
				ps.setString(1, result.get("ACCESSKEY"));
				try (var rs = ps.executeQuery()) {
					assertTrue(rs.next());
					assertEquals(stored[i], rs.getString(1));
					assertEquals(stored[i], rs.getString(2));
				}
			}
		}
	}
}
