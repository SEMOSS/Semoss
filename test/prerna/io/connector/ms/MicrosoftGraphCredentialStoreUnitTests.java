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
package prerna.io.connector.ms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.util.JdbcTestDatabase;

class MicrosoftGraphCredentialStoreUnitTests {

	@Test
	void validatesRenewableMicrosoftTokens() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			assertNull(MicrosoftGraphCredentialStore.get(null));
			assertNull(MicrosoftGraphCredentialStore.get(" "));
			assertFalse(MicrosoftGraphCredentialStore.isRefreshable(null));
			AccessToken token = token("test-access", "test-refresh", 3_600_000L);
			assertTrue(MicrosoftGraphCredentialStore.isRefreshable(token));
			token.setProvider(AuthProvider.GOOGLE);
			assertFalse(MicrosoftGraphCredentialStore.isRefreshable(token));
			assertThrows(IllegalArgumentException.class, () -> MicrosoftGraphCredentialStore.save(token));
			token.setProvider(AuthProvider.MICROSOFT);
			token.removeMetaKey(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY);
			token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, " ");
			assertFalse(MicrosoftGraphCredentialStore.isRefreshable(token));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void expiredAndUnknownLifetimesRefreshAndPersistBeforeReturning(boolean unknownExpiry) throws Exception {
		try (var db = database(); var users = users()) {
			AccessToken previous = token("old-access", "old-refresh", -1000L);
			if (unknownExpiry) {
				previous.setExpires_in(0);
			}
			MicrosoftGraphCredentialStore.save(previous);
			AccessToken renewed = token("new-access", "new-refresh", 3_600_000L);
			try (var fillers = mockConstruction(MicrosoftTokenFiller.class,
					(filler, context) -> when(filler.refreshAccessToken(any(), anyMap())).thenReturn(renewed))) {
				assertEquals("new-access", MicrosoftGraphCredentialStore.accessToken("account-one"));
				assertEquals(1, fillers.constructed().size());
				AccessToken persisted = MicrosoftGraphCredentialStore.get("account-one");
				assertEquals("new-access", persisted.getAccess_token());
				assertEquals("new-refresh", persisted.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY)
						.iterator().next());
				assertEquals(renewed.getStartTime() + renewed.getExpires_in() * 1000L,
						persisted.getStartTime() + persisted.getExpires_in() * 1000L);
				assertEquals("mail@example.test", persisted.getEmail());
				assertEquals(AuthProvider.MICROSOFT, persisted.getProvider());
				verify(users.constructed().get(0)).removeUserMemory();
			}
		}
	}

	@Test
	void failedRefreshCleansTemporaryUserAndKeepsOriginalCredentials() throws Exception {
		try (var db = database(); var users = users()) {
			MicrosoftGraphCredentialStore.save(token("old-access", "old-refresh", -1000L));
			try (var fillers = mockConstruction(MicrosoftTokenFiller.class,
					(filler, context) -> when(filler.refreshAccessToken(any(), anyMap()))
							.thenThrow(new IllegalStateException("Renewal failed")))) {
				assertThrows(IllegalStateException.class,
						() -> MicrosoftGraphCredentialStore.accessToken("account-one"));
				assertEquals("old-access", MicrosoftGraphCredentialStore.get("account-one").getAccess_token());
				verify(users.constructed().get(0)).removeUserMemory();
			}
		}
	}

	@Test
	void failedPersistenceNeverReturnsRefreshedTokenAndStillCleansUser() throws Exception {
		try (var db = database(); var users = users()) {
			MicrosoftGraphCredentialStore.save(token("old-access", "old-refresh", -1000L));
			try (var fillers = mockConstruction(MicrosoftTokenFiller.class,
					(filler, context) -> when(filler.refreshAccessToken(any(), anyMap()))
							.thenReturn(token("new-access", "new-refresh", 3_600_000L)))) {
				doThrow(new SQLException("test-only-credential-payload")).when(db.connection).commit();
				IllegalStateException failure = assertThrows(IllegalStateException.class,
						() -> MicrosoftGraphCredentialStore.accessToken("account-one"));
				assertFalse(failure.toString().contains("test-only-credential-payload"));
				assertNull(failure.getCause());
				assertEquals("old-access", MicrosoftGraphCredentialStore.get("account-one").getAccess_token());
				verify(users.constructed().get(0)).removeUserMemory();
			}
		}
	}

	@Test
	void absentCredentialCannotStartBackgroundAccess() throws Exception {
		try (var db = database(); var users = users()) {
			assertThrows(IllegalArgumentException.class,
					() -> MicrosoftGraphCredentialStore.accessToken("unlinked-account"));
			assertTrue(users.constructed().isEmpty());
		}
	}

	private static JdbcTestDatabase database() throws Exception {
		JdbcTestDatabase db = new JdbcTestDatabase();
		db.execute("CREATE TABLE MS_GRAPH_CREDENTIAL (USER_ID VARCHAR PRIMARY KEY, USER_EMAIL VARCHAR, "
				+ "ACCESS_TOKEN CLOB, REFRESH_TOKEN CLOB, TOKEN_EXPIRATION TIMESTAMP, CREATED_ON TIMESTAMP, UPDATED_ON TIMESTAMP)");
		return db;
	}

	private static MockedConstruction<User> users() {
		return mockConstruction(User.class, (user, context) -> {
			AtomicReference<AccessToken> current = new AtomicReference<>();
			doAnswer(call -> {
				current.set(call.getArgument(0));
				return null;
			}).when(user).setAccessToken(any());
			when(user.getAccessToken(AuthProvider.MICROSOFT)).thenAnswer(call -> current.get());
		});
	}

	private static AccessToken token(String value, String refresh, long remainingMillis) {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setId("account-one");
		token.setEmail("mail@example.test");
		token.setAccess_token(value);
		token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, refresh);
		token.setExpires_in(3600);
		token.setStartTime(System.currentTimeMillis() - 3_600_000L + remainingMillis);
		return token;
	}
}
