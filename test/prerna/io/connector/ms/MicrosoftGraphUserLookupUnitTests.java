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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.util.SocialPropertiesUtil;

/**
 * Checks the directory lookups and additions behind the group manager and
 * sharing endpoints, with the Graph client, the social properties and the
 * security database replaced by mocks.
 */
class MicrosoftGraphUserLookupUnitTests {

	private static final String USER_ID = "4f2c0a1e-graph-id";
	private static final String DISPLAY_NAME = "Directory Person";
	private static final String MAIL = "person@contoso.com";
	private static final String PRINCIPAL_NAME = "person@contoso.onmicrosoft.com";
	private static final String SIGN_IN_REQUIRED = "Sign in with Microsoft to use your organization's directory";

	private static final List<String> DEFAULT_SELECT = List.of("id", "displayName", "mail", "userPrincipalName");

	private MockedStatic<SocialPropertiesUtil> social;
	private SocialPropertiesUtil properties;
	private MockedStatic<SecurityQueryUtils> queries;
	private MockedStatic<SecurityUpdateUtils> updates;

	@BeforeEach
	void setup() {
		properties = mock(SocialPropertiesUtil.class);
		social = mockStatic(SocialPropertiesUtil.class);
		social.when(SocialPropertiesUtil::getInstance).thenReturn(properties);
		queries = mockStatic(SecurityQueryUtils.class);
		updates = mockStatic(SecurityUpdateUtils.class);
	}

	@AfterEach
	void cleanup() {
		updates.close();
		queries.close();
		social.close();
	}

	///
	/// signing in for directory calls
	///

	@Test
	void findDirectoryUser_usesTheUsersMicrosoftLogin() throws Exception {
		AccessToken login = microsoftToken("user-token");
		User user = userWithMicrosoftLogin(login);

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID);

			verify(clients.constructed().getFirst()).findUser(same(login), isNull(), eq("id"), eq(USER_ID),
					anyCollection());
		}
	}

	@Test
	void findDirectoryUser_usesTheApplicationCredentialsWhenConfigured() throws Exception {
		when(properties.getProperty(MicrosoftGraphUserLookup.APPLICATION_CREDENTIALS_PROPERTY)).thenReturn("true");
		User user = mock(User.class);

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID),
				microsoftToken("app-token"))) {
			assertEquals(USER_ID, MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID).get("id"));
			assertEquals(USER_ID, MicrosoftGraphUserLookup.findDirectoryUser(null, USER_ID).get("id"));

			verify(clients.constructed().getFirst()).findUser(isNull(), isNull(), eq("id"), eq(USER_ID),
					anyCollection());
			verify(user, never()).getAccessToken(any());
			verify(user, never()).setAccessToken(any());
		}
	}

	@Test
	void findDirectoryUser_requiresAMicrosoftLoginWithoutApplicationCredentials() throws Exception {
		User nativeOnly = mock(User.class);

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = mockConstruction(
				MicrosoftGraphUserSearchClient.class)) {
			assertEquals(SIGN_IN_REQUIRED, assertThrows(IllegalAccessException.class,
					() -> MicrosoftGraphUserLookup.findDirectoryUser(nativeOnly, USER_ID)).getMessage());
			assertEquals(SIGN_IN_REQUIRED, assertThrows(IllegalAccessException.class,
					() -> MicrosoftGraphUserLookup.findDirectoryUser(null, USER_ID)).getMessage());
			assertEquals(SIGN_IN_REQUIRED, assertThrows(IllegalAccessException.class,
					() -> MicrosoftGraphUserLookup.searchUsers(nativeOnly, "person", null)).getMessage());
			assertTrue(clients.constructed().isEmpty());
		}
	}

	@Test
	void findDirectoryUser_savesARefreshedLoginBackOntoTheUser() throws Exception {
		AccessToken refreshed = microsoftToken("refreshed-token");
		User user = userWithMicrosoftLogin(microsoftToken("old-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID),
				refreshed)) {
			MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID);

			verify(user).setAccessToken(refreshed);
		}
	}

	///
	/// findDirectoryUser
	///

	@Test
	void findDirectoryUser_matchesOnTheGraphIdWithTheDefaultProperties() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			Map<String, Object> found = MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID);

			assertEquals(USER_ID, found.get("id"));
			assertEquals(DISPLAY_NAME, found.get("name"));
			assertEquals(MAIL, found.get("email"));
			assertEquals(PRINCIPAL_NAME, found.get("username"));
			assertEquals(AuthProvider.MICROSOFT.name(), found.get("type"));
			assertEquals(DEFAULT_SELECT, capturedSelect(clients, "id"));
		}
	}

	@Test
	void findDirectoryUser_usesTheMappedIdPropertyAndSelectsTheMappedFields() throws Exception {
		when(properties.getProperty(MicrosoftGraphUserLookup.JSON_PATTERN_PROPERTY)).thenReturn(
				"{\"id\":\"mail\",\"name\":\"givenName\",\"email\":\"mail\",\"username\":\"userPrincipalName\"}");
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));
		String body = "{\"value\":[{\"id\":\"graph-guid\",\"mail\":\"" + MAIL + "\",\"givenName\":\"Person\","
				+ "\"userPrincipalName\":\"" + PRINCIPAL_NAME + "\"}]}";

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(body, null)) {
			Map<String, Object> found = MicrosoftGraphUserLookup.findDirectoryUser(user, MAIL);

			assertEquals(MAIL, found.get("id"));
			assertEquals("Person", found.get("name"));
			List<String> select = capturedSelect(clients, "mail");
			assertEquals(DEFAULT_SELECT, select.subList(0, DEFAULT_SELECT.size()));
			assertTrue(select.contains("givenName"));
			assertEquals(select.size(), select.stream().distinct().count());
		}
	}

	@Test
	void findDirectoryUser_scopesTheLookupToTheConfiguredGroup() throws Exception {
		when(properties.getProperty(MicrosoftGraphUserLookup.GROUP_ID_PROPERTY)).thenReturn("  group-guid  ");
		AccessToken login = microsoftToken("user-token");

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			MicrosoftGraphUserLookup.findDirectoryUser(userWithMicrosoftLogin(login), USER_ID);

			verify(clients.constructed().getFirst()).findUser(same(login), eq("group-guid"), eq("id"), eq(USER_ID),
					anyCollection());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "{\"value\":[]}", "{}" })
	void findDirectoryUser_returnsNullWhenTheDirectoryHasNoSuchUser(String body) throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(body, null)) {
			assertNull(MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID));
		}
	}

	@Test
	void findDirectoryUser_returnsNullWhenTheReturnedIdIsNotExactlyTheSame() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(
				graphUserJson(USER_ID.toUpperCase()), null)) {
			assertNull(MicrosoftGraphUserLookup.findDirectoryUser(user, USER_ID));
		}
	}

	///
	/// addMissingUser
	///

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "   " })
	void addMissingUser_ignoresABlankId(String userId) throws Exception {
		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = mockConstruction(
				MicrosoftGraphUserSearchClient.class)) {
			assertFalse(MicrosoftGraphUserLookup.addMissingUser(mock(User.class), userId));
			assertTrue(clients.constructed().isEmpty());
		}
	}

	@Test
	void addMissingUser_returnsFalseForAUserAlreadyInTheSecurityDatabase() throws Exception {
		queries.when(() -> SecurityQueryUtils.checkUserExist(USER_ID)).thenReturn(true);

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = mockConstruction(
				MicrosoftGraphUserSearchClient.class)) {
			assertFalse(MicrosoftGraphUserLookup.addMissingUser(mock(User.class), USER_ID));
			assertTrue(clients.constructed().isEmpty());
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()), never());
	}

	@Test
	void addMissingUser_storesTheDirectoryDetailsNotTheRequestValues() throws Exception {
		queries.when(() -> SecurityQueryUtils.checkUserExist(USER_ID)).thenReturn(false, true);
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			assertTrue(MicrosoftGraphUserLookup.addMissingUser(user, "  " + USER_ID + "  "));

			verify(clients.constructed().getFirst()).findUser(any(), isNull(), eq("id"), eq(USER_ID), anyCollection());
		}

		ArgumentCaptor<AccessToken> added = ArgumentCaptor.forClass(AccessToken.class);
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(added.capture()));
		assertEquals(USER_ID, added.getValue().getId());
		assertEquals(DISPLAY_NAME, added.getValue().getName());
		assertEquals(MAIL, added.getValue().getEmail());
		assertEquals(PRINCIPAL_NAME, added.getValue().getUsername());
		assertEquals(AuthProvider.MICROSOFT, added.getValue().getProvider());
	}

	@Test
	void addMissingUser_rejectsAUserWhoIsNotInTheDirectory() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns("{\"value\":[]}", null)) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> MicrosoftGraphUserLookup.addMissingUser(user, USER_ID));

			assertEquals("User " + USER_ID + " is not in your organization's directory", e.getMessage());
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()), never());
	}

	@Test
	void addMissingUser_turnsAFailedGraphCallIntoABadRequest() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = mockConstruction(
				MicrosoftGraphUserSearchClient.class,
				(client, context) -> when(client.findUser(any(), any(), anyString(), anyString(), anyCollection()))
						.thenThrow(new IOException("connection reset")))) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> MicrosoftGraphUserLookup.addMissingUser(user, USER_ID));

			assertEquals("Could not look up user " + USER_ID + " in your organization's directory. Try again.",
					e.getMessage());
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()), never());
	}

	@Test
	void addMissingUser_passesAMissingMicrosoftLoginThrough() throws Exception {
		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = mockConstruction(
				MicrosoftGraphUserSearchClient.class)) {
			IllegalAccessException e = assertThrows(IllegalAccessException.class,
					() -> MicrosoftGraphUserLookup.addMissingUser(mock(User.class), USER_ID));

			assertEquals(SIGN_IN_REQUIRED, e.getMessage());
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()), never());
	}

	@Test
	void addMissingUser_failsWhenTheUserIsStillMissingAfterTheAdd() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> MicrosoftGraphUserLookup.addMissingUser(user, USER_ID));

			assertEquals("Could not add user " + USER_ID + " from your organization's directory", e.getMessage());
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()));
	}

	///
	/// addMissingUsers
	///

	@Test
	void addMissingUsers_countsOnlyTheUsersItAdds() throws Exception {
		queries.when(() -> SecurityQueryUtils.checkUserExist("existing")).thenReturn(true);
		queries.when(() -> SecurityQueryUtils.checkUserExist(USER_ID)).thenReturn(false, true);
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));
		List<Map<String, Object>> request = new ArrayList<>();
		request.add(Map.of("userid", "existing", "permission", "READ_ONLY"));
		request.add(Map.of("userid", " " + USER_ID + " ", "permission", "EDIT"));
		request.add(Map.of("userid", "  ", "permission", "EDIT"));
		Map<String, Object> noId = new HashMap<>();
		noId.put("permission", "EDIT");
		request.add(noId);

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns(graphUserJson(USER_ID), null)) {
			assertEquals(1, MicrosoftGraphUserLookup.addMissingUsers(user, request));
			assertEquals(1, clients.constructed().size());
		}
	}

	@Test
	void addMissingUsers_acceptsNoRequest() throws Exception {
		assertEquals(0, MicrosoftGraphUserLookup.addMissingUsers(mock(User.class), null));
		assertEquals(0, MicrosoftGraphUserLookup.addMissingUsers(mock(User.class), List.of()));
	}

	@Test
	void addMissingUsers_stopsAtAUserWhoIsNotInTheDirectory() throws Exception {
		User user = userWithMicrosoftLogin(microsoftToken("user-token"));
		List<Map<String, Object>> request = List.of(Map.of("userid", USER_ID));

		try (MockedConstruction<MicrosoftGraphUserSearchClient> clients = graphReturns("{\"value\":[]}", null)) {
			assertThrows(IllegalArgumentException.class, () -> MicrosoftGraphUserLookup.addMissingUsers(user, request));
		}
		updates.verify(() -> SecurityUpdateUtils.addOAuthUser(any()), never());
	}

	private static MockedConstruction<MicrosoftGraphUserSearchClient> graphReturns(String body, AccessToken usedToken) {
		return mockConstruction(MicrosoftGraphUserSearchClient.class,
				(client, context) -> when(client.findUser(any(), any(), anyString(), anyString(), anyCollection()))
						.thenReturn(new MicrosoftGraphUserSearchClient.GraphApiResponse(body, usedToken)));
	}

	@SuppressWarnings("unchecked")
	private static List<String> capturedSelect(MockedConstruction<MicrosoftGraphUserSearchClient> clients,
			String idProperty) throws Exception {
		ArgumentCaptor<Collection<String>> select = ArgumentCaptor.forClass(Collection.class);
		verify(clients.constructed().getFirst()).findUser(any(), any(), eq(idProperty), anyString(), select.capture());
		return new ArrayList<>(select.getValue());
	}

	private static String graphUserJson(String id) {
		return "{\"value\":[{\"id\":\"" + id + "\",\"displayName\":\"" + DISPLAY_NAME + "\",\"mail\":\"" + MAIL
				+ "\",\"userPrincipalName\":\"" + PRINCIPAL_NAME + "\"}]}";
	}

	private static User userWithMicrosoftLogin(AccessToken login) {
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.MICROSOFT)).thenReturn(login);
		return user;
	}

	private static AccessToken microsoftToken(String value) {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setAccess_token(value);
		token.setExpires_in(3_600);
		token.setStartTime(System.currentTimeMillis());
		return token;
	}
}
