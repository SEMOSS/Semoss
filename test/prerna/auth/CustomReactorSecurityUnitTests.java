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
package prerna.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;

/**
 * The people search and permission questions custom reactors read through
 * {@link CustomReactorSecurity}.
 */
class CustomReactorSecurityUnitTests {

	private MockedStatic<SecurityQueryUtils> securityQueries;
	private MockedStatic<SecurityEngineUtils> engines;
	private MockedStatic<MicrosoftGraphUserLookup> directory;
	private User user;

	@BeforeEach
	void setUp() {
		this.securityQueries = mockStatic(SecurityQueryUtils.class);
		this.engines = mockStatic(SecurityEngineUtils.class);
		this.directory = mockStatic(MicrosoftGraphUserLookup.class);
		this.user = mock(User.class);
		when(this.user.getLogins()).thenReturn(List.of(AuthProvider.NATIVE));
	}

	@AfterEach
	void tearDown() {
		this.directory.close();
		this.engines.close();
		this.securityQueries.close();
	}

	@Test
	void aSearchNeedsASignedInUser() {
		User signedOut = mock(User.class);
		when(signedOut.getLogins()).thenReturn(List.of());

		assertThrows(IllegalAccessException.class, () -> CustomReactorSecurity.searchUsers(null, "jo", 10, 0));
		assertThrows(IllegalAccessException.class, () -> CustomReactorSecurity.searchUsers(signedOut, "jo", 10, 0));
		this.securityQueries.verifyNoInteractions();
	}

	@Test
	void theSecurityDatabaseSearchReturnsEachPersonsIdTypeNameAndEmail() throws Exception {
		this.securityQueries.when(() -> SecurityQueryUtils.searchForUser("jo")).thenReturn(List.of(person("jo1")));

		List<Map<String, Object>> people = CustomReactorSecurity.searchUsers(this.user, " jo ", 10, 0);

		assertEquals(List.of(Map.of("id", "jo1", "type", "NATIVE", "name", "Name jo1", "email", "jo1@example.com")),
				people);
	}

	@Test
	void aPageIsCutByLimitAndOffset() throws Exception {
		this.securityQueries.when(() -> SecurityQueryUtils.searchForUser("")).thenReturn(people("u", 5));

		assertEquals(List.of("u1", "u2"), ids(CustomReactorSecurity.searchUsers(this.user, null, 2, 1)));
	}

	@Test
	void aPageHoldsAtMostTheLimitCap() throws Exception {
		this.securityQueries.when(() -> SecurityQueryUtils.searchForUser("")).thenReturn(people("u", 150));

		assertEquals(CustomReactorSecurity.MAX_LIMIT, CustomReactorSecurity.searchUsers(this.user, "", 500, 0).size());
		assertEquals(CustomReactorSecurity.MAX_LIMIT, CustomReactorSecurity.searchUsers(this.user, "", 0, 0).size());
	}

	@Test
	void aPagePastTheLastResultIsEmpty() throws Exception {
		assertTrue(CustomReactorSecurity.searchUsers(this.user, "", 10, CustomReactorSecurity.MAX_RESULTS).isEmpty());
		this.securityQueries.verifyNoInteractions();
	}

	@Test
	void theDirectoryIsSearchedWhenTheServerUsesItAndItsPagesAreFollowed() throws Exception {
		this.directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		this.directory.when(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "jo", null))
				.thenReturn(directoryPage(List.of("d0", "d1"), "next"));
		this.directory.when(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "jo", "next"))
				.thenReturn(directoryPage(List.of("d2", "d3"), null));

		List<Map<String, Object>> people = CustomReactorSecurity.searchUsers(this.user, "jo", 3, 0);

		assertEquals(List.of("d0", "d1", "d2"), ids(people));
		assertEquals(Set.of("id", "type", "name", "email"), people.get(0).keySet());
		this.securityQueries.verifyNoInteractions();
	}

	@Test
	void aDirectorySearchStopsOnceThePageIsFull() throws Exception {
		this.directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		this.directory.when(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "", null))
				.thenReturn(directoryPage(List.of("d0", "d1", "d2"), "next"));

		assertEquals(List.of("d0", "d1"), ids(CustomReactorSecurity.searchUsers(this.user, "", 2, 0)));
		this.directory.verify(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "", "next"), never());
	}

	@Test
	void aSearchCanAskForTheSecurityDatabaseWhenTheServerUsesTheDirectory() throws Exception {
		this.directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		this.securityQueries.when(() -> SecurityQueryUtils.searchForUser("jo")).thenReturn(List.of(person("jo1")));

		assertEquals(List.of("jo1"), ids(CustomReactorSecurity.searchUsers(this.user, "jo", 10, 0, false)));
		this.directory.verify(() -> MicrosoftGraphUserLookup.searchUsers(any(), any(), any()), never());
	}

	@Test
	void aDirectorySearchWithoutAMicrosoftLoginSaysSo() {
		this.directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		this.directory.when(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "jo", null))
				.thenThrow(new IllegalAccessException("Sign in with Microsoft to use your organization's directory"));

		assertThrows(IllegalAccessException.class, () -> CustomReactorSecurity.searchUsers(this.user, "jo", 10, 0));
	}

	@Test
	void aDirectoryFailureIsReportedWithoutWhatTheDirectorySaid() {
		this.directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		this.directory.when(() -> MicrosoftGraphUserLookup.searchUsers(this.user, "jo", null))
				.thenThrow(new Exception("detail from the directory"));

		IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> CustomReactorSecurity.searchUsers(this.user, "jo", 10, 0));

		assertFalse(failure.getMessage().contains("detail from the directory"));
	}

	@Test
	void aPermissionQuestionIsAnsweredForTheSignedInUserOnly() {
		this.engines.when(() -> SecurityEngineUtils.userCanViewEngine(this.user, "e1")).thenReturn(true);

		assertTrue(CustomReactorSecurity.userCanViewEngine(this.user, "e1"));
		assertFalse(CustomReactorSecurity.userCanViewEngine(null, "e1"));
		this.engines.verify(() -> SecurityEngineUtils.userCanViewEngine(null, "e1"), never());
	}

	private static Map<String, Object> person(String id) {
		Map<String, Object> person = new HashMap<>();
		person.put("id", id);
		person.put("type", "NATIVE");
		person.put("username", "login-" + id);
		person.put("name", "Name " + id);
		person.put("email", id + "@example.com");
		return person;
	}

	private static List<Map<String, Object>> people(String prefix, int count) {
		List<Map<String, Object>> people = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			people.add(person(prefix + i));
		}
		return people;
	}

	private static MicrosoftGraphUserLookup.UserPage directoryPage(List<String> ids, String nextLink) {
		List<Map<String, Object>> users = new ArrayList<>();
		List<Map<String, Object>> graphUsers = new ArrayList<>();
		for (String id : ids) {
			Map<String, Object> directoryUser = person(id);
			directoryUser.put("type", "MICROSOFT");
			directoryUser.put("jobTitle", "Analyst");
			users.add(directoryUser);
			graphUsers.add(Map.of("id", id, "displayName", "Name " + id));
		}
		return new MicrosoftGraphUserLookup.UserPage(users, graphUsers, nextLink);
	}

	private static List<Object> ids(List<Map<String, Object>> people) {
		return people.stream().map(person -> person.get("id")).toList();
	}
}
