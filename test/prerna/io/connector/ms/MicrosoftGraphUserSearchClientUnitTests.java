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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.security.HttpHelperUtility;

/**
 * Checks the Graph request {@link MicrosoftGraphUserSearchClient#findUser}
 * sends, with the HTTP call replaced by a static mock that records the uri and
 * headers. The delegated token is current, so no refresh or system credentials
 * are used.
 */
class MicrosoftGraphUserSearchClientUnitTests {

	private static final String GRAPH_USERS = "https://graph.microsoft.com/v1.0/users";
	private static final String GRAPH_GROUP_MEMBERS = "https://graph.microsoft.com/v1.0/groups/group-guid/members/microsoft.graph.user";
	private static final String RESPONSE = "{\"value\":[]}";
	private static final String ACCESS_TOKEN = "delegated-token";
	private static final List<String> SELECT = List.of("id", "displayName", "mail");

	private MockedStatic<HttpHelperUtility> http;
	private final AtomicReference<String> sentUri = new AtomicReference<>();
	private final AtomicReference<Map<String, String>> sentHeaders = new AtomicReference<>();
	private AccessToken token;

	@BeforeEach
	void setup() {
		token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setAccess_token(ACCESS_TOKEN);
		token.setExpires_in(3_600);
		token.setStartTime(System.currentTimeMillis());

		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					sentUri.set(call.getArgument(0));
					sentHeaders.set(call.getArgument(1));
					return RESPONSE;
				});
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void findUser_searchesTheTenantsMembersWithoutAGroup() throws Exception {
		new MicrosoftGraphUserSearchClient().findUser(token, null, "id", "abc-123", SELECT);

		assertEquals(GRAPH_USERS, path());
		Map<String, String> params = queryParams();
		assertEquals("id eq 'abc-123' and userType eq 'Member'", params.get("$filter"));
		assertEquals("id,displayName,mail", params.get("$select"));
		assertEquals("1", params.get("$top"));
		assertEquals("true", params.get("$count"));
		assertEquals(4, params.size());
	}

	@Test
	void findUser_treatsAnEmptyGroupIdAsTheTenant() throws Exception {
		new MicrosoftGraphUserSearchClient().findUser(token, "", "id", "abc-123", SELECT);

		assertEquals(GRAPH_USERS, path());
		assertTrue(queryParams().get("$filter").endsWith(" and userType eq 'Member'"));
	}

	@Test
	void findUser_searchesTheGroupsMembersWithoutTheUserTypeClause() throws Exception {
		new MicrosoftGraphUserSearchClient().findUser(token, "group-guid", "mail", "person@contoso.com", SELECT);

		assertEquals(GRAPH_GROUP_MEMBERS, path());
		Map<String, String> params = queryParams();
		assertEquals("mail eq 'person@contoso.com'", params.get("$filter"));
		assertEquals("id,displayName,mail", params.get("$select"));
		assertEquals("1", params.get("$top"));
		assertEquals("true", params.get("$count"));
	}

	@Test
	void findUser_doublesSingleQuotesInTheValue() throws Exception {
		new MicrosoftGraphUserSearchClient().findUser(token, null, "mail", "o'brien'@contoso.com", SELECT);

		assertEquals("mail eq 'o''brien''@contoso.com' and userType eq 'Member'", queryParams().get("$filter"));
	}

	@Test
	void findUser_encodesTheQueryParameters() throws Exception {
		new MicrosoftGraphUserSearchClient().findUser(token, null, "id", "a b&c=d", SELECT);

		String query = sentUri.get().substring(sentUri.get().indexOf('?') + 1);
		assertFalse(query.contains(" "));
		assertEquals(4, query.split("&").length);
		assertEquals("id eq 'a b&c=d' and userType eq 'Member'", queryParams().get("$filter"));
	}

	@Test
	void findUser_sendsTheDelegatedTokenAndTheAdvancedQueryHeaders() throws Exception {
		MicrosoftGraphUserSearchClient.GraphApiResponse response = new MicrosoftGraphUserSearchClient().findUser(token,
				null, "id", "abc-123", SELECT);

		assertEquals("Bearer " + ACCESS_TOKEN, sentHeaders.get().get("Authorization"));
		assertEquals("eventual", sentHeaders.get().get("ConsistencyLevel"));
		assertEquals("application/json", sentHeaders.get().get("Accept"));
		assertEquals(RESPONSE, response.getResponseBody());
		assertSame(token, response.getAccessToken());
	}

	@Test
	void findUser_passesOtherRequestFailuresThrough() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenThrow(new IllegalArgumentException("Request_BadRequest"));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> new MicrosoftGraphUserSearchClient().findUser(token, null, "id", "abc-123", SELECT));
		assertEquals("Request_BadRequest", e.getMessage());
	}

	private String path() {
		String uri = sentUri.get();
		return uri.substring(0, uri.indexOf('?'));
	}

	/**
	 * @return the decoded query parameters of the uri sent, in order
	 */
	private Map<String, String> queryParams() {
		String uri = sentUri.get();
		Map<String, String> params = new LinkedHashMap<>();
		for (String pair : uri.substring(uri.indexOf('?') + 1).split("&")) {
			int equals = pair.indexOf('=');
			params.put(pair.substring(0, equals),
					URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
		}
		return params;
	}
}
