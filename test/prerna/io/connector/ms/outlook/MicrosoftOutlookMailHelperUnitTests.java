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
 *
 * 	You should have received a copy of the GNU General Public License
 * 	along with this program; if not, write to the Free Software
 * 	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * 	MA  02110-1301, USA.
 *******************************************************************************/
package prerna.io.connector.ms.outlook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.security.HttpHelperUtility;

class MicrosoftOutlookMailHelperUnitTests {

	private final MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
	private final MicrosoftOutlookMailHelper.MessageQuery query = new MicrosoftOutlookMailHelper.MessageQuery();
	private MockedStatic<HttpHelperUtility> http;
	private String request;
	private String response = "{\"value\":[]}";

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					request = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					return response;
				});
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void boundsFolderReadsInclusivelyWhilePreservingPaginationAndNewestFirstOrder() {
		query.since = Date.from(Instant.parse("2026-10-12T12:00:00.001Z"));
		query.until = Date.from(Instant.parse("2026-10-13T12:00:00Z"));
		query.top = 25;
		query.skip = 50;
		helper.listMessages("test-token", null, query);
		assertTrue(request.contains("/me/mailFolders/inbox/messages?"));
		assertTrue(request.contains("&$top=25&$skip=50"));
		assertTrue(request.contains("&$filter=receivedDateTime ge 2026-10-12T12:00:00.001Z"
				+ " and receivedDateTime le 2026-10-13T12:00:00Z&$orderby=receivedDateTime desc"));
		assertTrue(request.contains("parentFolderId"));
	}

	@Test
	void upperBoundPrecedesUnreadFilterWithoutAddingADummyLowerBound() {
		query.until = Date.from(Instant.parse("2026-10-13T12:00:00Z"));
		query.unreadOnly = true;
		query.includeBody = false;
		helper.listMessages("test-token", null, query);
		assertTrue(request.contains("&$filter=receivedDateTime le 2026-10-13T12:00:00Z"
				+ " and isRead eq false&$orderby=receivedDateTime desc"));
		assertFalse(request.contains("1900-01-01"));
		assertTrue(request.contains("parentFolderId"));
	}

	@Test
	void unsetUpperBoundPreservesExistingFolderFilters() {
		helper.listMessages("test-token", null, query);
		assertFalse(request.contains("$filter="));
		assertTrue(request.contains("&$orderby=receivedDateTime desc"));
		query.unreadOnly = true;
		helper.listMessages("test-token", null, query);
		assertTrue(request.contains("&$filter=receivedDateTime ge 1900-01-01T00:00:00Z"
				+ " and isRead eq false&$orderby=receivedDateTime desc"));
	}

	@Test
	void textSearchAppliesInclusiveDateBoundsInMemoryWithoutUnsupportedQueryParameters() {
		query.subject = "daily brief";
		query.since = Date.from(Instant.parse("2026-10-12T12:00:00Z"));
		query.until = Date.from(Instant.parse("2026-10-13T12:00:00Z"));
		query.skip = 50;
		response = new Gson().toJson(Map.of("value", List.of(
				Map.of("id", "before", "receivedDateTime", "2026-10-12T11:59:59.999Z"),
				Map.of("id", "at-start", "receivedDateTime", "2026-10-12T12:00:00Z"),
				Map.of("id", "at-end", "receivedDateTime", "2026-10-13T12:00:00Z"),
				Map.of("id", "after", "receivedDateTime", "2026-10-13T12:00:00.001Z"))));
		List<Map<String, Object>> messages = helper.listMessages("test-token", null, query);
		assertEquals(List.of("at-start", "at-end"), messages.stream().map(message -> message.get("id")).toList());
		assertTrue(request.contains("&$search=\"subject:daily brief\""));
		assertFalse(request.contains("$filter="));
		assertFalse(request.contains("$orderby="));
		assertFalse(request.contains("$skip="));
	}

	@Test
	void conversationReadsContinueIgnoringDateBoundsAndOtherFilters() {
		query.conversationId = "conversation'one";
		query.since = Date.from(Instant.parse("2026-10-12T12:00:00Z"));
		query.until = Date.from(Instant.parse("2026-10-13T12:00:00Z"));
		query.subject = "ignored";
		query.unreadOnly = true;
		helper.listMessages("test-token", null, query);
		assertTrue(request.contains("/me/messages?"));
		assertTrue(request.endsWith("&$filter=conversationId eq 'conversation''one'"));
		assertFalse(request.contains("$orderby="));
		assertFalse(request.contains("$search="));
	}
}
