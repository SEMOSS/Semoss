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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.query.parsers.MailSearchParser;
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
		response = new Gson().toJson(Map.of("value",
				List.of(Map.of("id", "before", "receivedDateTime", "2026-10-12T11:59:59.999Z"),
						Map.of("id", "at-start", "receivedDateTime", "2026-10-12T12:00:00Z"),
						Map.of("id", "at-end", "receivedDateTime", "2026-10-13T12:00:00Z"),
						Map.of("id", "after", "receivedDateTime", "2026-10-13T12:00:00.001Z"))));
		List<Map<String, Object>> messages = helper.listMessages("test-token", null, query);
		assertEquals(List.of("at-end", "at-start"), messages.stream().map(message -> message.get("id")).toList());
		assertTrue(request.contains("&$search=\"subject:daily AND subject:brief\""));
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

	@Test
	void typedSearchNarrowsEachWordToItsFieldAndKeepsPhrasesTogether() {
		query.terms = MailSearchParser.parse("from:ada subject:budget report \"next quarter\" body:\"due friday\"");
		query.subject = "q3 plan";
		helper.listMessages("test-token", null, query);
		assertTrue(request.contains("&$search=\"from:ada AND subject:budget AND report AND \\\"next quarter\\\""
				+ " AND body:\\\"due friday\\\" AND subject:q3 AND subject:plan\""));
	}

	@Test
	void keywordSyntaxTypedAsTextIsSearchedAsWords() {
		assertEquals("budget AND and AND q3 AND report",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("(budget AND q3) report")));
		assertEquals("Re AND budget", MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("Re: budget")));
		assertEquals("hasattachment:true AND to:bob",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("hasattachment:true to:bob")));
		assertEquals("", MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("\"\" ()")));
	}

	@Test
	void textSearchResultsComeBackNewestFirstRatherThanByRelevance() {
		query.subject = "budget";
		response = new Gson().toJson(Map.of("value",
				List.of(Map.of("id", "older", "receivedDateTime", "2026-10-01T09:00:00Z"),
						Map.of("id", "newest", "receivedDateTime", "2026-10-20T09:00:00Z"),
						Map.of("id", "middle", "receivedDateTime", "2026-10-10T09:00:00Z"))));
		List<Map<String, Object>> messages = helper.listMessages("test-token", null, query);
		assertEquals(List.of("newest", "middle", "older"),
				messages.stream().map(message -> message.get("id")).toList());
	}

	@Test
	void folderTreeListsEachFolderRightBeforeTheFoldersInsideIt() {
		stubFolderTree();
		List<MicrosoftOutlookMailHelper.FolderNode> tree = helper.listFolderTree("test-token", null);
		assertEquals(List.of("Inbox", "Inbox/Projects", "Inbox/Projects/Alpha", "Sent Items"),
				tree.stream().map(MicrosoftOutlookMailHelper.FolderNode::path).toList());
		assertEquals(List.of(0, 1, 2, 0), tree.stream().map(MicrosoftOutlookMailHelper.FolderNode::depth).toList());
		assertNull(tree.get(0).parentId());
		assertEquals("inbox-id", tree.get(1).parentId());
		assertEquals("projects-id", tree.get(2).parentId());
	}

	@Test
	void nestedFoldersResolveByPathOrByTheShallowestFolderOfThatName() {
		stubFolderTree();
		assertEquals("alpha-id", helper.resolveDestination("test-token", null, "inbox/projects/alpha"));
		assertEquals("projects-id", helper.resolveDestination("test-token", null, "Projects"));
		assertEquals("inbox", helper.resolveDestination("test-token", null, "Inbox"));
	}

	@Test
	void folderIdsPassStraightThroughWithoutReadingTheFolderTree() {
		String id = "AAMkAGVmMDEzMTM4LTZmYWUtNDdkNC1hMDZiLTU1OGY5OTZhYmY4OAAuAAAAAAAiQ8W967B7TKBjgx9rVEURAQ=";
		assertEquals(id, helper.resolveDestination("test-token", null, id));
		assertNull(request);
	}

	@Test
	void readsTheBodyAsItWasWrittenRatherThanOutlooksTextRendering() {
		List<Map<String, String>> sent = new ArrayList<>();
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					request = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					sent.add(call.getArgument(1));
					return "{\"body\":{\"contentType\":\"html\",\"content\":\"<p>Hi</p>\"}}";
				});
		Map<String, Object> message = helper.getMessageBody("test-token", null, "m/1");
		assertTrue(request.endsWith("/me/messages/m/1?$select=body"));
		assertFalse(sent.get(0).containsKey("Prefer"));
		assertEquals(Map.of("contentType", "html", "content", "<p>Hi</p>"), message.get("body"));
	}

	private void stubFolderTree() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					if (url.contains("/mailFolders/inbox-id/childFolders")) {
						return folders(Map.of("id", "projects-id", "displayName", "Projects", "childFolderCount", 1));
					}
					if (url.contains("/mailFolders/projects-id/childFolders")) {
						return folders(Map.of("id", "alpha-id", "displayName", "Alpha", "childFolderCount", 0));
					}
					return folders(Map.of("id", "inbox-id", "displayName", "Inbox", "childFolderCount", 1),
							Map.of("id", "sent-id", "displayName", "Sent Items", "childFolderCount", 0));
				});
	}

	@SafeVarargs
	private static String folders(Map<String, ?>... folders) {
		return new Gson().toJson(Map.of("value", List.of(folders)));
	}
}
