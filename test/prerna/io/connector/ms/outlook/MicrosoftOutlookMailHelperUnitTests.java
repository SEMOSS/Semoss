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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;

import java.io.RandomAccessFile;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.io.connector.ConnectorPage;
import prerna.io.connector.mail.AbstractListMailReactor;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.query.parsers.MailSearchParser;
import prerna.security.HttpHelperUtility;

class MicrosoftOutlookMailHelperUnitTests {

	private static final String NEXT_PAGE = "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$skiptoken=";

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
	void aMessageThatOnlyMentionsErrorIsReadAndARealGraphErrorStillThrows() {
		response = "{\"id\":\"m1\",\"subject\":\"error\",\"from\":{\"emailAddress\":{\"name\":\"error\","
				+ "\"address\":\"alerts@test.com\"}}}";
		assertEquals("error", helper.getMessage("test-token", null, "m1").get("subject"));

		response = "{\"error\":{\"code\":\"ErrorItemNotFound\",\"message\":\"The specified object was not found.\"}}";
		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> helper.getMessage("test-token", null, "m1"));
		assertTrue(refused.getMessage().contains("ErrorItemNotFound"), refused.getMessage());
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
	void excludedTermsAreLedByNotAndALoneMinusIsNoExclusion() {
		assertEquals("NOT from:ada AND NOT \\\"weekly digest\\\" AND q3 AND budget", MicrosoftOutlookMailHelper
				.keywordQuery(MailSearchParser.parse("-from:ada -\"weekly digest\" q3 - budget")));
		assertEquals("NOT report", MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("-report")));
		assertEquals("subject:draft AND subject:notes", MicrosoftOutlookMailHelper
				.keywordQuery(MailSearchParser.parse(MailSearchParser.Field.SUBJECT, "-draft notes")));
	}

	@Test
	void graphPropertiesKeepTheirDatesComparisonsRangesAndQuotedValues() {
		assertEquals("received:10/01/2026",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("received:10/01/2026")));
		assertEquals("received>=2026-10-01 AND size>100",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("received>=2026-10-01 size>100")));
		assertEquals("participants:\\\"Ada L\\\"",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("participants:\"Ada L\"")));
		assertEquals("size:1..500000",
				MicrosoftOutlookMailHelper.keywordQuery(MailSearchParser.parse("size:1..500000")));
	}

	@Test
	void aNameGraphDoesNotReadIsSearchedAsWords() {
		assertEquals("Re AND budget AND ticket AND 123 AND label AND Client AND X", MicrosoftOutlookMailHelper
				.keywordQuery(MailSearchParser.parse("Re:budget ticket:123 label:\"Client X\"")));
	}

	@Test
	void textSearchResultsComeBackInTheOrderTheyArrivedNewestFirst() {
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
	void anUnreadSearchReadsGraphsNextPagesUntilItHasEnough() {
		query.terms = MailSearchParser.parse("budget");
		query.unreadOnly = true;
		query.top = 2;
		List<String> urls = new ArrayList<>();
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					urls.add(call.getArgument(0));
					return switch (urls.size()) {
					case 1 -> page("p2", hit("r1", true), hit("r2", true), hit("r3", true));
					case 2 -> page("p3", hit("r4", true), hit("u1", false));
					default -> page("p4", hit("u2", false));
					};
				});
		List<Map<String, Object>> messages = helper.listMessages("test-token", null, query);
		assertEquals(List.of("u2", "u1"), messages.stream().map(message -> message.get("id")).toList());
		// full after the third page, so the fourth is never read
		assertEquals(List.of(NEXT_PAGE + "p2", NEXT_PAGE + "p3"), urls.subList(1, urls.size()));
	}

	@Test
	void aNarrowedSearchSaysThereIsMoreOnlyWhileGraphHasPagesLeft() throws Exception {
		List<String> urls = new ArrayList<>();
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					urls.add(call.getArgument(0));
					return page("p" + (urls.size() + 1), hit("r" + urls.size(), true));
				});
		AbstractListMailReactor.ListMailRequest request = new AbstractListMailReactor.ListMailRequest("inbox", 20, 0,
				MailSearchParser.parse("budget"), true, null, false, null);
		try (MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class)) {
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("test-token");
			ConnectorPage<MailMessage> listed = new MicrosoftOutlookListMailReactor().listMail(null, request);
			assertTrue(listed.items().isEmpty());
			assertTrue(listed.hasMore());
			assertEquals(5, urls.size());

			urls.clear();
			http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
					.thenAnswer(call -> {
						urls.add(call.getArgument(0));
						return urls.size() == 1 ? page("p2", hit("r1", true)) : page(null, hit("u1", false));
					});
			listed = new MicrosoftOutlookListMailReactor().listMail(null, request);
			assertEquals(List.of("u1"), listed.items().stream().map(MailMessage::id).toList());
			assertFalse(listed.hasMore());
		}
	}

	@Test
	void aFolderReadStillReadsOnePageForGraphSkipsOnTheServer() {
		List<String> urls = new ArrayList<>();
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					urls.add(call.getArgument(0));
					return page("p2", hit("r1", true));
				});
		query.unreadOnly = true;
		assertEquals(1, helper.listMessages("test-token", null, query).size());
		assertEquals(1, urls.size());
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
	void aLongPathIsLookedUpRatherThanTakenForAnId() {
		String path = "Inbox/Clients/Contoso/Projects/2026/Deliverables/Final-Reports";
		assertEquals(62, path.length());
		stubFolderTree();
		helper.resolveDestination("test-token", null, path);
		assertTrue(request.contains("/mailFolders"));
	}

	@Test
	void theShallowestFolderOfANameWinsWhereverTheWalkMeetsItFirst() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					if (url.contains("/mailFolders/a-id/childFolders")) {
						return folders(Map.of("id", "c-id", "displayName", "C", "childFolderCount", 1));
					}
					if (url.contains("/mailFolders/c-id/childFolders")) {
						return folders(Map.of("id", "deep-x-id", "displayName", "Reports", "childFolderCount", 0));
					}
					if (url.contains("/mailFolders/b-id/childFolders")) {
						return folders(Map.of("id", "near-x-id", "displayName", "Reports", "childFolderCount", 0));
					}
					return folders(Map.of("id", "a-id", "displayName", "A", "childFolderCount", 1),
							Map.of("id", "b-id", "displayName", "B", "childFolderCount", 1));
				});
		// the walk meets A/C/Reports first, two levels down, and B/Reports after it
		assertEquals("near-x-id", helper.resolveFolderId("test-token", null, "reports"));
		assertEquals("deep-x-id", helper.resolveFolderId("test-token", null, "A/C/Reports"));
	}

	@Test
	void aLevelOfFoldersIsReadAcrossItsPages() {
		List<String> urls = new ArrayList<>();
		String next = "https://graph.microsoft.com/v1.0/me/mailFolders?$skiptoken=page2";
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = call.getArgument(0);
					urls.add(url);
					if (url.equals(next)) {
						return folders(Map.of("id", "c-id", "displayName", "C", "childFolderCount", 0));
					}
					return new Gson().toJson(Map.of("value",
							List.of(Map.of("id", "a-id", "displayName", "A", "childFolderCount", 0),
									Map.of("id", "b-id", "displayName", "B", "childFolderCount", 0)),
							"@odata.nextLink", next));
				});
		List<MicrosoftOutlookMailHelper.FolderNode> tree = helper.listFolderTree("test-token", null);
		assertEquals(List.of("A", "B", "C"), tree.stream().map(MicrosoftOutlookMailHelper.FolderNode::path).toList());
		assertEquals(2, urls.size());
		assertEquals(next, urls.get(1));
	}

	@Test
	void theWalkStopsTenLevelsDown() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					int level = url.contains("/childFolders")
							? Integer.parseInt(url.replaceAll(".*/mailFolders/level-(\\d+)/childFolders.*", "$1")) + 1
							: 0;
					return folders(Map.of("id", "level-" + level, "displayName", "L" + level, "childFolderCount", 1));
				});
		List<MicrosoftOutlookMailHelper.FolderNode> tree = helper.listFolderTree("test-token", null);
		assertEquals(10, tree.size());
		assertEquals(9, tree.get(tree.size() - 1).depth());
	}

	@Test
	void aLevelGraphWillNotReadIsLeftOutAndTheWalkGoesOn() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> {
					String url = URLDecoder.decode(call.getArgument(0), StandardCharsets.UTF_8);
					if (url.contains("/mailFolders/shared-id/childFolders")) {
						return "{\"error\":{\"code\":\"ErrorAccessDenied\",\"message\":\"Access is denied.\"}}";
					}
					return folders(Map.of("id", "shared-id", "displayName", "Shared", "childFolderCount", 2),
							Map.of("id", "sent-id", "displayName", "Sent Items", "childFolderCount", 0));
				});
		List<MicrosoftOutlookMailHelper.FolderNode> tree = helper.listFolderTree("test-token", null);
		assertEquals(List.of("Shared", "Sent Items"),
				tree.stream().map(MicrosoftOutlookMailHelper.FolderNode::path).toList());
	}

	@Test
	void theTopOfTheMailboxThatGraphWillNotReadFailsTheWalk() {
		response = "{\"error\":{\"code\":\"ErrorAccessDenied\",\"message\":\"Access is denied.\"}}";
		assertThrows(IllegalArgumentException.class, () -> helper.listFolderTree("test-token", null));
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

	@Test
	void filesTooLargeToSendAreRefusedBeforeAnyIsRead(@TempDir Path folder) throws Exception {
		Path note = Files.writeString(folder.resolve("note.txt"), "hi");
		Path recording = folder.resolve("recording.mp4");
		try (RandomAccessFile out = new RandomAccessFile(recording.toFile(), "rw")) {
			// sparse, so nothing that size is written
			out.setLength(MicrosoftOutlookMailHelper.maxAttachmentBytes() + 1024 * 1024);
		}

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> MicrosoftOutlookMailHelper
				.fileAttachments(new String[] { note.toString(), recording.toString() }));
		assertEquals("The email's files are 26 MB, over the 25 MB that fit in one message (MS_MAIL_MAX_MESSAGE_MB)",
				refused.getMessage());
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

	/** One page of search results, with Graph's link to the next page unless next is null. */
	@SafeVarargs
	private static String page(String next, Map<String, ?>... messages) {
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("value", List.of(messages));
		if (next != null) {
			page.put("@odata.nextLink", NEXT_PAGE + next);
		}
		return new Gson().toJson(page);
	}

	/** A search hit; unread ones arrive newer the higher their number. */
	private static Map<String, ?> hit(String id, boolean read) {
		return Map.of("id", id, "isRead", read, "receivedDateTime",
				"2026-10-" + (10 + Integer.parseInt(id.substring(1))) + "T09:00:00Z");
	}
}
