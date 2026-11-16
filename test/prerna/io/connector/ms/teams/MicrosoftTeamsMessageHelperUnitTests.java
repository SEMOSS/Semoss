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
package prerna.io.connector.ms.teams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.security.HttpHelperUtility;

class MicrosoftTeamsMessageHelperUnitTests {

	private static final Gson GSON = new Gson();

	private MockedStatic<HttpHelperUtility> http;
	private final List<String> requests = new ArrayList<>();
	private final List<String> responses = new ArrayList<>();

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class);
		http.when(() -> HttpHelperUtility.postRequestStringBody(anyString(), anyMap(), anyString(),
				any(ContentType.class), isNull(), isNull(), isNull())).thenAnswer(call -> {
					requests.add(call.getArgument(2));
					return responses.isEmpty() ? page(List.of(), false) : responses.remove(0);
				});
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void chatsOnlyLeavesChannelPostsOutAndCountsEveryHitReadTowardTheNextOffset() throws Exception {
		responses.add(page(List.of(chatHit("a", "2026-10-01T09:00:00Z"), channelHit("b"),
				chatHit("c", "2026-10-20T09:00:00Z"), chatHit("d", "2026-10-05T09:00:00Z")), true));

		MicrosoftTeamsMessageHelper.MessageSearchPage page = MicrosoftTeamsMessageHelper.searchMessages("token",
				"budget", 0, 2, true);

		assertEquals(List.of("c", "a"), page.messages().stream().map(message -> message.get("id")).toList());
		assertEquals(3, page.nextOffset());
		assertTrue(page.hasMore());
	}

	@Test
	void theNextPageStartsAtTheOffsetGivenAndReadsOnPastChannelPosts() throws Exception {
		responses.add(page(List.of(channelHit("x"), channelHit("y")), true));
		responses.add(page(List.of(chatHit("z", "2026-10-02T09:00:00Z")), false));

		MicrosoftTeamsMessageHelper.MessageSearchPage page = MicrosoftTeamsMessageHelper.searchMessages("token",
				"budget", 25, 5, true);

		assertEquals(List.of("z"), page.messages().stream().map(message -> message.get("id")).toList());
		assertEquals(28, page.nextOffset());
		assertFalse(page.hasMore());
		assertTrue(requests.get(0).contains("\"from\":25"));
		assertTrue(requests.get(1).contains("\"from\":27"));
		assertTrue(requests.get(0).contains("\"entityTypes\":[\"chatMessage\"]"));
	}

	@Test
	void withChannelsTheSizeAskedForIsWhatThePageStillHolds() throws Exception {
		responses.add(page(List.of(chatHit("a", "2026-10-01T09:00:00Z"), channelHit("b")), true));

		MicrosoftTeamsMessageHelper.MessageSearchPage page = MicrosoftTeamsMessageHelper.searchMessages("token",
				"budget", 0, 2, false);

		assertEquals(List.of("a", "b"), page.messages().stream().map(message -> message.get("id")).toList());
		assertEquals(2, page.nextOffset());
		assertTrue(page.hasMore());
		assertTrue(requests.get(0).contains("\"size\":2"));
	}

	@Test
	void noHitsEndTheSearch() throws Exception {
		MicrosoftTeamsMessageHelper.MessageSearchPage page = MicrosoftTeamsMessageHelper.searchMessages("token",
				"nothing", 10, 5, true);

		assertTrue(page.messages().isEmpty());
		assertEquals(10, page.nextOffset());
		assertFalse(page.hasMore());
	}

	@Test
	void pastedContentIsStreamedToTheNameAskedForAndAnswersAsBefore(@TempDir Path folder) throws Exception {
		servePastedContent(false);

		Map<String, Object> result = MicrosoftTeamsMessageHelper.downloadAttachment("token", "19:a@thread.v2", null,
				null, "m1", null, null, folder.toString(), "screenshot.png");
		assertEquals(Map.of("attachmentId", "h1", "name", "screenshot.png", "filePath", "screenshot.png", "size", 5,
				"success", true), result);
		assertEquals("hello", Files.readString(folder.resolve("screenshot.png")));
		assertEquals(List.of("screenshot.png"), List.of(folder.toFile().list()));
		http.verify(() -> HttpHelperUtility.getRequestBytes(any(), any(), any(), any(), any()), never());
	}

	@Test
	void pastedContentThatRunsPastTheCapIsDeletedAndRefused(@TempDir Path folder) {
		servePastedContent(true);

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> MicrosoftTeamsMessageHelper.downloadAttachment("token", "19:a@thread.v2", null, null, "m1", null,
						null, folder.toString(), "screenshot.png"));
		assertEquals("screenshot.png is larger than the 100 MB limit for a download.", refused.getMessage());
		assertEquals(0, folder.toFile().list().length);
	}

	@Test
	void aPastedImageIsFoundThroughTheMessagesOwnListing(@TempDir Path folder) throws Exception {
		servePastedContent(false);

		Map<String, Object> result = MicrosoftTeamsMessageHelper.downloadAttachment("token", "19:a@thread.v2", null,
				null, "m1", null, null, folder.toString(), null);
		assertEquals("h1", result.get("attachmentId"));
		http.verify(() -> HttpHelperUtility.getRequestFileDownload(endsWith("/messages/m1/hostedContents/h1/$value"),
				any(), isNull(), isNull(), isNull(), anyString(), anyString()));
	}

	@Test
	void readingAMessageListsThePastedImagesItShows() throws Exception {
		servePastedContent(false);

		Map<String, Object> message = MicrosoftTeamsMessageHelper.getChatMessage("token", "19:a@thread.v2", "m1", 0);
		assertEquals(List.of(Map.of("id", "h1")), message.get("hostedContents"));
	}

	@Test
	void plainTextIsSentAsWrittenAndEscapedOnlyInsideHtml() throws Exception {
		MicrosoftTeamsMessageHelper.sendChatMessage("token", "48:notes", "Q&A <draft>", false, null, null, null, 0);
		MicrosoftTeamsMessageHelper.sendChatMessage("token", "48:notes", "Q&A <draft>", false, null,
				new String[] { "https://contoso.sharepoint.com/a.docx" }, null, 0);

		assertEquals(Map.of("contentType", "text", "content", "Q&A <draft>"), body(requests.get(0)));
		// beside an attachment the body is html, where the same text has to be escaped
		assertEquals("html", body(requests.get(1)).get("contentType"));
		assertTrue(String.valueOf(body(requests.get(1)).get("content")).startsWith("Q&amp;A &lt;draft&gt;<attachment"));
	}

	private static Map<?, ?> body(String request) {
		return (Map<?, ?>) GSON.fromJson(request, Map.class).get("body");
	}

	// a message holding one pasted image, whose bytes run past the cap when oversized; Graph
	// lists the image only under the message's own hostedContents
	private void servePastedContent(boolean oversized) {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenAnswer(call -> call.<String>getArgument(0).endsWith("/hostedContents")
						? GSON.toJson(Map.of("value", List.of(Map.of("id", "h1"))))
						: GSON.toJson(Map.of("id", "m1", "body", Map.of("contentType", "html", "content",
								"<p><img src=\"https://graph.microsoft.com/v1.0/chats/19:a@thread.v2/messages/m1"
										+ "/hostedContents/h1/$value\"></p>"))));
		// a read into memory
		http.when(() -> HttpHelperUtility.getRequestBytes(anyString(), any(), isNull(), isNull(), isNull()))
				.thenReturn("hello".getBytes(StandardCharsets.UTF_8));
		// a read onto disk
		http.when(() -> HttpHelperUtility.getRequestFileDownload(anyString(), any(), isNull(), isNull(), isNull(),
				anyString(), anyString())).thenAnswer(call -> {
					File file = new File(call.<String>getArgument(5), call.<String>getArgument(6));
					try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
						out.write("hello".getBytes(StandardCharsets.UTF_8));
						if (oversized) {
							// sparse, so nothing that size is written
							out.setLength(100L * 1024L * 1024L + 1);
						}
					}
					return file;
				});
	}

	private static Map<String, Object> chatHit(String id, String createdDateTime) {
		return Map.of("summary", id, "resource", Map.of("id", id, "createdDateTime", createdDateTime, "chatId",
				"19:" + id + "@thread.v2", "channelIdentity", Map.of()));
	}

	private static Map<String, Object> channelHit(String id) {
		return Map.of("summary", id, "resource", Map.of("id", id, "createdDateTime", "2026-09-01T09:00:00Z", "chatId",
				"19:channel@thread.tacv2", "channelIdentity", Map.of("teamId", "t1", "channelId", "c1")));
	}

	private static String page(List<Map<String, Object>> hits, boolean moreResultsAvailable) {
		return GSON.toJson(Map.of("value", List.of(Map.of("hitsContainers",
				List.of(Map.of("hits", hits, "total", hits.size(), "moreResultsAvailable", moreResultsAvailable))))));
	}
}
