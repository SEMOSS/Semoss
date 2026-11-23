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
package prerna.io.connector.ms.calendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;
import prerna.security.HttpStatusException;

/**
 * Reads meeting transcripts against Graph replies written here, since the real
 * ones need a scope an administrator consents to.
 */
class MicrosoftCalendarGetMeetingTranscriptReactorUnitTests {

	private static final Gson GSON = new Gson();

	/** A join link as an event carries it, already holding escapes of its own. */
	private static final String JOIN_URL = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_abc%40thread.v2/0"
			+ "?context=%7b%22Tid%22%3a%22t%22%7d";

	private static final String MEETING = "/me/onlineMeetings?$filter=";
	private static final String TRANSCRIPTS = "/me/onlineMeetings/MTG1/transcripts";
	private static final String AS_VTT = "?$format=text/vtt";

	/** A transcript the way Teams writes one: cue ids, Windows line ends, a cue over two lines. */
	private static final String VTT = "WEBVTT\r\n\r\n" + "abc/1-0\r\n00:00:01.000 --> 00:00:03.000\r\n"
			+ "<v Ada Lovelace>Hello all.</v>\r\n\r\n" + "abc/2-0\r\n00:00:03.500 --> 00:00:06.000\r\n"
			+ "<v Ada Lovelace>Shall we\r\nstart?</v>\r\n\r\n" + "abc/3-0\r\n00:00:06.500 --> 00:00:07.000\r\n"
			+ "<v Grace Hopper>Yes.</v>\r\n";

	/**
	 * What Graph answers, by a part of the url asked for: a body, or the failure
	 * to throw. The longest part a url holds is the one meant, so a transcript's
	 * content is not taken for the list it is in.
	 */
	private final Map<String, Object> graph = new HashMap<>();
	private final List<String> asked = new ArrayList<>();
	private final List<String> accepted = new ArrayList<>();
	private MockedStatic<HttpHelperUtility> http;

	@BeforeEach
	void setup() {
		http = mockStatic(HttpHelperUtility.class, call -> {
			if (!call.getMethod().getName().equals("getRequest")) {
				return null;
			}
			String url = call.getArgument(0);
			Map<String, String> headers = call.getArgument(1);
			asked.add(url);
			accepted.add(headers.get("Accept"));
			Object answer = graph.entrySet().stream().filter(reply -> url.contains(reply.getKey()))
					.max(Comparator.comparingInt(reply -> reply.getKey().length())).map(Map.Entry::getValue)
					.orElseThrow(() -> new AssertionError("Graph was asked for something unexpected: " + url));
			if (answer instanceof RuntimeException failure) {
				throw failure;
			}
			return answer;
		});
		graph.put("/events/", event(true));
		graph.put(MEETING, values(null, Map.of("id", "MTG1")));
	}

	@AfterEach
	void cleanup() {
		http.close();
	}

	@Test
	void aSittingOfASeriesReadsTheTranscriptThatStartedAroundItsOwnTime() {
		// the series shares one meeting, so last week's transcript is listed too, and
		// this sitting's is on the second page, started two minutes early
		graph.put(TRANSCRIPTS, values("https://graph.microsoft.com/v1.0/next-page",
				Map.of("id", "t-last-week", "createdDateTime", "2026-11-05T15:01:10.1234567Z")));
		graph.put("/next-page", values(null, Map.of("id", "t-own", "createdDateTime", "2026-11-12T14:58:00Z")));
		graph.put(TRANSCRIPTS + "/t-own/content" + AS_VTT, VTT);

		Map<?, ?> output = read(null);

		assertEquals("Weekly sync", output.get("subject"));
		assertEquals("[Transcript started 2026-11-12T14:58:00Z]\nAda Lovelace: Hello all. Shall we start?\n"
				+ "Grace Hopper: Yes.", output.get("transcript"));
		assertEquals(false, output.get("hasMore"));
		assertFalse(output.containsKey("nextOffset"));
		// the link is encoded once more inside the filter, the way Graph documents it
		String lookup = asked.stream().filter(url -> url.contains(MEETING)).findFirst().orElseThrow();
		assertTrue(lookup.endsWith("JoinWebUrl%20eq%20%27https%3A%2F%2Fteams.microsoft.com%2Fl%2Fmeetup-join%2F"
				+ "19%253ameeting_abc%2540thread.v2%2F0%3Fcontext%3D%257b%2522Tid%2522%253a%2522t%2522%257d%27"),
				lookup);
		assertEquals(1, asked.stream().filter(url -> url.contains("/content")).count());
	}

	@Test
	void aOneOffMeetingHeldAtAnotherTimeStillReadsItsNewestTranscripts() {
		graph.put("/events/", event(false));
		// nobody met at the time planned, and the link was met on each of the seven days after
		List<Map<String, Object>> held = new ArrayList<>();
		for (int day = 13; day < 20; day++) {
			held.add(Map.of("id", "t-" + day, "createdDateTime", "2026-11-" + day + "T10:00:00Z"));
			graph.put(TRANSCRIPTS + "/t-" + day + "/content" + AS_VTT, VTT);
		}
		graph.put(TRANSCRIPTS, GSON.toJson(Map.of("value", held)));

		assertTrue(read(null).get("transcript").toString().startsWith("[Transcript started 2026-11-15T10:00:00Z]\n"));
		assertEquals(5, asked.stream().filter(url -> url.contains("/content")).count());
	}

	@Test
	void aSittingNobodyTranscribedSaysSoAndNamesTheOnesThatWere() {
		graph.put(TRANSCRIPTS, values(null, Map.of("id", "t-last-week", "createdDateTime", "2026-11-05T15:01:10Z")));
		Map<?, ?> output = read(null);
		assertFalse(output.containsKey("transcript"));
		assertTrue(output.get("message").toString().contains("2026-11-05T15:01:10Z"), output.toString());

		graph.put(TRANSCRIPTS, values(null));
		assertTrue(read(null).get("message").toString().startsWith("This meeting has no transcript"));
		assertTrue(asked.stream().noneMatch(url -> url.contains("/content")));
	}

	@Test
	void speakerNamesAnOrganizationKeepsBackAreLeftOutInsteadOfFailing() {
		graph.put(TRANSCRIPTS, values(null, Map.of("id", "t-own", "createdDateTime", "2026-11-12T15:00:00Z")));
		graph.put(TRANSCRIPTS + "/t-own/content" + AS_VTT, refused(403, "SpeakerAttributionNotAllowed"));
		graph.put(TRANSCRIPTS + "/t-own/content", VTT.replaceAll("</?v[^>]*>", ""));

		assertEquals("[Transcript started 2026-11-12T15:00:00Z]\nHello all.\nShall we start?\nYes.",
				read(null).get("transcript"));
		assertEquals("application/vnd.microsoft.graph.transcript+text", accepted.get(accepted.size() - 1));
	}

	@Test
	void aLongTranscriptComesBackAPageAtATime() {
		StringBuilder vtt = new StringBuilder("WEBVTT\n\n");
		List<String> said = new ArrayList<>();
		said.add("[Transcript started 2026-11-12T15:00:00Z]");
		for (int cue = 0; cue < 1000; cue++) {
			String speaker = cue % 2 == 0 ? "Ada Lovelace" : "Grace Hopper";
			String words = "Point " + cue + " " + "and so on ".repeat(10).trim();
			vtt.append("00:00:01.000 --> 00:00:02.000\n<v ").append(speaker).append('>').append(words)
					.append("</v>\n\n");
			said.add(speaker + ": " + words);
		}
		graph.put(TRANSCRIPTS, values(null, Map.of("id", "t-own", "createdDateTime", "2026-11-12T15:00:00Z")));
		graph.put(TRANSCRIPTS + "/t-own/content" + AS_VTT, vtt.toString());

		List<String> pages = new ArrayList<>();
		Map<?, ?> page = read(null);
		pages.add(page.get("transcript").toString());
		while (Boolean.TRUE.equals(page.get("hasMore"))) {
			page = read(page.get("nextOffset").toString());
			pages.add(page.get("transcript").toString());
		}

		assertTrue(pages.size() > 1, "one page held " + pages.get(0).length() + " characters");
		assertTrue(pages.stream()
				.allMatch(text -> text.length() <= MicrosoftCalendarGetMeetingTranscriptReactor.PAGE_CHARS));
		assertEquals(String.join("\n", said), String.join("\n", pages));
		// past the end there is nothing more, rather than an error
		assertEquals("", read("5000").get("transcript"));
	}

	@Test
	void whatMicrosoftWillNotReadIsSaidInWords() {
		graph.put(TRANSCRIPTS, refused(403, "GraphAccessToTranscriptsDisabled"));
		assertTrue(failure().contains("turned off reading meeting transcripts"));

		// a sign in without the scopes, or somebody who was not invited
		graph.put(MEETING, refused(403, "Forbidden"));
		assertTrue(failure().contains("OnlineMeetings.Read and OnlineMeetingTranscript.Read.All"));

		graph.put(MEETING, values(null));
		assertTrue(failure().startsWith("Microsoft Teams has no meeting for this event"));

		graph.put("/events/", event(false, false));
		assertEquals("This event has no Teams meeting, so it has no transcript.", failure());
	}

	@Test
	void transcriptsThatAreNotTheUsualShapeAreStillRead() {
		assertEquals(List.of(), MicrosoftCalendarEventMapper.transcriptLines(null));
		assertEquals(List.of(), MicrosoftCalendarEventMapper.transcriptLines("WEBVTT\r\n\r\n"));
		// words that came without cues are kept as they are
		assertEquals(List.of("Hello all.", "Yes."),
				MicrosoftCalendarEventMapper.transcriptLines("Hello all.\n\n  Yes.  \n"));
		// one speaker who goes on starts a new line now and then, under their name again
		String cue = "00:00:01.000 --> 00:00:02.000\n<v Ada Lovelace>" + "x".repeat(400) + "</v>\n\n";
		List<String> lines = MicrosoftCalendarEventMapper.transcriptLines("WEBVTT\n\n" + cue.repeat(5));
		assertEquals(2, lines.size());
		assertTrue(lines.stream().allMatch(line -> line.startsWith("Ada Lovelace: x")));
	}

	/** The event the transcript is asked for, a sitting of a weekly series or a one-off. */
	private static String event(boolean recurring) {
		return event(recurring, true);
	}

	private static String event(boolean recurring, boolean online) {
		Map<String, Object> event = new LinkedHashMap<>();
		event.put("id", "evt-1");
		event.put("subject", "Weekly sync");
		event.put("start", Map.of("dateTime", "2026-11-12T15:00:00.0000000", "timeZone", "UTC"));
		event.put("end", Map.of("dateTime", "2026-11-12T15:30:00.0000000", "timeZone", "UTC"));
		if (online) {
			event.put("isOnlineMeeting", true);
			event.put("onlineMeeting", Map.of("joinUrl", JOIN_URL));
		}
		if (recurring) {
			event.put("type", "occurrence");
			event.put("seriesMasterId", "series-1");
		}
		return GSON.toJson(event);
	}

	@SafeVarargs
	private static String values(String nextLink, Map<String, Object>... values) {
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("value", List.of(values));
		if (nextLink != null) {
			page.put("@odata.nextLink", nextLink);
		}
		return GSON.toJson(page);
	}

	/** A refusal the way the http helper reports one, with Graph's code in the body. */
	private static HttpStatusException refused(int status, String code) {
		return new HttpStatusException("GET request returned HTTP " + status
				+ ". Response body: {\"error\":{\"code\":\"Forbidden\",\"innerError\":{\"code\":\"" + code + "\"}}}",
				status);
	}

	private static String failure() {
		return assertThrows(SemossPixelException.class, () -> read(null)).getMessage();
	}

	private static Map<?, ?> read(String offset) {
		AccessToken token = new AccessToken();
		token.setAccess_token("token");
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.MICROSOFT)).thenReturn(token);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		MicrosoftCalendarGetMeetingTranscriptReactor reactor = new MicrosoftCalendarGetMeetingTranscriptReactor();
		reactor.setInsight(insight);
		NounStore store = new NounStore("all");
		store.makeNoun("id").addLiteral("evt-1");
		if (offset != null) {
			store.makeNoun("offset").addLiteral(offset);
		}
		reactor.setNounStore(store);
		return (Map<?, ?>) reactor.execute().getValue();
	}
}
