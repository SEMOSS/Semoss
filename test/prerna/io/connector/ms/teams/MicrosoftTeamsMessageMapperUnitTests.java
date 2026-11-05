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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MicrosoftTeamsMessageMapperUnitTests {

	@Test
	void aChatHitNamesItsChatAndSender() {
		Map<String, Object> hit = MicrosoftTeamsMessageMapper
				.toSearchHit(Map.of("summary", "...the <c0>budget</c0> for Q3<ddd/>", "resource",
						Map.of("id", "1657782060227", "createdDateTime", "2026-10-14T07:01:01Z", "subject", "", "from",
								Map.of("emailAddress", Map.of("name", "Ada Lovelace", "address", "ada@example.com")),
								"channelIdentity", Map.of(), "chatId", "19:abc@thread.v2")));

		assertEquals("1657782060227", hit.get("id"));
		assertEquals(MicrosoftTeamsMessageMapper.CHAT, hit.get("kind"));
		assertEquals("19:abc@thread.v2", hit.get("chatId"));
		assertEquals("Ada Lovelace", hit.get("fromName"));
		assertEquals("ada@example.com", hit.get("fromEmail"));
		assertEquals("2026-10-14T07:01:01Z", hit.get("createdDateTime"));
		assertEquals("...the budget for Q3...", hit.get("summary"));
		assertFalse(hit.containsKey("subject"));
		assertFalse(hit.containsKey("teamId"));
	}

	@Test
	void aChannelHitNamesItsTeamAndChannelRatherThanAChat() {
		Map<String, Object> hit = MicrosoftTeamsMessageMapper.toSearchHit(
				Map.of("summary", "launch", "resource", Map.of("id", "m1", "chatId", "19:channel@thread.tacv2",
						"channelIdentity", Map.of("teamId", "t1", "channelId", "19:channel@thread.tacv2"))));

		assertEquals(MicrosoftTeamsMessageMapper.CHANNEL, hit.get("kind"));
		assertEquals("t1", hit.get("teamId"));
		assertEquals("19:channel@thread.tacv2", hit.get("channelId"));
		assertFalse(hit.containsKey("chatId"));
	}

	@Test
	void aHitWithoutAMessageIsSkipped() {
		assertNull(MicrosoftTeamsMessageMapper.toSearchHit(Map.of("summary", "x")));
		assertNull(MicrosoftTeamsMessageMapper.toSearchHit(Map.of("resource", Map.of("chatId", "19:abc"))));
	}

	@Test
	void aSummaryLosesItsMarkupAndKeepsWhatWasWritten() {
		assertEquals("Tom & Jerry <script> said ... hi",
				MicrosoftTeamsMessageMapper.searchSummary("Tom &amp; <c0>Jerry</c0> &lt;script&gt; said <ddd/>\n hi"));
		assertEquals("", MicrosoftTeamsMessageMapper.searchSummary(null));
	}

	@Test
	void timestampsSortNewestFirstWithUnreadableOnesLast() {
		List<Object> times = new ArrayList<>(
				Arrays.asList("2026-10-01T00:00:00Z", null, "2026-10-03T00:00:00Z", "soon", "2026-10-02T00:00:00Z"));
		times.sort(MicrosoftTeamsMessageMapper::compareNewestFirst);

		assertEquals(List.of("2026-10-03T00:00:00Z", "2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z"),
				times.subList(0, 3));
		assertTrue(times.subList(3, 5).contains(null) && times.subList(3, 5).contains("soon"));
	}
}
