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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Thread insights: re-reading the same mail adds nothing, and the owner's steps are never rewritten. */
class WorkThreadInsightsUnitTests {

	private static final String ME = "self-person";

	@Test
	void readsAnswersWrappedInReasoningAndFences() {
		WorkThreadInsights.Answer answer = WorkThreadInsights.parse("<think>{\"summary\": 1}</think>\n```json\n"
				+ "{\"summary\": \"Kira needs the budget.\", \"actionItems\": [{\"id\": \"\", \"text\": \"Send it\", "
				+ "\"ownerId\": \"me\", \"due\": \"\", \"done\": false}, {\"text\": \"\"}]}\n```");
		assertNotNull(answer);
		assertEquals("Kira needs the budget.", answer.summary());
		assertNull(WorkThreadInsights.parse("{\"summary\": \"\"}"));
		assertNull(WorkThreadInsights.parse("not json"));
	}

	@Test
	void leavesOutExcludedMessagesAndKeepsTheNewestWithinBudget() {
		List<Map<String, Object>> read = new ArrayList<>();
		for (int i = 0; i < 15; i++) {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("fromId", i % 2 == 0 ? "kira" : ME);
			m.put("at", "2026-10-0" + (i % 9 + 1) + "T10:00:00Z");
			m.put("text", i == 14 ? "newest" : "x".repeat(4000));
			m.put("excluded", i == 13);
			m.put("to", List.of(Map.of("name", "Kira", "address", "kira@example.com")));
			read.add(m);
		}
		List<Map<String, Object>> out = WorkThreadInsights.messages(read, Map.of("p1", "kira"), ME);
		assertEquals("newest", out.get(out.size() - 1).get("text"));
		assertEquals("p1", out.get(out.size() - 1).get("from"));
		// the excluded 14th message is gone: the 13th and then the owner's 12th come before the newest
		assertEquals("p1", out.get(out.size() - 2).get("from"));
		assertEquals("me", out.get(out.size() - 3).get("from"));
		assertTrue(out.stream().mapToInt(m -> ((String) m.get("text")).length()).sum() <= 40000);
		assertTrue(out.size() < 14);
	}

	@Test
	void aSummaryIsCurrentOnlyForTheNewestMessage() {
		assertTrue(WorkThreadInsights.covers("m2", "m2"));
		assertFalse(WorkThreadInsights.covers("m1", "m2"));
		assertFalse(WorkThreadInsights.covers(null, "m2"));
		assertTrue(WorkThreadInsights.covers(null, null));
	}
}
