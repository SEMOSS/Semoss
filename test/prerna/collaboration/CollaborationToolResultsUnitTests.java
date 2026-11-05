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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class CollaborationToolResultsUnitTests {

	private static String mailPage(int count, int bodyChars) {
		List<Map<String, Object>> messages = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			messages.add(Map.of("id", "m" + i, "subject", "Subject " + i, "body", "x".repeat(bodyChars)));
		}
		return new JSONObject(Map.of("folder", "inbox", "count", count, "messages", messages)).toString();
	}

	@Test
	void aLargeMailPageSharesOneBodyBudget() {
		JSONObject result = new JSONObject(CollaborationToolResults.forModel("ListM365Mail", mailPage(50, 10_000), null));
		JSONArray messages = result.getJSONArray("messages");
		int share = CollaborationToolResults.MAIL_PAGE_BODY_CHARS / 50;
		assertEquals(50, messages.length());
		for (int i = 0; i < messages.length(); i++) {
			assertEquals("x".repeat(share) + "...", messages.getJSONObject(i).getString("body"));
		}
		assertTrue(result.getString("note").contains(share + " characters"));
	}

	@Test
	void aSmallMailPageKeepsItsBodies() {
		JSONObject result = new JSONObject(CollaborationToolResults.forModel("ListM365Mail", mailPage(3, 5_000), null));
		JSONArray messages = result.getJSONArray("messages");
		for (int i = 0; i < messages.length(); i++) {
			assertEquals(5_000, messages.getJSONObject(i).getString("body").length());
		}
		assertFalse(result.has("note"));
	}
}
