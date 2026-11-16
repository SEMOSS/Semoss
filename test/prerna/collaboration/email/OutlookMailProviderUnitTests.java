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
package prerna.collaboration.email;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.security.HttpHelperUtility;

class OutlookMailProviderUnitTests {

	private static final Gson GSON = new Gson();

	@Test
	void aReplyGivenOnlyCcStillGoesToWhoeverItAnswers() throws Exception {
		Map<String, Object> sent = replyUpdate(new String[0], new String[] { "boss@example.com" });
		// the to list Outlook wrote is kept rather than emptied
		assertFalse(sent.containsKey("toRecipients"), String.valueOf(sent));
		assertEquals(List.of(address("boss@example.com")), sent.get("ccRecipients"));
	}

	@Test
	void aReplyGivenOnlyToGoesToJustThem() throws Exception {
		Map<String, Object> sent = replyUpdate(new String[] { "dana@example.com" }, new String[0]);
		// not copied to everyone the thread had on cc
		assertEquals(List.of(address("dana@example.com")), sent.get("toRecipients"));
		assertEquals(List.of(), sent.get("ccRecipients"));
	}

	// the update a reply with these lists makes to the reply-all draft Outlook wrote
	private static Map<String, Object> replyUpdate(String[] to, String[] cc) throws Exception {
		String[] update = new String[1];
		try (MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class);
				MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
					if (invocation.getMethod().getName().equals("postRequestStringBody")) {
						// the reply-all draft as Outlook writes it, to the sender with the thread's cc
						return GSON.toJson(Map.of("id", "draft-1", "body",
								Map.of("contentType", "html", "content", "<p>Quote</p>"), "toRecipients",
								List.of(address("sender@example.com")), "ccRecipients",
								List.of(address("team@example.com"))));
					}
					// Graph applies the update over the draft it wrote
					update[0] = invocation.getArgument(2);
					Map<String, Object> draft = new LinkedHashMap<>(Map.of("id", "draft-1", "toRecipients",
							List.of(address("sender@example.com")), "ccRecipients", List.of(), "bccRecipients", List.of()));
					draft.putAll(GSON.fromJson(update[0], new TypeToken<Map<String, Object>>() {
					}.getType()));
					return GSON.toJson(draft);
				})) {
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("token");

			String id = new OutlookMailProvider(mock(User.class))
					.saveDraft(new OutgoingEmail(to, cc, new String[0], null, "Looping in my boss.", "source"));

			assertEquals("draft-1", id);
			return GSON.fromJson(update[0], new TypeToken<Map<String, Object>>() {
			}.getType());
		}
	}

	private static Map<String, Object> address(String address) {
		return Map.of("emailAddress", Map.of("address", address));
	}
}
