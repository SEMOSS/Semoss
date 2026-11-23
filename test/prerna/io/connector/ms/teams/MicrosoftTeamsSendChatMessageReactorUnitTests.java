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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.security.HttpHelperUtility;

class MicrosoftTeamsSendChatMessageReactorUnitTests {

	private static final Gson GSON = new Gson();

	@Test
	void messagingTheSamePeopleAgainUsesTheirGroupChat() {
		List<String> posts = new ArrayList<>();
		try (MockedStatic<MicrosoftLoginUtils> login = mockStatic(MicrosoftLoginUtils.class);
				MockedStatic<HttpHelperUtility> http = mockStatic(HttpHelperUtility.class, invocation -> {
					String url = invocation.getArgument(0);
					if (invocation.getMethod().getName().equals("postRequestStringBody")) {
						posts.add(url.substring(url.indexOf("/chats")));
						return url.endsWith("/chats") ? GSON.toJson(Map.of("id", "19:new", "chatType", "group"))
								: GSON.toJson(Map.of("id", "m1", "body", Map.of("contentType", "text", "content", "Hi")));
					}
					if (url.contains("/me?")) {
						return GSON.toJson(Map.of("id", "me-id"));
					}
					if (url.contains("/users/")) {
						String who = url.contains("alex") ? "alex" : "bea";
						return GSON.toJson(Map.of("id", who + "-id", "displayName", who));
					}
					// the chats Bea is in: a meeting and a group chat with all three, then the
					// group chat with exactly the three of them
					return GSON.toJson(Map.of("value",
							List.of(chat("19:meeting", "meeting", "me-id", "alex-id", "bea-id"),
									chat("19:bigger", "group", "me-id", "alex-id", "bea-id", "cat-id"),
									chat("19:ours", "group", "me-id", "alex-id", "bea-id"))));
				})) {
			login.when(() -> MicrosoftLoginUtils.getValidAccessToken(any())).thenReturn("token");
			login.when(() -> MicrosoftLoginUtils.getMicrosoftEmail(any())).thenReturn("me@contoso.com");

			Map<?, ?> sent = send("alex@contoso.com,bea@contoso.com");

			assertEquals("19:ours", sent.get("chatId"));
			// no new chat is started
			assertEquals(List.of("/chats/19:ours/messages"), posts);
		}
	}

	private static Map<String, Object> chat(String id, String chatType, String... userIds) {
		return Map.of("id", id, "chatType", chatType, "members",
				Arrays.stream(userIds).map(userId -> Map.of("userId", userId)).toList());
	}

	private static Map<?, ?> send(String recipients) {
		AccessToken token = new AccessToken();
		token.setAccess_token("token");
		User user = mock(User.class);
		when(user.getAccessToken(AuthProvider.MICROSOFT)).thenReturn(token);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		MicrosoftTeamsSendChatMessageReactor reactor = new MicrosoftTeamsSendChatMessageReactor();
		reactor.setInsight(insight);
		NounStore store = new NounStore("all");
		store.makeNoun("recipients").addLiteral(recipients);
		store.makeNoun("message").addLiteral("Hi");
		reactor.setNounStore(store);
		return (Map<?, ?>) reactor.execute().getValue();
	}
}
