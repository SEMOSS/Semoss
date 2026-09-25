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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

// reads one message from Graph with the owner's delegated token, only after the rules let it through
final class BrainGraphMessageSource implements BrainMessageSource {

	private static final String BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final String MAIL_SELECT = "subject,from,body,uniqueBody,receivedDateTime,conversationId";

	@Override
	public Map<String, Object> fetch(User user, String source, String conversationId, String graphId)
			throws Exception {
		if (graphId == null) {
			return null;
		}
		String url;
		if ("email".equals(source)) {
			url = BASE + "/me/messages/" + encode(graphId) + "?$select=" + MAIL_SELECT;
		} else if ("teams".equals(source) && conversationId != null) {
			url = BASE + "/chats/" + encode(conversationId) + "/messages/" + encode(graphId);
		} else {
			return null;
		}
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		Map<String, Object> message = CollaborationDbUtils
				.parseMap(HttpHelperUtility.getRequest(url, MicrosoftLoginUtils.getBearerHeader(token), null, null, null));
		return "teams".equals(source) ? fromChat(message) : message;
	}

	// a chat message has no subject, uniqueBody, or sender address; map it onto the mail shape
	@SuppressWarnings("unchecked")
	private static Map<String, Object> fromChat(Map<String, Object> chat) {
		if (chat == null) {
			return null;
		}
		Map<String, Object> from = chat.get("from") instanceof Map<?, ?> f ? (Map<String, Object>) f : Map.of();
		Map<String, Object> user = from.get("user") instanceof Map<?, ?> u ? (Map<String, Object>) u : Map.of();
		Map<String, Object> sender = new LinkedHashMap<>();
		sender.put("name", user.get("displayName"));
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("from", Map.of("emailAddress", sender));
		message.put("body", chat.get("body"));
		message.put("receivedDateTime", chat.get("createdDateTime"));
		message.put("conversationId", chat.get("chatId"));
		return message;
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
