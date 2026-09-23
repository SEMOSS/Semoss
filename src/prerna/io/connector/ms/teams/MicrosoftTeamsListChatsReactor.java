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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the Teams chats the signed in user is in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Chat.ReadBasic} for {@code GET /me/chats}, or {@code Chat.Read} to
 * read the chats as well as list them. {@code Chat.ReadWrite} also satisfies
 * both.</li>
 * </ul>
 *
 * <p>
 * The id of a chat here is what the other chat reactors take as their
 * {@code chatId}. A one on one chat has no topic of its own, so the
 * {@code displayName} that comes back is the name of whoever else is in it,
 * which is how it is labeled in the Teams client too.
 * </p>
 *
 * <p>
 * Unread state is a property of the chat rather than of its messages: every
 * chat carries {@code lastMessageReadDateTime}, saying when the signed in user
 * last read it. Comparing that with when the last message arrived is what
 * answers "what have I not read", so {@code includeLastMessage} reads the last
 * message of each chat and reports {@code hasUnread} beside it, and
 * {@code unreadOnly} keeps just those chats.
 * </p>
 */
public class MicrosoftTeamsListChatsReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsListChatsReactor.class);

	private static final String INCLUDE_LAST_MESSAGE = "includeLastMessage";
	private static final String UNREAD_ONLY = "unreadOnly";

	/** How many chats come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for. */
	private static final int MAX_LIMIT = 200;

	public MicrosoftTeamsListChatsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), INCLUDE_LAST_MESSAGE, UNREAD_ONLY };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		boolean unreadOnly = Boolean.parseBoolean(this.keyValue.get(UNREAD_ONLY));
		// asking which chats are unread is asking for the last message of each, so
		// the narrower key is the one that decides
		boolean includeLastMessage = unreadOnly || Boolean.parseBoolean(this.keyValue.get(INCLUDE_LAST_MESSAGE));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			// the signed in user's own address is what keeps them out of the name a
			// chat is given
			String userEmail = MicrosoftLoginUtils.getMicrosoftEmail(user);
			List<Map<String, Object>> chats = MicrosoftTeamsMessageHelper.listChats(accessToken, userEmail,
					includeLastMessage, unreadOnly, limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", chats.size());
			if (includeLastMessage) {
				output.put("unreadCount",
						chats.stream().filter(chat -> Boolean.TRUE.equals(chat.get("hasUnread"))).count());
			}
			output.put("chats", chats);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the Microsoft Teams chats of the signed in user", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft Teams chats of the signed in user", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of chats. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the Microsoft Teams chats the signed in user is part of, and say which of them hold unread messages.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of chats to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		} else if (key.equals(INCLUDE_LAST_MESSAGE)) {
			return "Optional boolean for whether the last message of each chat comes back, which is also what decides whether the chat is reported as unread. Defaults to false.";
		} else if (key.equals(UNREAD_ONLY)) {
			return "Optional boolean for returning only the chats holding messages the signed in user has not read. Defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}
}
