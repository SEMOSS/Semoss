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
 * Reads the messages of a Teams chat.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Chat.Read} for {@code GET /chats/{id}/messages}.
 * {@code Chat.ReadWrite} also satisfies it. {@code Chat.ReadBasic} is not
 * enough: it lists chats without reading what was said in them.</li>
 * </ul>
 *
 * <p>
 * Graph answers most recent first, and that is the order this keeps, so a limit
 * takes the latest part of the conversation rather than the oldest.
 * </p>
 */
public class MicrosoftTeamsListChatMessagesReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsListChatMessagesReactor.class);

	/** How many messages come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for, so a pixel cannot pull a whole history. */
	private static final int MAX_LIMIT = 200;

	public MicrosoftTeamsListChatMessagesReactor() {
		this.keysToGet = new String[] { CHAT_ID, ReactorKeysEnum.LIMIT.getKey(), MAX_BODY_CHARS };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String chatId = trimToNull(this.keyValue.get(CHAT_ID));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		if (chatId == null) {
			throw new SemossPixelException("A " + CHAT_ID + " is required to read Microsoft Teams chat messages.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> messages = MicrosoftTeamsMessageHelper.listChatMessages(accessToken, chatId,
					maxBodyChars, limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(CHAT_ID, chatId);
			output.put("count", messages.size());
			output.put("messages", messages);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the messages of Microsoft Teams chat '{}'", chatId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read Microsoft Teams chat messages", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the messages of Microsoft Teams chat '{}'", chatId, e);
			throw new SemossPixelException(
					"An error occurred retrieving the chat messages. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read the messages of a Microsoft Teams chat, most recent first.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of messages to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
