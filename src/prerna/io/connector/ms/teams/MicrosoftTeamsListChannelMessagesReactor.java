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
 * Reads the messages posted in a Teams channel.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code ChannelMessage.Read.All} for
 * {@code GET /teams/{id}/channels/{id}/messages}. This one is admin consented
 * in most tenants, since it reads every channel the signed in user belongs
 * to.</li>
 * </ul>
 *
 * <p>
 * A channel is a set of conversations rather than a flat list, so what comes
 * back is the message that started each one, most recent first. The replies
 * underneath them come too when {@code includeReplies} asks for them, and that
 * is the whole thread: reading a channel without them shows what was raised,
 * and reading it with them shows what was said about it.
 * </p>
 */
public class MicrosoftTeamsListChannelMessagesReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsListChannelMessagesReactor.class);

	/** How many messages come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 20;

	/** The most a caller can ask for, so a pixel cannot pull a whole channel. */
	private static final int MAX_LIMIT = 200;

	public MicrosoftTeamsListChannelMessagesReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, INCLUDE_REPLIES, ReactorKeysEnum.LIMIT.getKey(),
				MAX_BODY_CHARS };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = trimToNull(this.keyValue.get(TEAM_ID));
		String channelId = trimToNull(this.keyValue.get(CHANNEL_ID));
		boolean includeReplies = Boolean.parseBoolean(this.keyValue.get(INCLUDE_REPLIES));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		if (teamId == null) {
			throw new SemossPixelException("A " + TEAM_ID + " is required to read Microsoft Teams channel messages.");
		}
		if (channelId == null) {
			throw new SemossPixelException(
					"A " + CHANNEL_ID + " is required to read Microsoft Teams channel messages.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> messages = MicrosoftTeamsMessageHelper.listChannelMessages(accessToken, teamId,
					channelId, includeReplies, maxBodyChars, limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(TEAM_ID, teamId);
			output.put(CHANNEL_ID, channelId);
			output.put("count", messages.size());
			output.put("messages", messages);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the messages of Microsoft Teams channel '{}'", channelId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read Microsoft Teams channel messages", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the messages of Microsoft Teams channel '{}'", channelId, e);
			throw new SemossPixelException(
					"An error occurred retrieving the channel messages. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read the messages posted in a Microsoft Teams channel, most recent first.";
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
