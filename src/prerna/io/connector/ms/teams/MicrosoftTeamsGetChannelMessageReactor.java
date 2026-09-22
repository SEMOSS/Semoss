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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads one message posted in a Teams channel, or one reply to it.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code ChannelMessage.Read.All} for
 * {@code GET /teams/{id}/channels/{id}/messages/{id}} and for the replies
 * underneath it</li>
 * </ul>
 *
 * <p>
 * This is how a whole thread is read: name the message that started it and ask
 * for {@code includeReplies}. Naming a {@code replyId} instead reads that one
 * reply on its own.
 * </p>
 */
public class MicrosoftTeamsGetChannelMessageReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsGetChannelMessageReactor.class);

	public MicrosoftTeamsGetChannelMessageReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, MESSAGE_ID, REPLY_ID, INCLUDE_REPLIES, MAX_BODY_CHARS };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = trimToNull(this.keyValue.get(TEAM_ID));
		String channelId = trimToNull(this.keyValue.get(CHANNEL_ID));
		String messageId = trimToNull(this.keyValue.get(MESSAGE_ID));
		String replyId = trimToNull(this.keyValue.get(REPLY_ID));
		boolean includeReplies = Boolean.parseBoolean(this.keyValue.get(INCLUDE_REPLIES));
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		if (teamId == null) {
			throw new SemossPixelException("A " + TEAM_ID + " is required to read a Microsoft Teams channel message.");
		}
		if (channelId == null) {
			throw new SemossPixelException(
					"A " + CHANNEL_ID + " is required to read a Microsoft Teams channel message.");
		}
		if (messageId == null) {
			throw new SemossPixelException(
					"A " + MESSAGE_ID + " is required to read a Microsoft Teams channel message.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			Map<String, Object> message = MicrosoftTeamsMessageHelper.getChannelMessage(accessToken, teamId, channelId,
					messageId, replyId, includeReplies, maxBodyChars);
			return new NounMetadata(message, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading Microsoft Teams channel message '{}'", messageId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read a Microsoft Teams channel message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read Microsoft Teams channel message '{}'", messageId, e);
			throw new SemossPixelException(
					"An error occurred retrieving the channel message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read one message posted in a Microsoft Teams channel, with the replies underneath it when they are asked for.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(REPLY_ID)) {
			return "Optional id of a reply to the message, which is what is read when it is given.";
		}
		return super.getDescriptionForKey(key);
	}
}
