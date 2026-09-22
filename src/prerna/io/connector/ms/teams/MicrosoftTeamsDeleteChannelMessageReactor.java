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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deletes a message, or a reply, that the signed in user posted in a Teams
 * channel.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code ChannelMessage.ReadWrite} for
 * {@code POST /teams/{id}/channels/{id}/messages/{id}/softDelete} and for the
 * same under a reply</li>
 * </ul>
 *
 * <p>
 * This is the soft delete the Teams client performs: the message disappears
 * from the channel for everybody. Deleting the message that started a
 * conversation takes the replies underneath it with it, so a single wrong
 * answer is better removed by naming its {@code replyId}.
 * </p>
 *
 * <p>
 * Graph has no way to change what a message says. Editing means deleting the
 * message and posting another with {@code MicrosoftTeamsSendChannelMessage}.
 * </p>
 */
public class MicrosoftTeamsDeleteChannelMessageReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsDeleteChannelMessageReactor.class);

	public MicrosoftTeamsDeleteChannelMessageReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, MESSAGE_ID, REPLY_ID };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = trimToNull(this.keyValue.get(TEAM_ID));
		String channelId = trimToNull(this.keyValue.get(CHANNEL_ID));
		String messageId = trimToNull(this.keyValue.get(MESSAGE_ID));
		String replyId = trimToNull(this.keyValue.get(REPLY_ID));

		if (teamId == null) {
			throw new SemossPixelException(
					"A " + TEAM_ID + " is required to delete a Microsoft Teams channel message.");
		}
		if (channelId == null) {
			throw new SemossPixelException(
					"A " + CHANNEL_ID + " is required to delete a Microsoft Teams channel message.");
		}
		if (messageId == null) {
			throw new SemossPixelException(
					"A " + MESSAGE_ID + " is required to delete a Microsoft Teams channel message.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			MicrosoftTeamsMessageHelper.deleteChannelMessage(accessToken, teamId, channelId, messageId, replyId);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(TEAM_ID, teamId);
			output.put(CHANNEL_ID, channelId);
			output.put(MESSAGE_ID, messageId);
			if (replyId != null) {
				output.put(REPLY_ID, replyId);
			}
			output.put("deleted", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting Microsoft Teams channel message '{}'", messageId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to delete a Microsoft Teams channel message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to delete Microsoft Teams channel message '{}'", messageId, e);
			throw new SemossPixelException("An error occurred deleting the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete a message, or a reply, that the signed in user posted in a Microsoft Teams channel.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(REPLY_ID)) {
			return "Optional id of a reply to the message, which is what is deleted when it is given. Deleting the message itself removes its replies as well.";
		}
		return super.getDescriptionForKey(key);
	}
}
