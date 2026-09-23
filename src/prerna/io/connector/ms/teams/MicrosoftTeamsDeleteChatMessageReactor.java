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
 * Deletes a message the signed in user sent in a Teams chat.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Chat.ReadWrite} for
 * {@code POST /users/{id}/chats/{id}/messages/{id}/softDelete}</li>
 * <li>{@code User.Read} for {@code GET /me}, which is read because Graph
 * documents that delete under a named user rather than under {@code /me}</li>
 * </ul>
 *
 * <p>
 * This is the soft delete the Teams client performs: the message disappears
 * from the conversation for everybody in it. Only the person who sent a message
 * can delete it, so this cannot be used to remove somebody else's.
 * </p>
 *
 * <p>
 * Graph has no way to change what a message says. Editing means deleting the
 * message and sending another with {@code MicrosoftTeamsSendChatMessage}.
 * </p>
 */
public class MicrosoftTeamsDeleteChatMessageReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsDeleteChatMessageReactor.class);

	public MicrosoftTeamsDeleteChatMessageReactor() {
		this.keysToGet = new String[] { CHAT_ID, MESSAGE_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String chatId = trimToNull(this.keyValue.get(CHAT_ID));
		String messageId = trimToNull(this.keyValue.get(MESSAGE_ID));

		if (chatId == null) {
			throw new SemossPixelException("A " + CHAT_ID + " is required to delete a Microsoft Teams chat message.");
		}
		if (messageId == null) {
			throw new SemossPixelException(
					"A " + MESSAGE_ID + " is required to delete a Microsoft Teams chat message.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftTeamsMessageHelper.deleteChatMessage(accessToken, chatId, messageId);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(CHAT_ID, chatId);
			output.put(MESSAGE_ID, messageId);
			output.put("deleted", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting Microsoft Teams chat message '{}'", messageId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to delete a Microsoft Teams chat message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to delete Microsoft Teams chat message '{}'", messageId, e);
			throw new SemossPixelException("An error occurred deleting the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete a message the signed in user sent in a Microsoft Teams chat.";
	}
}
