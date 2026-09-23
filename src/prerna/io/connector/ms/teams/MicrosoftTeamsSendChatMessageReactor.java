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
 * Sends a message in a Teams chat, as the signed in user.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code ChatMessage.Send} for {@code POST /chats/{id}/messages}.
 * {@code Chat.ReadWrite} also satisfies it.</li>
 * <li>{@code Chat.Create} and {@code User.Read.All} as well, when
 * {@code recipients} names people rather than a {@code chatId}, since the chat
 * has to be found or started first.</li>
 * </ul>
 *
 * <p>
 * Either the chat or the people are named. Naming people is what makes this
 * "message Alex" rather than "message chat 19:...": the one on one chat with
 * them is found, or started if there is none, and the message goes there. Graph
 * answers the existing chat when one is already there, so this does not leave a
 * trail of empty duplicates.
 * </p>
 *
 * <p>
 * A mention is what actually notifies somebody, so an urgent message names them
 * in {@code mentions}. A file has to be in a drive before it can be attached,
 * so upload it with {@code MicrosoftOneDriveUploadFile} first and pass the
 * {@code webUrl} that comes back.
 * </p>
 */
public class MicrosoftTeamsSendChatMessageReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsSendChatMessageReactor.class);

	private static final String RECIPIENTS = "recipients";

	public MicrosoftTeamsSendChatMessageReactor() {
		this.keysToGet = new String[] { CHAT_ID, RECIPIENTS, MESSAGE, HTML, MENTIONS, ATTACHMENT_URLS,
				ATTACHMENT_NAMES };
		this.keyRequired = new int[] { 0, 0, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String chatId = trimToNull(this.keyValue.get(CHAT_ID));
		String content = this.keyValue.get(MESSAGE);
		boolean html = Boolean.parseBoolean(this.keyValue.get(HTML));
		String[] recipients = values(RECIPIENTS);
		String[] mentions = values(MENTIONS);
		String[] attachmentUrls = values(ATTACHMENT_URLS);
		String[] attachmentNames = values(ATTACHMENT_NAMES);

		if (chatId == null && recipients == null) {
			throw new SemossPixelException(
					"Either a " + CHAT_ID + " or the " + RECIPIENTS + " to message are required.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			String userEmail = MicrosoftLoginUtils.getMicrosoftEmail(user);

			String chat = chatId;
			if (chat == null) {
				// one person means the one on one chat with them, and several means a
				// group chat of everybody named
				String chatType = recipients.length > 1 ? "group" : "oneOnOne";
				Object created = MicrosoftTeamsMessageHelper
						.createChat(accessToken, chatType, recipients, null, userEmail).get("id");
				if (created == null) {
					throw new SemossPixelException("Microsoft Teams returned no chat to send the message to.");
				}
				chat = created.toString();
			}

			Map<String, Object> sent = MicrosoftTeamsMessageHelper.sendChatMessage(accessToken, chat, content, html,
					mentions, attachmentUrls, attachmentNames, DEFAULT_MAX_BODY_CHARS);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(CHAT_ID, chat);
			output.putAll(sent);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sending a Microsoft Teams chat message", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to send a Microsoft Teams chat message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to send a Microsoft Teams chat message", e);
			throw new SemossPixelException("An error occurred sending the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Send a message in a Microsoft Teams chat, either to an existing chat or to the people named.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CHAT_ID)) {
			return "Id of the chat to send to, as returned by MicrosoftTeamsListChats. Not needed when recipients are named.";
		} else if (key.equals(RECIPIENTS)) {
			return "Email addresses or user ids to message, passed as several values or as one comma separated value. The one on one chat with one person is used or started, and several people are put in a group chat. Not needed when a chat id is given.";
		}
		return super.getDescriptionForKey(key);
	}
}
