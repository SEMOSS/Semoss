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
 * Starts a Teams chat with one or more people.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Chat.Create} for {@code POST /chats}. {@code Chat.ReadWrite} also
 * satisfies it.</li>
 * <li>{@code User.Read.All} for {@code GET /users/{address}}, which turns the
 * email addresses given here into the directory ids Graph binds chat members
 * to.</li>
 * </ul>
 *
 * <p>
 * The signed in user is always in the chat, so the people named here are
 * everybody else. Asking for a one on one chat with somebody the user already
 * has one with answers that same chat rather than a second one, which is why
 * {@code MicrosoftTeamsSendChatMessage} can find a chat by recipient without
 * ever making a duplicate.
 * </p>
 */
public class MicrosoftTeamsCreateChatReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsCreateChatReactor.class);

	private static final String MEMBERS = "members";
	private static final String CHAT_TYPE = "chatType";
	private static final String TOPIC = "topic";

	public MicrosoftTeamsCreateChatReactor() {
		this.keysToGet = new String[] { MEMBERS, CHAT_TYPE, TOPIC };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String[] members = values(MEMBERS);
		String chatType = trimToNull(this.keyValue.get(CHAT_TYPE));
		String topic = trimToNull(this.keyValue.get(TOPIC));

		if (members == null) {
			throw new SemossPixelException("At least one other person is required to start a Microsoft Teams chat.");
		}
		// naming several people is itself a statement that this is a group chat, so
		// the type only has to be passed to make a group chat of one other person
		if (chatType == null) {
			chatType = members.length > 1 ? "group" : "oneOnOne";
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			String userEmail = MicrosoftLoginUtils.getMicrosoftEmail(user);
			Map<String, Object> chat = MicrosoftTeamsMessageHelper.createChat(accessToken, chatType, members, topic,
					userEmail);
			return new NounMetadata(chat, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while starting a Microsoft Teams chat", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to start a Microsoft Teams chat", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to start a Microsoft Teams chat", e);
			throw new SemossPixelException("An error occurred starting the chat. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Start a Microsoft Teams chat with one or more people, or find the existing one on one chat with somebody.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MEMBERS)) {
			return "Email addresses or user ids of the other people in the chat, passed as several values or as one comma separated value. The signed in user is always included.";
		} else if (key.equals(CHAT_TYPE)) {
			return "Optional kind of chat, either oneOnOne or group. Defaults to oneOnOne for one other person and group for several.";
		} else if (key.equals(TOPIC)) {
			return "Optional title for a group chat. Microsoft Teams does not let a one on one chat have one.";
		}
		return super.getDescriptionForKey(key);
	}
}
