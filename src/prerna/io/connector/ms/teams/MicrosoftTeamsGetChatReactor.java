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
 * Reads one Teams chat, including who is in it.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Chat.ReadBasic} for {@code GET /chats/{id}?$expand=members}.
 * {@code Chat.Read} and {@code Chat.ReadWrite} also satisfy it.</li>
 * </ul>
 *
 * <p>
 * Worth reading before sending, since the members are the answer to whether a
 * chat is the right place to say something. What it does not carry is the
 * conversation itself, which is {@code MicrosoftTeamsListChatMessages}.
 * </p>
 */
public class MicrosoftTeamsGetChatReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsGetChatReactor.class);

	public MicrosoftTeamsGetChatReactor() {
		this.keysToGet = new String[] { CHAT_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String chatId = trimToNull(this.keyValue.get(CHAT_ID));

		if (chatId == null) {
			throw new SemossPixelException("A " + CHAT_ID + " is required to read a Microsoft Teams chat.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			String userEmail = MicrosoftLoginUtils.getMicrosoftEmail(user);
			Map<String, Object> chat = MicrosoftTeamsMessageHelper.getChat(accessToken, chatId, userEmail);
			return new NounMetadata(chat, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading Microsoft Teams chat '{}'", chatId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read a Microsoft Teams chat", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read Microsoft Teams chat '{}'", chatId, e);
			throw new SemossPixelException("An error occurred retrieving the chat. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read one Microsoft Teams chat, including who is in it.";
	}
}
