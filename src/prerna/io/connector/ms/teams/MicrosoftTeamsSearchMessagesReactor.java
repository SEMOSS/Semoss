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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * Searches the Teams messages the signed in user can read, in their chats and
 * in the channels of their teams.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Chat.Read} for {@code POST /search/query} with the
 * {@code chatMessage} entity type, which finds the messages of the user's
 * chats. {@code Chat.ReadWrite} also satisfies it.</li>
 * <li>{@code ChannelMessage.Read.All} as well for matches in the channels of
 * the user's teams, which a search without it leaves out.</li>
 * </ul>
 *
 * <p>
 * The search matches the text of a message, the name of whoever sent it and the
 * files attached to it, and reads Keyword Query Language, so {@code from:ada},
 * {@code sent>2026-10-01} or {@code hasAttachment:true} narrow it. Each match
 * comes back with a summary of where it matched rather than its whole body; a
 * chat match names the {@code chatId} that
 * {@code MicrosoftTeamsListChatMessages} reads, and a channel match the
 * {@code teamId} and {@code channelId} that
 * {@code MicrosoftTeamsGetChannelMessage} reads.
 * </p>
 *
 * <p>
 * Graph answers a message search newest first and cannot be asked for another
 * order; each page is put newest first again once it is read, so its order
 * holds whatever Graph sends. Graph cannot be asked for chat messages alone, so
 * {@code chatsOnly} drops channel posts here and reads on until the page is
 * full; {@code nextOffset} counts every match read, kept or not, and is the
 * {@code offset} of the next page.
 * </p>
 */
public class MicrosoftTeamsSearchMessagesReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsSearchMessagesReactor.class);

	private static final String CHATS_ONLY = "chatsOnly";

	/** How many messages come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 25;

	/** The most a caller can ask for at once. */
	private static final int MAX_LIMIT = 100;

	public MicrosoftTeamsSearchMessagesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), CHATS_ONLY };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String search = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey()));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		int offset = offset();
		boolean chatsOnly = Boolean.parseBoolean(this.keyValue.get(CHATS_ONLY));

		if (search == null) {
			throw new SemossPixelException("Search text is required to search Microsoft Teams messages.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftTeamsMessageHelper.MessageSearchPage page = MicrosoftTeamsMessageHelper.searchMessages(accessToken,
					search, offset, limit, chatsOnly);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("search", search);
			output.put(ReactorKeysEnum.OFFSET.getKey(), offset);
			output.put("count", page.messages().size());
			output.put("hasMore", page.hasMore());
			output.put("nextOffset", page.nextOffset());
			output.put("messages", page.messages());
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while searching Microsoft Teams messages for '{}'", search, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to search Microsoft Teams messages", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to search Microsoft Teams messages for '{}'", search, e);
			throw new SemossPixelException(
					"An error occurred searching the Teams messages. Error message: " + e.getMessage());
		}
	}

	/**
	 * Read the number of matches to skip, which may be 0.
	 *
	 * @return the offset, 0 when the caller left it out
	 */
	private int offset() {
		String key = ReactorKeysEnum.OFFSET.getKey();
		String value = ValueUtils.trimToNull(this.keyValue.get(key));
		if (value == null) {
			return 0;
		}
		int parsed;
		try {
			// models often send a whole number as 25.0
			parsed = new BigDecimal(value).intValueExact();
		} catch (NumberFormatException | ArithmeticException e) {
			classLogger.error("Invalid {} of '{}' passed to search Microsoft Teams messages", key, value, e);
			throw new SemossPixelException(key + " must be a whole number.");
		}
		if (parsed < 0) {
			throw new SemossPixelException(key + " must be 0 or more.");
		}
		return parsed;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description(
						"Search the Microsoft Teams messages of the signed in user's chats and channels by their text, "
								+ "their sender, and the files attached to them, newest first.")
				.key(ReactorKeysEnum.SEARCH,
						"What to look for: words in the messages, the sender's name, or an attached file's name or contents. "
								+ "Keyword Query Language narrows it, such as from:ada, sent>2026-10-01, or hasAttachment:true.")
				.key(ReactorKeysEnum.LIMIT,
						"Optional maximum number of messages to return. Defaults to " + DEFAULT_LIMIT
								+ " and is capped at " + MAX_LIMIT + ".")
				.key(ReactorKeysEnum.OFFSET,
						"Optional number of matches to skip, such as the nextOffset of the page before. Defaults to 0.")
				.key(CHATS_ONLY,
						"Optional boolean for whether only messages in chats come back, leaving out channel posts. "
								+ "Defaults to false.");
	}
}
