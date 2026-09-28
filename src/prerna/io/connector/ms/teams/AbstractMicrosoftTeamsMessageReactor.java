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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.ms.AbstractMicrosoftReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What every Teams messaging reactor has in common.
 *
 * <p>
 * The keys naming a chat, a channel, a message and a reply mean the same thing
 * wherever they appear, so they are named and described once here rather than
 * once per reactor. The readers of those keys are here for the same reason:
 * every one of these reactors has to turn pixel strings into numbers and lists,
 * and the way a bad value is reported should not depend on which reactor
 * happened to read it.
 * </p>
 *
 * <p>
 * A chat and a channel are addressed differently and always will be: a chat by
 * its own id, a channel by the team that owns it as well as its own id. That is
 * why sending to a chat and posting in a channel are separate reactors rather
 * than one reactor with a mode.
 * </p>
 */
public abstract class AbstractMicrosoftTeamsMessageReactor extends AbstractMicrosoftReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftTeamsMessageReactor.class);

	protected static final String TEAM_ID = "teamId";
	protected static final String CHANNEL_ID = "channelId";
	protected static final String CHAT_ID = "chatId";
	protected static final String MESSAGE_ID = "messageId";
	protected static final String REPLY_ID = "replyId";
	protected static final String MESSAGE = "message";
	protected static final String HTML = "html";
	protected static final String MENTIONS = "mentions";
	protected static final String ATTACHMENT_URLS = "attachmentUrls";
	protected static final String ATTACHMENT_NAMES = "attachmentNames";
	protected static final String MAX_BODY_CHARS = "maxBodyChars";
	protected static final String INCLUDE_REPLIES = "includeReplies";

	/** How much of a body comes back before it is cut short. */
	protected static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	/**
	 * Read a key that has to be a positive whole number.
	 *
	 * @param key      the key to read
	 * @param fallback what it is when the caller left it out
	 * @param cap      the largest value allowed, which the value is held down to
	 * @return the value
	 */
	protected int positiveInt(String key, int fallback, int cap) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return fallback;
		}
		int parsed;
		try {
			parsed = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid {} of '{}' passed to a Microsoft Teams message reactor", key, value, e);
			throw new SemossPixelException(key + " must be a whole number.");
		}
		if (parsed <= 0) {
			throw new SemossPixelException(key + " must be greater than 0.");
		}
		if (parsed > cap) {
			classLogger.warn("A {} of {} was asked for, using {} instead", key, parsed, cap);
			return cap;
		}
		return parsed;
	}

	/**
	 * Read one set of values, which a caller may pass as several nouns, as one
	 * comma separated noun, or as any mixture of the two.
	 *
	 * @param key the key to read
	 * @return the values, or null when none were passed
	 */
	protected String[] values(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null) {
				continue;
			}
			// a single value can still be a comma separated list, since that is how
			// somebody writing the pixel by hand tends to pass more than one
			for (String entry : value.toString().split(",")) {
				if (!entry.trim().isEmpty()) {
					values.add(entry.trim());
				}
			}
		}
		if (values.isEmpty()) {
			return null;
		}
		return values.toArray(new String[0]);
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it
	 */
	protected static String trimToNull(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TEAM_ID)) {
			return "Microsoft Teams team id, as returned by MicrosoftTeamsListTeams.";
		} else if (key.equals(CHANNEL_ID)) {
			return "Microsoft Teams channel id, as returned by MicrosoftTeamsListChannels.";
		} else if (key.equals(CHAT_ID)) {
			return "Microsoft Teams chat id, as returned by MicrosoftTeamsListChats.";
		} else if (key.equals(MESSAGE_ID)) {
			return "Id of the message, as returned by the Microsoft Teams message listings.";
		} else if (key.equals(REPLY_ID)) {
			return "Optional id of a reply to the message, which is what is acted on when it is given.";
		} else if (key.equals(MESSAGE)) {
			return "What the message says.";
		} else if (key.equals(HTML)) {
			return "Optional boolean for whether the message is html rather than plain text. Defaults to false.";
		} else if (key.equals(MENTIONS)) {
			return "Optional email addresses or user ids to mention, which is what notifies those people. Passed as several values or as one comma separated value.";
		} else if (key.equals(ATTACHMENT_URLS)) {
			return "Optional urls of files already in OneDrive or SharePoint to attach, such as the webUrl returned by MicrosoftOneDriveUploadFile or MicrosoftTeamsUploadFile.";
		} else if (key.equals(ATTACHMENT_NAMES)) {
			return "Optional names to show the attached files under, in the same order as the urls. The name each url ends in is used when omitted.";
		} else if (key.equals(MAX_BODY_CHARS)) {
			return "Optional longest message body to return before it is truncated. Defaults to "
					+ DEFAULT_MAX_BODY_CHARS + ".";
		} else if (key.equals(INCLUDE_REPLIES)) {
			return "Optional boolean for whether the replies underneath a channel message come back as well. Defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}

}
