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
package prerna.io.connector.ms.outlook;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads the mail of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Read} for {@code GET /me/messages} and
 * {@code GET /me/mailFolders/{id}/messages}</li>
 * </ul>
 *
 * <p>
 * Messages come back in the shape the mail function engines answer in, so
 * whatever reads one can read the other.
 * </p>
 */
public class MicrosoftOutlookListMailReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookListMailReactor.class);

	private static final String FOLDER = "folder";
	private static final String SUBJECT = "subject";
	private static final String FROM = "from";
	private static final String UNREAD_ONLY = "unreadOnly";
	private static final String SINCE_DAYS = "sinceDays";
	private static final String INCLUDE_BODY = "includeBody";
	private static final String MAX_BODY_CHARS = "maxBodyChars";

	/** The folder read when a caller does not say. */
	private static final String DEFAULT_FOLDER = "inbox";

	/** How many messages come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 10;

	/** The most a caller can ask for, so a pixel cannot pull a whole mailbox. */
	private static final int MAX_LIMIT = 100;

	/** How much of a body comes back before it is cut short. */
	private static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	public MicrosoftOutlookListMailReactor() {
		this.keysToGet = new String[] { FOLDER, ReactorKeysEnum.LIMIT.getKey(), SUBJECT, FROM, UNREAD_ONLY, SINCE_DAYS,
				INCLUDE_BODY, MAX_BODY_CHARS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		MicrosoftOutlookMailHelper.MessageQuery query = new MicrosoftOutlookMailHelper.MessageQuery();
		String folder = trimToNull(this.keyValue.get(FOLDER));
		query.folder = folder == null ? DEFAULT_FOLDER : folder;
		query.top = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);
		query.subject = trimToNull(this.keyValue.get(SUBJECT));
		query.from = trimToNull(this.keyValue.get(FROM));
		query.unreadOnly = Boolean.parseBoolean(this.keyValue.get(UNREAD_ONLY));
		String includeBody = trimToNull(this.keyValue.get(INCLUDE_BODY));
		query.includeBody = includeBody == null || Boolean.parseBoolean(includeBody);

		if (trimToNull(this.keyValue.get(SINCE_DAYS)) != null) {
			query.since = Date
					.from(Instant.now().minus(positiveInt(SINCE_DAYS, 1, Integer.MAX_VALUE), ChronoUnit.DAYS));
		}

		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);

			// null mailbox is what addresses /me, so the signed in user is the only
			// mailbox this reactor is able to read
			List<Map<String, Object>> found = new MicrosoftOutlookMailHelper().listMessages(accessToken, null, query);

			List<Map<String, Object>> messages = new ArrayList<>();
			for (Map<String, Object> message : found) {
				Map<String, Object> described = MicrosoftOutlookMessageMapper.toMessage(message, query.includeBody,
						maxBodyChars);
				// the attachments themselves are another call each, so a listing says
				// only whether there are any
				described.put("hasAttachments", Boolean.TRUE.equals(message.get("hasAttachments")));
				messages.add(described);
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("folder", query.folder);
			output.put("count", messages.size());
			output.put("messages", messages);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the signed in user's mail", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read the signed in user's mail", e);
			throw new SemossPixelException("An error occurred reading your mail. Error message: " + e.getMessage());
		}
	}

	/**
	 * Read a key that has to be a positive whole number.
	 *
	 * @param key      the key to read
	 * @param fallback what it is when the caller left it out
	 * @param cap      the largest value allowed, which the value is held down to
	 * @return the value
	 */
	private int positiveInt(String key, int fallback, int cap) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return fallback;
		}
		int parsed;
		try {
			parsed = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid {} of '{}' passed to read mail", key, value, e);
			throw new SemossPixelException(key + " must be a positive integer.");
		}
		if (parsed <= 0) {
			throw new SemossPixelException(key + " must be greater than 0.");
		}
		if (parsed > cap) {
			classLogger.warn("A {} of {} was asked for, reading {} instead", key, parsed, cap);
			return cap;
		}
		return parsed;
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it
	 */
	private static String trimToNull(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Read the mail of the signed in user's own Microsoft 365 mailbox.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FOLDER)) {
			return "Optional mail folder to read, by well known name such as inbox or sentitems, or by folder id. Defaults to inbox.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of messages to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		} else if (key.equals(SUBJECT)) {
			return "Optional text the subject has to contain. Searching by text cannot be combined with a date, which is applied afterwards.";
		} else if (key.equals(FROM)) {
			return "Optional text the sender has to contain.";
		} else if (key.equals(UNREAD_ONLY)) {
			return "Optional boolean to return only messages that have not been read. Defaults to false.";
		} else if (key.equals(SINCE_DAYS)) {
			return "Optional number of days back to read. All messages in the folder are considered when omitted.";
		} else if (key.equals(INCLUDE_BODY)) {
			return "Optional boolean for whether the message body comes back. Defaults to true.";
		} else if (key.equals(MAX_BODY_CHARS)) {
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		}
		return super.getDescriptionForKey(key);
	}
}
