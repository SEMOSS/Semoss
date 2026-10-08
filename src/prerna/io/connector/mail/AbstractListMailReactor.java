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
package prerna.io.connector.mail;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorOutput;
import prerna.io.connector.ConnectorPage;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads the signed in user's mail, a page at a time, newest first.
 */
public abstract class AbstractListMailReactor extends AbstractMailReactor {

	/** The folder read when a caller does not say. */
	public static final String DEFAULT_FOLDER = "inbox";

	/** How many messages come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 10;

	/** The most a caller can ask for, so a pixel cannot pull a whole mailbox. */
	private static final int MAX_LIMIT = 100;

	private static final String[] KEYS = { FOLDER, LIMIT, OFFSET, SUBJECT, FROM, UNREAD_ONLY, SINCE_DAYS, INCLUDE_BODY,
			MAX_BODY_CHARS, CONVERSATION_ID };

	/**
	 * What to look for in a mailbox.
	 *
	 * @param folder         the folder to read, by well known name, name or id
	 * @param limit          how many messages the page holds
	 * @param offset         how many messages to skip
	 * @param subject        text the subject has to contain, or null
	 * @param from           text the sender has to contain, or null
	 * @param unreadOnly     whether to return only messages nobody has opened
	 * @param since          the oldest message to return, or null
	 * @param includeBody    whether the bodies come back
	 * @param conversationId the thread to read from every folder instead, or null
	 */
	public record ListMailRequest(String folder, int limit, int offset, String subject, String from, boolean unreadOnly,
			Instant since, boolean includeBody, String conversationId) {

		/**
		 * @return how many messages, counted from the first, a provider that cannot
		 *         skip has to read to fill the page and tell whether there is more
		 */
		public int needed() {
			return this.offset + this.limit + 1;
		}

		/**
		 * @return whether the request filters by text
		 */
		public boolean searches() {
			return this.subject != null || this.from != null;
		}
	}

	protected AbstractListMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet);
	}

	/**
	 * Read one page of messages, newest first.
	 *
	 * @param user    the signed in user
	 * @param request what to look for
	 * @return the page
	 * @throws Exception when the provider cannot be read
	 */
	protected abstract ConnectorPage<MailMessage> listMail(User user, ListMailRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read your mail", () -> {
			Integer sinceDays = readOptionalInt(SINCE_DAYS);
			if (sinceDays != null && sinceDays <= 0) {
				throw new SemossPixelException(SINCE_DAYS + " must be greater than 0.");
			}
			String folder = readString(FOLDER);
			boolean includeBody = readBoolean(INCLUDE_BODY, true);
			ListMailRequest request = new ListMailRequest(folder == null ? DEFAULT_FOLDER : folder,
					readCount(LIMIT, DEFAULT_LIMIT, MAX_LIMIT), readOffset(), readString(SUBJECT), readString(FROM),
					readBoolean(UNREAD_ONLY, false),
					sinceDays == null ? null : Instant.now().minus(sinceDays, ChronoUnit.DAYS), includeBody,
					readString(CONVERSATION_ID));
			int maxBodyChars = readCount(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);

			ConnectorPage<MailMessage> page = listMail(this.insight.getUser(), request);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(FOLDER, request.folder());
			ConnectorOutput.putIfPresent(output, CONVERSATION_ID, request.conversationId());
			output.put(OFFSET, request.offset());
			output.put("count", page.items().size());
			output.put("hasMore", page.hasMore());
			output.put("messages",
					page.items().stream().map(message -> message.toMap(includeBody, maxBodyChars)).toList());
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "mail/list";
	}

	@Override
	protected final String describe() {
		return "Read the mail in the signed in user's own " + mailbox() + ", newest first, a page at a time.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case FOLDER:
			return "Optional folder to read: inbox, sentitems, drafts, deleteditems, junkemail or archive, or a "
					+ "folder id or name as returned by " + reactorName("ListMailFolders") + ". Defaults to "
					+ DEFAULT_FOLDER + ".";
		case LIMIT:
			return "Optional number of messages to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		case SUBJECT:
			return "Optional text the subject has to contain.";
		case FROM:
			return "Optional text the sender has to contain.";
		case UNREAD_ONLY:
			return "Optional boolean to return only messages nobody has opened. Defaults to false.";
		case SINCE_DAYS:
			return "Optional number of days back to read. Every message in the folder is considered when omitted.";
		case INCLUDE_BODY:
			return "Optional boolean for whether each message's body comes back. Defaults to true.";
		case CONVERSATION_ID:
			return "Optional conversationId of a listed message, to read that whole thread from every folder, "
					+ "including sent replies, instead of one folder. Each message then also has uniqueBody, its "
					+ "text without the earlier messages it quotes. The folder, subject, from, unreadOnly and "
					+ "sinceDays filters do not apply to it.";
		default:
			return super.describeKey(key);
		}
	}
}
