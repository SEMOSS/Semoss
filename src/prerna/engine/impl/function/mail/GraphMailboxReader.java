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
package prerna.engine.impl.function.mail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMessageMapper;
import prerna.om.Insight;

/**
 * Reads a Microsoft 365 mailbox through Graph, in the shape the mail engines
 * answer in.
 *
 * <p>
 * The Graph calls themselves are
 * {@link prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper}, which is
 * deliberately generic, and describing a message is
 * {@link prerna.io.connector.ms.outlook.MicrosoftOutlookMessageMapper}, which
 * is shared with the reactors that read a signed in user's own mail. This is
 * what is left that only an engine needs: turning the engine's search into a
 * Graph query, and applying the guardrails the SMSS set. A caller cannot tell
 * from the output which way the mailbox was read, other than by the uid - Graph
 * names a message with an opaque string where IMAP uses a number.
 */
public class GraphMailboxReader {

	private static final Logger classLogger = LogManager.getLogger(GraphMailboxReader.class);

	private final MicrosoftOutlookMailHelper mail;
	private final AbstractMailStoreFunctionEngine engine;

	/**
	 * @param mail   the Graph calls to make
	 * @param engine the engine whose settings bound what a read may return
	 */
	public GraphMailboxReader(MicrosoftOutlookMailHelper mail, AbstractMailStoreFunctionEngine engine) {
		this.mail = mail;
		this.engine = engine;
	}

	/**
	 * Find messages and turn them into the maps the engine answers with.
	 *
	 * @param accessToken         the token to read with
	 * @param mailbox             the mailbox to read
	 * @param folder              the folder to read
	 * @param criteria            what the caller asked to match on
	 * @param limit               the most messages to return
	 * @param includeBody         whether the body comes back
	 * @param downloadAttachments whether attachments are written into the insight
	 * @param executingInsight    the insight this call is running under, or null
	 * @return the search output
	 */
	public Map<String, Object> search(String accessToken, String mailbox, String folder,
			AbstractMailStoreFunctionEngine.MailSearchCriteria criteria, int limit, boolean includeBody,
			boolean downloadAttachments, Insight executingInsight) {
		MicrosoftOutlookMailHelper.MessageQuery query = new MicrosoftOutlookMailHelper.MessageQuery();
		// graph knows its own folders by lower case well known names, and the engines
		// spell the inbox the way the protocols do
		query.folder = "INBOX".equalsIgnoreCase(folder) ? "inbox" : folder;
		query.from = criteria.from;
		query.subject = criteria.subject;
		query.since = criteria.since;
		query.unreadOnly = criteria.unreadOnly;
		query.includeBody = includeBody;
		// asked for more than the caller wants, because the sender allowlist is
		// applied here rather than by graph, and would otherwise return short
		query.top = this.engine.allowedSenderDomains.isEmpty() ? limit : Math.min(limit * 4, 200);

		List<Map<String, Object>> found = this.mail.listMessages(accessToken, mailbox, query);

		List<Map<String, Object>> messages = new ArrayList<>();
		for (Map<String, Object> message : found) {
			if (messages.size() >= limit) {
				break;
			}
			String sender = MicrosoftOutlookMessageMapper.addressOf(message.get("from"));
			if (!this.engine.isSenderAllowed(sender)) {
				continue;
			}
			messages.add(
					toMessageMap(accessToken, mailbox, message, includeBody, downloadAttachments, executingInsight));
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("folder", folder);
		output.put("count", messages.size());
		output.put("messages", messages);
		return output;
	}

	/**
	 * Turn one Graph message into the map the engine answers with.
	 *
	 * @param accessToken         the token to read attachments with
	 * @param mailbox             the mailbox the message is in
	 * @param message             the message as Graph returned it
	 * @param includeBody         whether the body comes back
	 * @param downloadAttachments whether attachments are written into the insight
	 * @param executingInsight    the insight this call is running under, or null
	 * @return the message as a map
	 */
	private Map<String, Object> toMessageMap(String accessToken, String mailbox, Map<String, Object> message,
			boolean includeBody, boolean downloadAttachments, Insight executingInsight) {
		// the message itself is described the same way however it was read, so only
		// the attachments, which the engine may write into the insight, are left here
		Map<String, Object> output = MicrosoftOutlookMessageMapper.toMessage(message, includeBody,
				this.engine.maxBodyChars);

		if (Boolean.TRUE.equals(message.get("hasAttachments"))) {
			output.put("attachments", attachments(accessToken, mailbox, (String) message.get("id"), downloadAttachments,
					executingInsight));
		}
		return output;
	}

	/**
	 * Describe the attachments of a message, saving them when the caller asked and
	 * the engine allows it.
	 *
	 * @param accessToken      the token to read with
	 * @param mailbox          the mailbox the message is in
	 * @param messageId        the message whose attachments to describe
	 * @param download         whether the files are written into the insight
	 * @param executingInsight the insight this call is running under, or null
	 * @return the attachments
	 */
	private List<Map<String, Object>> attachments(String accessToken, String mailbox, String messageId,
			boolean download, Insight executingInsight) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> attachment : this.mail.listAttachments(accessToken, mailbox, messageId)) {
			Map<String, Object> entry = new LinkedHashMap<>();
			String name = String.valueOf(attachment.get("name"));
			entry.put("name", name);
			// json numbers come back as doubles, and a byte count reads oddly as one
			Object size = attachment.get("size");
			if (size instanceof Number) {
				entry.put("size", ((Number) size).longValue());
			}
			MicrosoftOutlookMessageMapper.putIfPresent(entry, "contentType", attachment.get("contentType"));

			Object contentBytes = attachment.get("contentBytes");
			if (download && contentBytes != null && executingInsight != null) {
				try {
					File saved = this.engine.saveAttachmentBytes(name,
							Base64.getDecoder().decode(contentBytes.toString()), executingInsight);
					if (saved != null) {
						// the name, not the resolved path, so the caller is not handed
						// the server side layout of the insight folder
						entry.put("savedAs", saved.getName());
					}
				} catch (IOException | IllegalArgumentException e) {
					classLogger.warn("Could not save the attachment " + name, e);
				}
			}
			described.add(entry);
		}
		return described;
	}

	/**
	 * Mark messages read or unread.
	 *
	 * @param accessToken the token to write with
	 * @param mailbox     the mailbox the messages are in
	 * @param ids         the messages to mark
	 * @param read        true to mark read, false to mark unread
	 * @return how many were changed
	 */
	public int mark(String accessToken, String mailbox, List<String> ids, boolean read) {
		int changed = 0;
		for (String id : ids) {
			this.mail.setRead(accessToken, mailbox, id, read);
			changed++;
		}
		return changed;
	}

	/**
	 * Move messages to another folder.
	 *
	 * @param accessToken       the token to write with
	 * @param mailbox           the mailbox the messages are in
	 * @param ids               the messages to move
	 * @param destinationFolder the folder to move them to
	 * @return the ids the moved messages now have, since Graph reissues them
	 */
	public List<String> move(String accessToken, String mailbox, List<String> ids, String destinationFolder) {
		List<String> moved = new ArrayList<>();
		for (String id : ids) {
			moved.add(this.mail.moveMessage(accessToken, mailbox, id, destinationFolder));
		}
		return moved;
	}

	/**
	 * Delete messages, which on Graph means moving them to Deleted Items.
	 *
	 * @param accessToken the token to write with
	 * @param mailbox     the mailbox the messages are in
	 * @param ids         the messages to delete
	 * @return how many were deleted
	 */
	public int delete(String accessToken, String mailbox, List<String> ids) {
		int deleted = 0;
		for (String id : ids) {
			this.mail.deleteMessage(accessToken, mailbox, id);
			deleted++;
		}
		return deleted;
	}

}
