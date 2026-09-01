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
package prerna.engine.impl.function.mail.adapter.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.auth.microsoft365.Microsoft365MailOAuth;
import prerna.engine.impl.function.mail.model.MailSearchRequest;
import prerna.engine.impl.function.mail.model.MailSearchResult;
import prerna.engine.impl.function.mail.model.MailboxActionResult;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailboxClient;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMessageMapper;

/**
 * Reading a mailbox through Microsoft Graph.
 *
 * <p>
 * The Graph calls themselves are {@link MicrosoftOutlookMailHelper} and
 * describing a message is {@link MicrosoftOutlookMessageMapper}, both of which
 * are deliberately generic and are shared with the reactors that read a signed
 * in user's own mail. What is left here is what only an engine needs: turning a
 * search into a Graph query, and applying the policy the SMSS set.
 *
 * <p>
 * Two things differ from reading the same mailbox over a protocol. A message is
 * named with an opaque string rather than a number, and a delete moves the
 * message to Deleted Items rather than expunging it. Everything else, including
 * the shape of the answer, is the same.
 */
public final class GraphMailboxClient implements MailboxClient {

	private static final Logger classLogger = LogManager.getLogger(GraphMailboxClient.class);

	private final MicrosoftOutlookMailHelper mail;
	private final MicrosoftGraphAppTokenProvider tokenProvider;
	private final String mailbox;
	private final MailReadPolicy policy;
	private final AttachmentStore attachmentStore;
	private final Supplier<String> authenticationHint;
	private final boolean markAsRead;

	/**
	 * @param mailbox            the mailbox to read, which this engine was
	 *                           cataloged against
	 * @param policy             how much of it may be returned
	 * @param attachmentStore    where an attachment is written when one is asked
	 *                           for
	 * @param authenticationHint what to check when Graph refuses, supplied as a
	 *                           function because it depends on what the engine is
	 *                           doing at the time
	 * @param markAsRead         whether reading a message marks it read
	 * @param mail               the Graph calls to make
	 * @param tokenProvider      the provider holding the app registration's tokens
	 */
	public GraphMailboxClient(MicrosoftOutlookMailHelper mail, MicrosoftGraphAppTokenProvider tokenProvider,
			String mailbox, MailReadPolicy policy, AttachmentStore attachmentStore, Supplier<String> authenticationHint,
			boolean markAsRead) {
		this.mail = mail;
		this.tokenProvider = tokenProvider;
		this.mailbox = mailbox;
		this.policy = policy;
		this.attachmentStore = attachmentStore;
		this.authenticationHint = authenticationHint;
		this.markAsRead = markAsRead;
	}

	/**
	 * Search the mailbox.
	 *
	 * <p>
	 * More messages are asked for than the caller wants whenever a sender allowlist
	 * is in force, because that filter is applied here rather than by Graph and the
	 * search would otherwise come back short.
	 */
	@Override
	public MailSearchResult search(MailSearchRequest request) {
		try {
			MicrosoftOutlookMailHelper.MessageQuery query = new MicrosoftOutlookMailHelper.MessageQuery();
			query.folder = "INBOX".equalsIgnoreCase(request.folder()) ? "inbox" : request.folder();
			query.from = request.criteria().from();
			query.subject = request.criteria().subject();
			query.since = request.criteria().since() == null ? null : java.util.Date.from(request.criteria().since());
			query.unreadOnly = request.criteria().unreadOnly();
			query.includeBody = request.includeBody();
			query.top = this.policy.allowedSenderDomains().isEmpty() ? request.limit()
					: Math.min(request.limit() * 4, 200);

			String token = token();
			List<Map<String, Object>> found = this.mail.listMessages(token, this.mailbox, query);
			List<Map<String, Object>> messages = new ArrayList<>();
			for (Map<String, Object> message : found) {
				if (messages.size() >= request.limit()) {
					break;
				}
				String sender = MicrosoftOutlookMessageMapper.addressOf(message.get("from"));
				if (!this.policy.isSenderAllowed(sender)) {
					continue;
				}
				messages.add(toMessageMap(token, message, request));
				if (this.markAsRead && !Boolean.TRUE.equals(message.get("isRead"))) {
					this.mail.setRead(token, this.mailbox, String.valueOf(message.get("id")), true);
				}
			}
			return new MailSearchResult(request.folder(), messages, 0);
		} catch (RuntimeException e) {
			throw graphError(e);
		}
	}

	/**
	 * Describe one message, and its attachments when it has any.
	 *
	 * @param token   the token to read attachments with
	 * @param message the message as Graph returned it
	 * @param request what the caller asked for
	 * @return the message as a map
	 */
	private Map<String, Object> toMessageMap(String token, Map<String, Object> message, MailSearchRequest request) {
		Map<String, Object> output = MicrosoftOutlookMessageMapper.toMessage(message, request.includeBody(),
				this.policy.maxBodyChars());
		if (Boolean.TRUE.equals(message.get("hasAttachments"))) {
			output.put("attachments", attachments(token, String.valueOf(message.get("id")), request));
		}
		return output;
	}

	/**
	 * Describe the attachments of a message, saving them when the caller asked for
	 * that.
	 *
	 * <p>
	 * Graph hands attachments over already encoded rather than as a stream, so this
	 * is one call per message that has any. A file that cannot be written is logged
	 * and still described, since the message itself was read successfully.
	 *
	 * @param token     the token to read with
	 * @param messageId the message whose attachments to describe
	 * @param request   what the caller asked for
	 * @return the attachments
	 */
	private List<Map<String, Object>> attachments(String token, String messageId, MailSearchRequest request) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> attachment : this.mail.listAttachments(token, this.mailbox, messageId)) {
			Map<String, Object> entry = new LinkedHashMap<>();
			String name = String.valueOf(attachment.get("name"));
			entry.put("name", name);
			if (attachment.get("size") instanceof Number size) {
				entry.put("size", size.longValue());
			}
			MicrosoftOutlookMessageMapper.putIfPresent(entry, "contentType", attachment.get("contentType"));

			Object content = attachment.get("contentBytes");
			if (request.downloadAttachments() && request.insight() == null) {
				classLogger.warn("Attachments can only be downloaded from within an insight that holds the files");
			} else if (request.downloadAttachments() && content != null) {
				try {
					File saved = this.attachmentStore.save(name, Base64.getDecoder().decode(content.toString()),
							request.insight());
					if (saved != null) {
						entry.put("savedAs", saved.getName());
					}
				} catch (IOException | IllegalArgumentException e) {
					classLogger.warn("Could not save attachment " + name, e);
				}
			}
			described.add(entry);
		}
		return described;
	}

	@Override
	public MailboxActionResult mark(String folder, List<String> messageIds, boolean read) {
		return call(() -> {
			String token = token();
			int changed = 0;
			for (String id : messageIds) {
				this.mail.setRead(token, this.mailbox, id, read);
				changed++;
			}
			return MailboxActionResult.of(changed, messageIds);
		});
	}

	@Override
	public MailboxActionResult move(String folder, List<String> messageIds, String destinationFolder) {
		return call(() -> {
			String token = token();
			List<String> moved = new ArrayList<>();
			for (String id : messageIds) {
				moved.add(this.mail.moveMessage(token, this.mailbox, id, destinationFolder));
			}
			return MailboxActionResult.of(moved.size(), moved);
		});
	}

	@Override
	public MailboxActionResult delete(String folder, List<String> messageIds) {
		return call(() -> {
			String token = token();
			int deleted = 0;
			for (String id : messageIds) {
				this.mail.deleteMessage(token, this.mailbox, id);
				deleted++;
			}
			return MailboxActionResult.of(deleted, messageIds);
		});
	}

	/**
	 * Run one mailbox change, turning a refusal into an error that says what to
	 * check.
	 *
	 * @param operation the change to make
	 * @return what it did
	 */
	private MailboxActionResult call(Supplier<MailboxActionResult> operation) {
		try {
			return operation.get();
		} catch (RuntimeException e) {
			throw graphError(e);
		}
	}

	/**
	 * @return the bearer token for this call, asked for per call rather than held,
	 *         so one that expired since the last is replaced
	 */
	private String token() {
		return this.tokenProvider.getAccessToken();
	}

	/**
	 * Log what the token said and turn a refusal into the error a caller sees.
	 *
	 * <p>
	 * The claims go in the log rather than the error because they separate the
	 * causes that look identical from outside: the wrong scope, missing consent, or
	 * a mailbox the application was never granted. The token is dropped as well,
	 * since a permission that has just been changed would otherwise go on being
	 * refused for the rest of the token's hour.
	 *
	 * @param error what Graph refused with
	 * @return the error to throw
	 */
	private RuntimeException graphError(RuntimeException error) {
		classLogger.error("Graph refused a call for {} and {}", this.mailbox,
				Microsoft365MailOAuth.tokenDiagnostic(this.tokenProvider));
		this.tokenProvider.invalidate();
		String hint = this.authenticationHint == null ? null : this.authenticationHint.get();
		return hint == null || hint.isEmpty() ? error
				: new IllegalArgumentException(error.getMessage() + " " + hint, error);
	}
}
