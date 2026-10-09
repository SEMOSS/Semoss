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

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Replies to a message, keeping the answer in its thread with the message
 * being answered quoted underneath.
 */
public abstract class AbstractReplyMailReactor extends AbstractDraftDeliveryReactor {

	private static final String[] KEYS = { ID, BODY, REPLY_ALL, AS_DRAFT, HTML, OVERRIDE_RECIPIENTS, TO, CC,
			ATTACHMENTS };

	/**
	 * A reply the caller wrote.
	 *
	 * @param id          the message being answered
	 * @param body        what the reply says, above the quoted message
	 * @param html        whether the body is html rather than plain text
	 * @param replyAll    whether everybody on the message is answered, rather than
	 *                    only whoever sent it
	 * @param recipients  exactly who the reply goes to, or null for whoever the
	 *                    reply goes to on its own
	 * @param attachments the files to attach
	 */
	public record ReplyMailRequest(String id, String body, boolean html, boolean replyAll, MailRecipients recipients,
			List<File> attachments) {

		public ReplyMailRequest {
			attachments = attachments == null ? List.of() : List.copyOf(attachments);
		}
	}

	protected AbstractReplyMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID, BODY);
	}

	/**
	 * Write the reply as a draft in the thread of the message it answers, without
	 * sending it.
	 *
	 * @param user    the signed in user
	 * @param request the reply
	 * @return the draft as saved, with its recipients
	 * @throws Exception when the draft cannot be written
	 */
	protected abstract ComposedMail draftReply(User user, ReplyMailRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("answer the message", () -> {
			String id = requireId("answer a message");
			String body = this.keyValue.get(BODY);
			if (body == null || body.trim().isEmpty()) {
				throw new IllegalArgumentException("A " + BODY + " is required to answer a message.");
			}
			boolean replyAll = readBoolean(REPLY_ALL, false);
			MailRecipients recipients = readBoolean(OVERRIDE_RECIPIENTS, false)
					? new MailRecipients(MailRecipientRules.validate(readValues(TO)),
							MailRecipientRules.validate(readValues(CC)))
					: null;
			boolean asDraft = readBoolean(AS_DRAFT, false);
			if (recipients != null && recipients.to().isEmpty() && recipients.cc().isEmpty() && !asDraft) {
				// checked before the draft is written, so a reply that cannot go anywhere
				// leaves nothing behind in Drafts
				throw new IllegalArgumentException(
						"At least one recipient in " + TO + " or " + CC + " is required to send a reply to chosen recipients.");
			}
			ReplyMailRequest request = new ReplyMailRequest(id, body, readBoolean(HTML, false), replyAll, recipients,
					readInsightFiles(ATTACHMENTS));
			ComposedMail draft = draftReply(this.insight.getUser(), request);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("repliedTo", id);
			output.put(REPLY_ALL, replyAll);
			output.putAll(finish(draft, !asDraft));
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "mail/compose?intent=reply";
	}

	@Override
	protected final String describe() {
		return "Reply to a message in the signed in user's own " + mailbox()
				+ ", keeping the answer in its thread with the message being answered quoted underneath.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case BODY:
			return "What the reply says, above the quoted message.";
		case REPLY_ALL:
			return "Optional boolean to answer everybody on the message rather than only whoever sent it. Defaults to false.";
		case OVERRIDE_RECIPIENTS:
			return "Optional boolean to send to exactly the to and cc lists passed, including empty lists, instead "
					+ "of whoever the reply goes to on its own. Defaults to false.";
		case TO:
			return "Recipients when overrideRecipients is true, as a list of email addresses.";
		case CC:
			return "Copied recipients when overrideRecipients is true, as a list of email addresses.";
		default:
			return super.describeKey(key);
		}
	}
}
