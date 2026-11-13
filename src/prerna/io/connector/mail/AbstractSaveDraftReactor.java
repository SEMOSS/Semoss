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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Saves a message as a draft, for the user to finish and send. Nothing is
 * required, since a draft is meant to be finished by hand.
 */
public abstract class AbstractSaveDraftReactor extends AbstractComposeMailReactor {

	public static final String REPLY_TO = "replyTo";

	// an original typed into the body is a fake reply; a real one is made with replyTo
	private static final Pattern PASTED_HISTORY = Pattern
			.compile("(?im)^\\s*-{2,}\\s*original message\\s*-{2,}\\s*$|^\\s*from:.*\\R\\s*(sent|date):.*\\R\\s*to:");

	protected AbstractSaveDraftReactor(String... extraKeys) {
		super(extraKeys);
	}

	/**
	 * Save the message in the user's drafts.
	 *
	 * @param user the signed in user
	 * @param mail what the caller wrote
	 * @return the draft as saved, with the id that sends it
	 * @throws Exception when the draft cannot be saved
	 */
	protected abstract ComposedMail saveDraft(User user, OutgoingMail mail) throws Exception;

	/**
	 * Save a reply in the thread of the message it answers, with that message
	 * quoted by the mailbox. A mailbox that cannot says so; its reactor does not
	 * offer {@code replyTo}.
	 *
	 * @param user    the signed in user
	 * @param request the reply
	 * @return the draft as saved
	 * @throws Exception when the draft cannot be saved
	 */
	protected ComposedMail saveReply(User user, AbstractReplyMailReactor.ReplyMailRequest request) throws Exception {
		throw new IllegalArgumentException("This mailbox cannot save a reply draft; save a new draft without " + REPLY_TO + ".");
	}

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("save the draft", () -> {
			String replyTo = readString(REPLY_TO);
			ComposedMail draft;
			String repliedTo = null;
			if (replyTo != null && !replyTo.isBlank()) {
				repliedTo = replyTo.trim();
				draft = saveReply(this.insight.getUser(), readReply(repliedTo));
			} else {
				OutgoingMail mail = readOutgoing(false, "save a draft");
				if (takesReply() && mail.body() != null && PASTED_HISTORY.matcher(mail.body()).find()) {
					throw new IllegalArgumentException("The body quotes an original message. Do not type the original into "
							+ "the body: pass " + REPLY_TO + " (the id or emailRef of the message being answered) and write "
							+ "only your new text, and the original is quoted in the thread for you.");
				}
				draft = saveDraft(this.insight.getUser(), mail);
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("draft", true);
			if (repliedTo != null) {
				output.put("repliedTo", repliedTo);
			}
			output.putAll(draft.toMap());
			return output;
		});
	}

	private AbstractReplyMailReactor.ReplyMailRequest readReply(String id) throws Exception {
		String body = this.keyValue.get(BODY);
		if (body == null || body.trim().isEmpty()) {
			throw new IllegalArgumentException("A " + BODY + " is required to reply to a message.");
		}
		List<String> to = MailRecipientRules.validate(readValues(TO));
		List<String> cc = MailRecipientRules.validate(readValues(CC));
		// to and cc passed with replyTo replace the recipients the reply would have on its own
		MailRecipients recipients = to.isEmpty() && cc.isEmpty() ? null : new MailRecipients(to, cc);
		return new AbstractReplyMailReactor.ReplyMailRequest(id, body, readBoolean(HTML, false),
				readBoolean(REPLY_ALL, false), recipients, readInsightFiles(ATTACHMENTS));
	}

	/**
	 * Whether this reactor saves a reply in the thread it answers, which its
	 * {@code replyTo} key offers.
	 *
	 * @return true when the reactor takes {@code replyTo}
	 */
	private boolean takesReply() {
		return Arrays.asList(this.keysToGet).contains(REPLY_TO);
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "mail/compose?intent=draft";
	}

	@Override
	protected final String describe() {
		String description = "Save an email as a draft in the signed in user's own " + mailbox()
				+ ", for them to review and send.";
		if (!takesReply()) {
			return description;
		}
		return description + " To answer an email, pass " + REPLY_TO + ": the draft is then a real reply in that "
				+ "thread, with the original quoted and the recipients chosen for you.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case TO:
			return "Optional recipients of the draft, as a list of email addresses, which can be left for the "
					+ "person reviewing it to fill in.";
		case SUBJECT:
			if (!takesReply()) {
				return "Optional subject line of the draft.";
			}
			return "Optional subject line of the draft. Ignored with " + REPLY_TO + ", which keeps the thread's subject.";
		case BODY:
			if (!takesReply()) {
				return "Optional body of the draft.";
			}
			return "Optional body of the draft. With " + REPLY_TO + ", only your new text: never type the original "
					+ "message or its From/Sent lines, which are added for you.";
		case REPLY_TO:
			return "Optional id of the message being answered, as an Outlook id or the emailRef a thread read "
					+ "returned. Makes the draft a real reply in that thread.";
		case REPLY_ALL:
			return "Optional boolean with " + REPLY_TO + " to answer everybody on the message rather than only "
					+ "whoever sent it. Defaults to false.";
		default:
			return super.describeKey(key);
		}
	}
}
