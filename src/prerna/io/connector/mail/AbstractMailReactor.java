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
import java.util.List;
import java.util.concurrent.Callable;

import prerna.io.connector.AbstractConnectorAppReactor;
import prerna.io.connector.IConnectorApp;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.EmailUtility;
import prerna.util.EmailUtility.EmailMetadata;

/**
 * What every mail reactor has in common, whichever mailbox it works against.
 *
 * <p>
 * The keys mean the same thing in every operation and for every mailbox, so
 * they are named, read and described once here. So is the one rule every
 * reactor that sends mail follows: the send passes through
 * {@link EmailUtility#sendEmail(Callable, EmailMetadata)}, so it is recorded
 * however it went out.
 * </p>
 *
 * <p>
 * Every one of these works against the signed in user's own mailbox, as the
 * user. Nothing takes a sender or a mailbox, because the token is what says
 * whose mail this is.
 * </p>
 */
public abstract class AbstractMailReactor extends AbstractConnectorAppReactor {

	public static final String ID = "id";
	public static final String FOLDER = "folder";
	public static final String LIMIT = "limit";
	public static final String OFFSET = "offset";
	public static final String SUBJECT = "subject";
	public static final String FROM = "from";
	public static final String UNREAD_ONLY = "unreadOnly";
	public static final String SINCE_DAYS = "sinceDays";
	public static final String INCLUDE_BODY = "includeBody";
	public static final String MAX_BODY_CHARS = "maxBodyChars";
	public static final String CONVERSATION_ID = "conversationId";
	public static final String INCLUDE_ATTACHMENTS = "includeAttachments";
	public static final String INCLUDE_DISPLAY_BODY = "includeDisplayBody";
	public static final String INCLUDE_REPLY_RECIPIENTS = "includeReplyRecipients";
	public static final String TO = "to";
	public static final String CC = "cc";
	public static final String BCC = "bcc";
	public static final String BODY = "body";
	public static final String HTML = "html";
	public static final String ATTACHMENTS = "attachments";
	public static final String REPLY_ALL = "replyAll";
	public static final String AS_DRAFT = "asDraft";
	public static final String OVERRIDE_RECIPIENTS = "overrideRecipients";
	public static final String READ = "read";
	public static final String ATTACHMENT_ID = "attachmentId";
	public static final String FILE_NAME = "fileName";

	/**
	 * Outlook only: whether a sent message keeps a copy in Sent Items. Gmail always
	 * keeps one.
	 */
	public static final String SAVE_TO_SENT_ITEMS = "saveToSentItems";

	/** How much of a body comes back before it is cut short. */
	public static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	/**
	 * @return the mailbox this reactor works against
	 */
	protected abstract MailApp getMailApp();

	@Override
	protected final IConnectorApp getApp() {
		return getMailApp();
	}

	/**
	 * @param operation the operation, such as ListMail
	 * @return the name of this mailbox's reactor for it, for a description to point
	 *         at
	 */
	protected final String reactorName(String operation) {
		return getMailApp().getReactorPrefix() + operation;
	}

	/**
	 * @return the mailbox as a description names it, such as "Gmail mailbox"
	 */
	protected final String mailbox() {
		return getMailApp().getDisplayName() + " mailbox";
	}

	@Override
	protected String describeKey(String key) {
		switch (key) {
		case ID:
			return "Id of the message, as returned by " + reactorName("ListMail") + " or " + reactorName("GetMail")
					+ ".";
		case LIMIT:
			return "Optional number of results to return.";
		case OFFSET:
			return "Optional number of results to skip, to read the page after one already read. Defaults to 0; "
					+ "hasMore in the result says whether there is another page.";
		case TO:
			return "Recipients, as a list of email addresses.";
		case CC:
			return "Optional recipients to copy, as a list of email addresses.";
		case BCC:
			return "Optional recipients to blind copy, as a list of email addresses.";
		case SUBJECT:
			return "Subject line of the email.";
		case BODY:
			return "Body of the email.";
		case HTML:
			return "Optional boolean for whether the body is html rather than plain text. Defaults to false.";
		case ATTACHMENTS:
			return "Optional files to attach, as a list of paths relative to the insight folder.";
		case AS_DRAFT:
			return "Optional boolean to leave the message in Drafts instead of sending it, so somebody can read it "
					+ "first and send it with " + reactorName("SendDraft") + ". Defaults to false.";
		case MAX_BODY_CHARS:
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		case SAVE_TO_SENT_ITEMS:
			return "Optional boolean for whether a copy is kept in Sent Items. Defaults to true.";
		default:
			return null;
		}
	}

	/**
	 * Read the message this reactor was pointed at.
	 *
	 * @param toDo what is being done to it, used in the error
	 * @return the message id
	 */
	protected String requireId(String toDo) {
		return requireString(ID,
				"An " + ID + ", as returned by " + reactorName("ListMail") + ", is required to " + toDo + ".");
	}

	/**
	 * Read a message the caller wrote.
	 *
	 * @param requireContent whether a recipient and a body have to be there, which
	 *                       they do to send and do not to save a draft somebody is
	 *                       going to finish
	 * @param toDo           what this is being written for, used in the errors
	 * @return the message
	 * @throws Exception when an attachment cannot be resolved
	 */
	protected OutgoingMail readOutgoing(boolean requireContent, String toDo) throws Exception {
		List<File> attachments = readInsightFiles(ATTACHMENTS);
		OutgoingMail mail = new OutgoingMail(MailRecipientRules.validate(readValues(TO)),
				MailRecipientRules.validate(readValues(CC)), MailRecipientRules.validate(readValues(BCC)),
				readString(SUBJECT), this.keyValue.get(BODY), readBoolean(HTML, false), attachments);
		if (requireContent) {
			if (!mail.hasRecipients()) {
				throw new SemossPixelException(
						"At least one of " + TO + ", " + CC + " or " + BCC + " is required to " + toDo + ".");
			}
			if (mail.body() == null || mail.body().trim().isEmpty()) {
				throw new SemossPixelException("A " + BODY + " is required to " + toDo + ".");
			}
		}
		return mail;
	}

	/**
	 * Send a message and record that it was sent, however it goes out.
	 *
	 * @param mail     what is being sent, which is what gets recorded
	 * @param delivery the provider call that sends it
	 * @return what the provider answered with
	 * @throws Exception whatever the provider threw, after the attempt is recorded
	 */
	protected final ComposedMail deliver(ComposedMail mail, Callable<ComposedMail> delivery) throws Exception {
		EmailMetadata metadata = new EmailMetadata(array(mail.to()), array(mail.cc()), array(mail.bcc()),
				accountEmail(this.insight.getUser()), mail.subject(), mail.body(), mail.html(),
				array(mail.attachments()));
		return EmailUtility.sendEmail(delivery, metadata);
	}

	private static String[] array(List<String> values) {
		return values == null || values.isEmpty() ? null : values.toArray(new String[0]);
	}
}
