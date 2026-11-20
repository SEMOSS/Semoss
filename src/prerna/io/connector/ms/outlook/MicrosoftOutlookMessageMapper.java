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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;

import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailFolder;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.MailRecipients;
import prerna.util.ValueUtils;

/**
 * Turns the json Graph returns for a message into the map this codebase answers
 * with.
 *
 * <p>
 * Kept apart from both callers because they are otherwise unrelated: the mail
 * function engines read a mailbox they were configured with using an app only
 * token, and the Outlook reactors read the signed in user's own mailbox using a
 * delegated one. What a message looks like on the way out should not depend on
 * which of those asked, so the mapping lives here and neither owns it.
 *
 * <p>
 * The shape is the one the IMAP and POP3 engines settled on rather than
 * anything Graph suggests, so a caller reading over Graph sees the same keys it
 * would reading over a protocol. The visible differences are what only Graph
 * reports: the uid, which is Graph's opaque id rather than a number; the
 * conversationId, which ties the messages of one thread together; the sender's
 * display name; and, when a thread is read, each message's unique body, its
 * text without the earlier messages it quotes.
 */
public class MicrosoftOutlookMessageMapper {

	private MicrosoftOutlookMessageMapper() {

	}

	/**
	 * Describe one message.
	 *
	 * <p>
	 * Attachments are left to the caller, since deciding what to do with them -
	 * naming them, saving them, or leaving them alone - is where an app only reader
	 * and a delegated one genuinely differ.
	 *
	 * @param message      the message as Graph returned it
	 * @param includeBody  whether the body text comes back
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @return the message as a map
	 */
	public static Map<String, Object> toMessage(Map<String, Object> message, boolean includeBody, int maxBodyChars) {
		Map<String, Object> output = new LinkedHashMap<>();
		// graph names a message with an opaque string, where a protocol uses a
		// number. it round trips the same way, which is all a caller does with it
		output.put("uid", message.get("id"));
		putIfPresent(output, "messageId", message.get("internetMessageId"));
		putIfPresent(output, "conversationId", message.get("conversationId"));
		putIfPresent(output, "from", addressOf(message.get("from")));
		putIfPresent(output, "fromName", nameOf(message.get("from")));
		putIfPresent(output, "to", addressList(message.get("toRecipients")));
		putIfPresent(output, "cc", addressList(message.get("ccRecipients")));
		putIfPresent(output, "subject", message.get("subject"));
		putIfPresent(output, "sentDate", message.get("sentDateTime"));
		putIfPresent(output, "receivedDate", message.get("receivedDateTime"));
		output.put("unread", !Boolean.TRUE.equals(message.get("isRead")));

		if (includeBody) {
			String body = bodyOf(message);
			if (maxBodyChars > 0 && body.length() > maxBodyChars) {
				body = body.substring(0, maxBodyChars) + " ... [truncated]";
				output.put("bodyTruncated", true);
			}
			output.put("body", body);

			// only asked for by a thread read; the text of this message alone
			if (message.get("uniqueBody") instanceof Map) {
				String uniqueBody = textOf((Map<?, ?>) message.get("uniqueBody"));
				if (maxBodyChars > 0 && uniqueBody.length() > maxBodyChars) {
					uniqueBody = uniqueBody.substring(0, maxBodyChars) + " ... [truncated]";
					output.put("uniqueBodyTruncated", true);
				}
				output.put("uniqueBody", uniqueBody);
			}
		}
		return output;
	}

	/**
	 * Describe one attachment, without its bytes.
	 *
	 * <p>
	 * What an attachment is matters more than it might seem. A file attachment
	 * carries its own bytes and can be written out. An item attachment is another
	 * message or event embedded in this one, and a reference attachment is a link
	 * to a file living in a drive, so neither has bytes here to save. That is what
	 * {@code isFile} says, and it is what
	 * {@code MicrosoftOutlookDownloadAttachment} checks before writing anything.
	 * </p>
	 *
	 * @param attachment the attachment as Graph returned it
	 * @return the attachment as a map
	 */
	public static Map<String, Object> toAttachment(Map<String, Object> attachment) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", attachment.get("id"));
		putIfPresent(output, "name", attachment.get("name"));
		putIfPresent(output, "contentType", attachment.get("contentType"));
		putIfPresent(output, "size", attachment.get("size"));
		putIfPresent(output, "lastModifiedDateTime", attachment.get("lastModifiedDateTime"));
		output.put("isInline", Boolean.TRUE.equals(attachment.get("isInline")));
		output.put("type", attachment.get("@odata.type"));
		output.put("isFile", isFileAttachment(attachment));
		return output;
	}

	/**
	 * @param attachment an attachment as Graph returned it
	 * @return true when the attachment carries bytes of its own that can be written
	 *         out
	 */
	public static boolean isFileAttachment(Map<String, Object> attachment) {
		return "#microsoft.graph.fileAttachment".equals(String.valueOf(attachment.get("@odata.type")));
	}

	/**
	 * The readable text of a message, preferring what Graph says is plain over
	 * markup, the same way the protocol engines do.
	 *
	 * @param message the message as Graph returned it
	 * @return the body text, empty when there is none
	 */
	public static String bodyOf(Map<String, Object> message) {
		Object body = message.get("body");
		if (!(body instanceof Map)) {
			Object preview = message.get("bodyPreview");
			return preview == null ? "" : preview.toString().trim();
		}
		return textOf((Map<?, ?>) body);
	}

	/**
	 * The readable text of one Graph item body, a {@code body} or a
	 * {@code uniqueBody}.
	 *
	 * @param body the item body as Graph returned it
	 * @return the text, empty when there is none
	 */
	private static String textOf(Map<?, ?> body) {
		String content = body.get("content") == null ? "" : body.get("content").toString();
		if ("html".equalsIgnoreCase(String.valueOf(body.get("contentType")))) {
			// the markup is noise to whoever asked what the message says
			return Jsoup.parse(content).text().trim();
		}
		return content.trim();
	}

	/**
	 * The address out of a Graph recipient object.
	 *
	 * @param recipient the {@code from} or one entry of a recipient collection
	 * @return the address, or null when there is none
	 */
	public static String addressOf(Object recipient) {
		if (!(recipient instanceof Map)) {
			return null;
		}
		Object emailAddress = ((Map<?, ?>) recipient).get("emailAddress");
		if (!(emailAddress instanceof Map)) {
			return null;
		}
		Object address = ((Map<?, ?>) emailAddress).get("address");
		return address == null ? null : address.toString();
	}

	/**
	 * The display name out of a Graph recipient object.
	 *
	 * @param recipient the {@code from} or one entry of a recipient collection
	 * @return the name, or null when there is none
	 */
	public static String nameOf(Object recipient) {
		if (!(recipient instanceof Map)) {
			return null;
		}
		Object emailAddress = ((Map<?, ?>) recipient).get("emailAddress");
		if (!(emailAddress instanceof Map)) {
			return null;
		}
		Object name = ((Map<?, ?>) emailAddress).get("name");
		return name == null || name.toString().trim().isEmpty() ? null : name.toString().trim();
	}

	/**
	 * The addresses of one recipient collection, joined the way the protocol
	 * engines join them.
	 *
	 * @param recipients the collection as Graph returned it
	 * @return the addresses joined, or null when there are none
	 */
	public static String addressList(Object recipients) {
		String[] addresses = addressArray(recipients);
		return addresses == null ? null : String.join(", ", addresses);
	}

	/**
	 * The addresses of one recipient collection, for a caller that wants them
	 * separately rather than joined.
	 *
	 * @param recipients the collection as Graph returned it
	 * @return the addresses, or null when there are none
	 */
	public static String[] addressArray(Object recipients) {
		if (!(recipients instanceof List)) {
			return null;
		}
		List<String> addresses = new ArrayList<>();
		for (Object recipient : (List<?>) recipients) {
			String address = addressOf(recipient);
			if (address != null) {
				addresses.add(address);
			}
		}
		if (addresses.isEmpty()) {
			return null;
		}
		return addresses.toArray(new String[0]);
	}

	/**
	 * Set a key only when there is something to set it to, so a caller reading the
	 * output does not have to tell a null apart from an absent field.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, ignored when null
	 */
	public static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

	/**
	 * Describe one message the way every mail reactor answers with it.
	 *
	 * @param message         the message as Graph returned it
	 * @param attachments     what is attached, or null when it was not asked for
	 * @param displayBody     the body for showing to a person, or null
	 * @param replyRecipients who a reply to everybody goes to, or null
	 * @return the message
	 */
	public static MailMessage toMailMessage(Map<String, Object> message, List<MailAttachment> attachments,
			Map<String, Object> displayBody, MailRecipients replyRecipients) {
		String uniqueBody = message.get("uniqueBody") instanceof Map ? textOf((Map<?, ?>) message.get("uniqueBody"))
				: null;
		return new MailMessage(ValueUtils.toStringOrNull(message.get("id")),
				ValueUtils.toStringOrNull(message.get("internetMessageId")),
				ValueUtils.toStringOrNull(message.get("conversationId")), addressOf(message.get("from")),
				nameOf(message.get("from")), addresses(message.get("toRecipients")),
				addresses(message.get("ccRecipients")), ValueUtils.toStringOrNull(message.get("subject")),
				ConnectorTimes.parseProviderTime(ValueUtils.toStringOrNull(message.get("sentDateTime"))),
				ConnectorTimes.parseProviderTime(ValueUtils.toStringOrNull(message.get("receivedDateTime"))),
				!Boolean.TRUE.equals(message.get("isRead")), Boolean.TRUE.equals(message.get("hasAttachments")),
				bodyOf(message), uniqueBody, ValueUtils.toStringOrNull(message.get("webLink")), attachments,
				displayBody, replyRecipients);
	}

	/**
	 * Describe a draft the way the mail reactors that write answer with it.
	 *
	 * @param draft       the draft as Graph returned it
	 * @param body        the body as the caller wrote it, or null to read it off
	 *                    the draft
	 * @param html        whether that body is html
	 * @param attachments the names of the files attached
	 * @return the draft
	 */
	public static ComposedMail toComposedMail(Map<String, Object> draft, String body, boolean html,
			List<String> attachments) {
		return new ComposedMail(ValueUtils.toStringOrNull(draft.get("id")),
				ValueUtils.toStringOrNull(draft.get("conversationId")), ValueUtils.toStringOrNull(draft.get("webLink")),
				addresses(draft.get("toRecipients")), addresses(draft.get("ccRecipients")),
				addresses(draft.get("bccRecipients")), ValueUtils.toStringOrNull(draft.get("subject")),
				body == null ? bodyOf(draft) : body, body != null && html, attachments);
	}

	/**
	 * Describe one attachment the way every mail reactor answers with it.
	 *
	 * @param attachment the attachment as Graph returned it
	 * @return the attachment
	 */
	public static MailAttachment toMailAttachment(Map<String, Object> attachment) {
		String type = String.valueOf(attachment.get("@odata.type"));
		String kind = type.endsWith("itemAttachment") ? MailAttachment.ITEM
				: type.endsWith("referenceAttachment") ? MailAttachment.LINK : MailAttachment.FILE;
		Object size = attachment.get("size");
		return new MailAttachment(ValueUtils.toStringOrNull(attachment.get("id")),
				ValueUtils.toStringOrNull(attachment.get("name")),
				ValueUtils.toStringOrNull(attachment.get("contentType")),
				size instanceof Number ? ((Number) size).longValue() : null,
				Boolean.TRUE.equals(attachment.get("isInline")), kind);
	}

	/**
	 * Describe one folder the way every mail reactor answers with it.
	 *
	 * @param folder the folder as Graph returned it
	 * @return the folder
	 */
	public static MailFolder toMailFolder(Map<String, Object> folder) {
		Object total = folder.get("totalItemCount");
		Object unread = folder.get("unreadItemCount");
		return new MailFolder(ValueUtils.toStringOrNull(folder.get("id")),
				ValueUtils.toStringOrNull(folder.get("displayName")), MailFolder.FOLDER,
				total instanceof Number ? ((Number) total).longValue() : null,
				unread instanceof Number ? ((Number) unread).longValue() : null);
	}

	/**
	 * @param recipients a recipient collection as Graph returned it
	 * @return the addresses, empty when there are none
	 */
	public static List<String> addresses(Object recipients) {
		String[] addresses = addressArray(recipients);
		return addresses == null ? List.of() : List.of(addresses);
	}

}
