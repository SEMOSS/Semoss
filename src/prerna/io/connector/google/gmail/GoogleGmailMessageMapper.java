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
package prerna.io.connector.google.gmail;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MailDateFormat;
import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailFolder;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.io.connector.mail.MailRecipients;
import prerna.io.connector.ms.MicrosoftMessageDisplay;

/**
 * Turns the json Gmail returns for a message or a label into the records every
 * mail reactor answers with.
 *
 * <p>
 * A Gmail message is a tree of MIME parts. The body is the first plain text
 * part, or the first html part when there is no plain one, and anything with a
 * file name is an attachment. Bodies are written out as text the same way
 * Outlook's are, by {@link MicrosoftMessageDisplay}, so a message reads the
 * same whichever mailbox it came from.
 * </p>
 */
public final class GoogleGmailMessageMapper {

	/** The label Gmail keeps on every message nobody has opened. */
	public static final String UNREAD = "UNREAD";

	/**
	 * The labels Gmail keeps that are places mail is filed, in the order a person
	 * expects them.
	 */
	private static final List<String> FILED_SYSTEM_LABELS = List.of("INBOX", "STARRED", "IMPORTANT", "SENT", "DRAFT",
			"SPAM", "TRASH");

	/** How Gmail writes the line above a quoted message. */
	private static final Pattern ATTRIBUTION = Pattern.compile("^On .+wrote:\\s*$");

	/** How Gmail writes the line above a forwarded message. */
	private static final Pattern FORWARDED = Pattern.compile("^-{5,}\\s*Forwarded message\\s*-{5,}\\s*$");

	private GoogleGmailMessageMapper() {

	}

	/**
	 * Describe one message.
	 *
	 * @param message                the message as Gmail returned it in full
	 * @param account                the signed in user's address
	 * @param includeAttachments     whether what is attached is listed
	 * @param includeDisplayBody     whether the body for showing to a person comes
	 *                               back
	 * @param includeReplyRecipients whether who a reply to everybody goes to comes
	 *                               back
	 * @param includeUniqueBody      whether the text of this message alone comes
	 *                               back, without what it quotes
	 * @return the message
	 */
	public static MailMessage toMailMessage(Map<String, Object> message, String account, boolean includeAttachments,
			boolean includeDisplayBody, boolean includeReplyRecipients, boolean includeUniqueBody) {
		Map<?, ?> payload = message.get("payload") instanceof Map<?, ?> map ? map : Map.of();
		List<Map<?, ?>> leaves = new ArrayList<>();
		collectLeaves(payload, leaves);

		String html = htmlBody(leaves);
		String text = textBody(leaves, html);
		List<MailAttachment> attachments = attachments(leaves);
		InternetAddress sender = firstAddress(header(payload, "From"));
		String threadId = stringOf(message.get("threadId"));

		Map<String, Object> displayBody = null;
		if (includeDisplayBody) {
			Map<String, Object> shown = new LinkedHashMap<>();
			shown.put("body",
					Map.of("contentType", html == null ? "text" : "html", "content", html == null ? text : html));
			shown.put("attachments",
					attachments.stream().map(attachment -> Map.of("name", nameOrDefault(attachment))).toList());
			displayBody = MicrosoftMessageDisplay.body(shown, text);
		}
		MailRecipients replyRecipients = includeReplyRecipients ? MailRecipientRules.replyDefaults(
				addresses(header(payload, "Reply-To")), sender == null ? null : sender.getAddress(),
				addresses(header(payload, "To")), addresses(header(payload, "Cc")), account) : null;

		return new MailMessage(stringOf(message.get("id")), header(payload, "Message-ID"), threadId,
				sender == null ? null : sender.getAddress(), sender == null ? null : blankToNull(sender.getPersonal()),
				addresses(header(payload, "To")), addresses(header(payload, "Cc")), header(payload, "Subject"),
				parseDate(header(payload, "Date")), internalDate(message.get("internalDate")),
				labelIds(message).contains(UNREAD), attachments.stream().anyMatch(attachment -> !attachment.isInline()),
				text, includeUniqueBody ? uniqueBody(html, text) : null, webLink(account, threadId),
				includeAttachments ? attachments : null, displayBody, replyRecipients);
	}

	/**
	 * Describe one label as a place mail is filed.
	 *
	 * @param label the label as Gmail returned it
	 * @return the folder
	 */
	public static MailFolder toMailFolder(Map<String, Object> label) {
		boolean system = "system".equalsIgnoreCase(String.valueOf(label.get("type")));
		Object total = label.get("messagesTotal");
		Object unread = label.get("messagesUnread");
		String id = stringOf(label.get("id"));
		return new MailFolder(id, system ? systemName(id) : stringOf(label.get("name")),
				system ? MailFolder.SYSTEM : MailFolder.LABEL,
				total instanceof Number ? ((Number) total).longValue() : null,
				unread instanceof Number ? ((Number) unread).longValue() : null);
	}

	/**
	 * @param label a label as Gmail returned it
	 * @return whether it is a place mail is filed, rather than a state such as
	 *         unread or a category Gmail sorts the inbox by
	 */
	public static boolean isFiled(Map<String, Object> label) {
		if (!"system".equalsIgnoreCase(String.valueOf(label.get("type")))) {
			return true;
		}
		return FILED_SYSTEM_LABELS.contains(String.valueOf(label.get("id")));
	}

	/**
	 * @param label a label as Gmail returned it
	 * @return where it sorts among the labels: the ones Gmail keeps first, in the
	 *         order a person expects them, then the user's own
	 */
	public static int sortOrder(Map<String, Object> label) {
		int index = FILED_SYSTEM_LABELS.indexOf(String.valueOf(label.get("id")));
		return index < 0 ? FILED_SYSTEM_LABELS.size() : index;
	}

	/**
	 * The parts of a message that are files, with what it takes to read their
	 * bytes.
	 *
	 * @param message the message as Gmail returned it in full
	 * @return the parts that carry a file name
	 */
	public static List<Map<?, ?>> attachmentParts(Map<String, Object> message) {
		Map<?, ?> payload = message.get("payload") instanceof Map<?, ?> map ? map : Map.of();
		List<Map<?, ?>> leaves = new ArrayList<>();
		collectLeaves(payload, leaves);
		return leaves.stream().filter(GoogleGmailMessageMapper::isAttachment).toList();
	}

	/**
	 * The id an attachment is known by. Gmail hands out a new attachment id every
	 * time a message is read, so the part's own id, which stays the same, is what a
	 * caller holds and passes back.
	 *
	 * @param part a part of a message
	 * @return the part's id
	 */
	public static String partId(Map<?, ?> part) {
		Object id = part.get("partId");
		return id == null ? attachmentId(part) : id.toString();
	}

	/**
	 * @param part a part of a message, as just read
	 * @return the id the attachment download takes for this read of the message, or
	 *         null when the bytes are held in the part itself
	 */
	public static String attachmentId(Map<?, ?> part) {
		Map<?, ?> body = part.get("body") instanceof Map<?, ?> map ? map : Map.of();
		Object id = body.get("attachmentId");
		return id == null ? null : id.toString();
	}

	/**
	 * @param part a part of a message
	 * @return the bytes the part holds itself, or null when they have to be read
	 *         with the attachment download
	 */
	public static byte[] inlineBytes(Map<?, ?> part) {
		Map<?, ?> body = part.get("body") instanceof Map<?, ?> map ? map : Map.of();
		Object data = body.get("data");
		return data == null ? null : decode(data.toString());
	}

	/**
	 * @param message the message as Gmail returned it in full
	 * @return the message's html body, or null when it has none
	 */
	public static String htmlBody(Map<String, Object> message) {
		Map<?, ?> payload = message.get("payload") instanceof Map<?, ?> map ? map : Map.of();
		List<Map<?, ?>> leaves = new ArrayList<>();
		collectLeaves(payload, leaves);
		return htmlBody(leaves);
	}

	/**
	 * @param message the message as Gmail returned it
	 * @param name    the header
	 * @return the header's value, or null when the message has none
	 */
	public static String messageHeader(Map<String, Object> message, String name) {
		return header(message.get("payload") instanceof Map<?, ?> map ? map : Map.of(), name);
	}

	/**
	 * @param header a header holding addresses, such as To
	 * @return the addresses, empty when there are none or they cannot be read
	 */
	public static List<String> addresses(String header) {
		List<String> addresses = new ArrayList<>();
		if (header == null || header.isBlank()) {
			return addresses;
		}
		try {
			for (InternetAddress address : InternetAddress.parseHeader(header, false)) {
				// a group, such as undisclosed-recipients:;, stands for its members, and
				// often has none
				InternetAddress[] members = address.isGroup() ? address.getGroup(false) : null;
				for (InternetAddress member : members == null ? new InternetAddress[] { address } : members) {
					if (member.getAddress() != null && !member.getAddress().isBlank() && !member.isGroup()) {
						addresses.add(member.getAddress().trim());
					}
				}
			}
		} catch (AddressException e) {
			// a header a client wrote badly is read as nobody rather than failing the read
		}
		return addresses;
	}

	/**
	 * @param account  the signed in user's address, which picks their account when
	 *                 they are signed in to Gmail with more than one
	 * @param threadId the thread
	 * @return where the thread opens in Gmail, or null when there is no thread
	 */
	public static String webLink(String account, String threadId) {
		if (threadId == null) {
			return null;
		}
		String user = account == null ? "" : "?authuser=" + URLEncoder.encode(account, StandardCharsets.UTF_8);
		return "https://mail.google.com/mail/" + user + "#all/" + threadId;
	}

	/**
	 * @param sentOrDraft a sent message, or a draft holding one, as Gmail returned
	 *                    it
	 * @return the thread the message belongs to, or null when Gmail did not say
	 */
	public static String threadOf(Map<String, Object> sentOrDraft) {
		if (sentOrDraft == null) {
			return null;
		}
		if (sentOrDraft.get("message") instanceof Map<?, ?> message) {
			return stringOf(message.get("threadId"));
		}
		return stringOf(sentOrDraft.get("threadId"));
	}

	/**
	 * @param data base64url, as Gmail writes bytes
	 * @return the bytes
	 */
	public static byte[] decode(String data) {
		return Base64.getUrlDecoder().decode(data.replace('+', '-').replace('/', '_').replace("=", ""));
	}

	private static void collectLeaves(Map<?, ?> part, List<Map<?, ?>> leaves) {
		if (part.get("parts") instanceof List<?> children && !children.isEmpty()) {
			for (Object child : children) {
				if (child instanceof Map<?, ?> childPart) {
					collectLeaves(childPart, leaves);
				}
			}
			return;
		}
		leaves.add(part);
	}

	private static boolean isAttachment(Map<?, ?> part) {
		Object filename = part.get("filename");
		return filename != null && !filename.toString().isBlank();
	}

	private static String htmlBody(List<Map<?, ?>> leaves) {
		for (Map<?, ?> part : leaves) {
			if (!isAttachment(part) && "text/html".equalsIgnoreCase(String.valueOf(part.get("mimeType")))) {
				return decodeText(part);
			}
		}
		return null;
	}

	private static String textBody(List<Map<?, ?>> leaves, String html) {
		for (Map<?, ?> part : leaves) {
			if (!isAttachment(part) && "text/plain".equalsIgnoreCase(String.valueOf(part.get("mimeType")))) {
				String text = decodeText(part);
				return text == null ? "" : text.strip();
			}
		}
		return html == null ? "" : htmlToText(html);
	}

	private static String decodeText(Map<?, ?> part) {
		byte[] bytes = inlineBytes(part);
		return bytes == null ? null : new String(bytes, charsetOf(header(part, "Content-Type")));
	}

	/**
	 * @param contentType a part's Content-Type header
	 * @return the charset it names, or UTF-8 when it names none Java knows
	 */
	private static Charset charsetOf(String contentType) {
		if (contentType != null) {
			for (String parameter : contentType.split(";")) {
				String[] pair = parameter.trim().split("=", 2);
				if (pair.length == 2 && "charset".equalsIgnoreCase(pair[0].trim())) {
					try {
						return Charset.forName(pair[1].trim().replace("\"", ""));
					} catch (IllegalArgumentException e) {
						break;
					}
				}
			}
		}
		return StandardCharsets.UTF_8;
	}

	private static String htmlToText(String html) {
		return MicrosoftMessageDisplay.text(Map.of("body", Map.of("contentType", "html", "content", html)));
	}

	/**
	 * The text of a message without the earlier messages it quotes. Gmail wraps a
	 * quote in a {@code gmail_quote} block, and a plain text quote starts at the
	 * line saying who wrote it.
	 */
	private static String uniqueBody(String html, String text) {
		if (html != null) {
			Document document = Jsoup.parse(html);
			document.select(".gmail_quote, blockquote").remove();
			return htmlToText(document.body().html());
		}
		StringBuilder unique = new StringBuilder();
		for (String line : text.split("\n", -1)) {
			String trimmed = line.trim();
			if (ATTRIBUTION.matcher(trimmed).matches() || FORWARDED.matcher(trimmed).matches()
					|| trimmed.startsWith(">")) {
				break;
			}
			unique.append(line).append('\n');
		}
		return unique.toString().strip();
	}

	private static List<MailAttachment> attachments(List<Map<?, ?>> leaves) {
		List<MailAttachment> attachments = new ArrayList<>();
		for (Map<?, ?> part : leaves) {
			if (!isAttachment(part)) {
				continue;
			}
			Map<?, ?> body = part.get("body") instanceof Map<?, ?> map ? map : Map.of();
			Object size = body.get("size");
			String disposition = header(part, "Content-Disposition");
			String kind = disposition == null ? "" : disposition.trim().toLowerCase(Locale.ROOT);
			// a part with a content id is shown in the body unless it says it is attached
			boolean inline = kind.startsWith("inline")
					|| (header(part, "Content-ID") != null && !kind.startsWith("attachment"));
			attachments.add(
					new MailAttachment(partId(part), stringOf(part.get("filename")), stringOf(part.get("mimeType")),
							size instanceof Number ? ((Number) size).longValue() : null, inline, MailAttachment.FILE));
		}
		return attachments;
	}

	private static String nameOrDefault(MailAttachment attachment) {
		return attachment.name() == null ? "Attachment" : attachment.name();
	}

	private static String header(Map<?, ?> part, String name) {
		if (!(part.get("headers") instanceof List<?> headers)) {
			return null;
		}
		for (Object entry : headers) {
			if (entry instanceof Map<?, ?> header && name.equalsIgnoreCase(String.valueOf(header.get("name")))) {
				Object value = header.get("value");
				return value == null ? null : value.toString();
			}
		}
		return null;
	}

	private static InternetAddress firstAddress(String header) {
		if (header == null || header.isBlank()) {
			return null;
		}
		try {
			InternetAddress[] parsed = InternetAddress.parseHeader(header, false);
			return parsed.length == 0 ? null : parsed[0];
		} catch (AddressException e) {
			return null;
		}
	}

	/**
	 * @param message a message as Gmail returned it
	 * @return the labels it carries
	 */
	public static List<String> labelIds(Map<String, Object> message) {
		if (!(message.get("labelIds") instanceof List<?> labels)) {
			return List.of();
		}
		return labels.stream().map(label -> String.valueOf(label)).toList();
	}

	private static Instant parseDate(String header) {
		if (header == null || header.isBlank()) {
			return null;
		}
		try {
			return new MailDateFormat().parse(header.trim()).toInstant();
		} catch (ParseException e) {
			return null;
		}
	}

	private static Instant internalDate(Object value) {
		if (value == null) {
			return null;
		}
		try {
			return Instant.ofEpochMilli(Long.parseLong(value.toString().trim()));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String systemName(String id) {
		if (id == null) {
			return null;
		}
		switch (id) {
		case "DRAFT":
			return "Drafts";
		default:
			return id.charAt(0) + id.substring(1).toLowerCase(Locale.ROOT);
		}
	}

	private static String blankToNull(String value) {
		return value == null || value.isBlank() ? null : value.trim();
	}

	private static String stringOf(Object value) {
		return value == null ? null : value.toString();
	}
}
