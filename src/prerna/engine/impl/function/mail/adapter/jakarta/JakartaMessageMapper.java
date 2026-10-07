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
package prerna.engine.impl.function.mail.adapter.jakarta;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;

import jakarta.mail.Address;
import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Part;
import jakarta.mail.internet.MimeUtility;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.om.Insight;

/**
 * Turns a MIME message into the map a caller of these engines receives.
 *
 * <p>
 * A message as jakarta.mail hands it over is a tree of parts, in whatever
 * arrangement the sending client chose, with the body possibly in two forms and
 * attachments announced in more than one way. Flattening that into a few fields
 * and a list of files is most of what this does.
 *
 * <p>
 * The map is the same shape the Graph reader produces, so what an engine
 * returns does not depend on how the mailbox was reached. This knows nothing
 * about engines, which is why the uid is passed in as a function - it is the
 * one field only the protocol can supply.
 */
public final class JakartaMessageMapper {

	private static final Logger classLogger = LogManager.getLogger(JakartaMessageMapper.class);
	/** What a body that was cut short ends with, so a reader can tell. */
	private static final String TRUNCATION_MARKER = " ... [truncated]";

	/** How a message is named in a way that will still find it later. */
	@FunctionalInterface
	public interface UidResolver {

		/**
		 * @param folder  the folder the message is in
		 * @param message the message to name
		 * @return the id, or null when the protocol has none
		 * @throws MessagingException when it cannot be read
		 */
		Object resolve(Folder folder, Message message) throws MessagingException;
	}

	private final MailReadPolicy policy;
	private final AttachmentStore attachmentStore;

	/**
	 * @param policy          how much of a message may be returned
	 * @param attachmentStore where an attachment is written when one is asked for
	 */
	public JakartaMessageMapper(MailReadPolicy policy, AttachmentStore attachmentStore) {
		this.policy = policy;
		this.attachmentStore = attachmentStore;
	}

	/**
	 * Describe one message.
	 *
	 * <p>
	 * Plain text is preferred over markup where a message carries both, since the
	 * markup is noise to whoever asked what the message says, and html is reduced
	 * to its text when that is all there is.
	 *
	 * @param folder              the folder the message is in
	 * @param message             the message to describe
	 * @param supportsFlags       whether the protocol knows if it has been read,
	 *                            which decides whether that is reported at all
	 *                            rather than reported wrongly
	 * @param uidResolver         how to name the message
	 * @param includeBody         whether the body text comes back
	 * @param downloadAttachments whether attachments are written into the insight
	 * @param insight             the insight this call is running under, or null
	 * @return the message as a map
	 * @throws MessagingException when the message cannot be read
	 * @throws IOException        when one of its parts cannot be read
	 */
	public Map<String, Object> toMap(Folder folder, Message message, boolean supportsFlags, UidResolver uidResolver,
			boolean includeBody, boolean downloadAttachments, Insight insight) throws MessagingException, IOException {
		Map<String, Object> output = new LinkedHashMap<>();
		putIfPresent(output, "uid", uidResolver.resolve(folder, message));
		putIfPresent(output, "messageId", firstHeader(message, "Message-ID"));
		putIfPresent(output, "from", joinAddresses(message.getFrom()));
		putIfPresent(output, "to", joinAddresses(message.getRecipients(Message.RecipientType.TO)));
		putIfPresent(output, "cc", joinAddresses(message.getRecipients(Message.RecipientType.CC)));
		putIfPresent(output, "subject", message.getSubject());
		putIfPresent(output, "sentDate", formatDate(message.getSentDate()));
		putIfPresent(output, "receivedDate", formatDate(message.getReceivedDate()));
		if (supportsFlags) {
			output.put("unread", !message.isSet(Flags.Flag.SEEN));
		}

		StringBuilder plainBody = new StringBuilder();
		StringBuilder htmlBody = new StringBuilder();
		List<Part> attachments = new ArrayList<>();
		collectParts(message, plainBody, htmlBody, attachments);

		if (includeBody) {
			String body = plainBody.length() > 0 ? plainBody.toString() : Jsoup.parse(htmlBody.toString()).text();
			body = body.trim();
			if (body.length() > this.policy.maxBodyChars()) {
				body = body.substring(0, this.policy.maxBodyChars()) + TRUNCATION_MARKER;
				output.put("bodyTruncated", true);
			}
			output.put("body", body);
		}

		if (!attachments.isEmpty()) {
			List<Map<String, Object>> described = new ArrayList<>();
			for (Part attachment : attachments) {
				described.add(describeAttachment(attachment, downloadAttachments, insight));
			}
			output.put("attachments", described);
		}
		return output;
	}

	/**
	 * Walk the parts of a message, sorting them into body text and attachments.
	 *
	 * <p>
	 * A part counts as an attachment if it says so, or if it has a file name and is
	 * not text. The second test is there because plenty of clients send an
	 * attachment without the disposition that would announce it, and a file that
	 * silently vanished from the listing would be worse than one described
	 * generously.
	 *
	 * @param part        the part to walk, which may be the message itself
	 * @param plainBody   collects the plain text
	 * @param htmlBody    collects the markup
	 * @param attachments collects the parts that are files
	 * @throws MessagingException when a part cannot be read
	 * @throws IOException        when its content cannot be read
	 */
	private void collectParts(Part part, StringBuilder plainBody, StringBuilder htmlBody, List<Part> attachments)
			throws MessagingException, IOException {
		if (part.isMimeType("multipart/*") && part.getContent() instanceof Multipart multipart) {
			for (int i = 0; i < multipart.getCount(); i++) {
				collectParts(multipart.getBodyPart(i), plainBody, htmlBody, attachments);
			}
			return;
		}

		String disposition = part.getDisposition();
		if (Part.ATTACHMENT.equalsIgnoreCase(disposition)
				|| (part.getFileName() != null && !part.isMimeType("text/*"))) {
			attachments.add(part);
			return;
		}
		if (part.isMimeType("text/plain")) {
			plainBody.append(partAsString(part));
		} else if (part.isMimeType("text/html")) {
			htmlBody.append(partAsString(part));
		} else if (part.getFileName() != null) {
			attachments.add(part);
		}
	}

	/**
	 * Read a text part, however jakarta.mail chose to hand it over.
	 *
	 * @param part the part to read
	 * @return its text
	 * @throws MessagingException when the part cannot be read
	 * @throws IOException        when its content cannot be read
	 */
	private static String partAsString(Part part) throws MessagingException, IOException {
		Object content = part.getContent();
		if (content instanceof String string) {
			return string;
		}
		if (content instanceof InputStream stream) {
			try (stream) {
				return IOUtils.toString(stream, StandardCharsets.UTF_8);
			}
		}
		return content == null ? "" : content.toString();
	}

	/**
	 * Describe one attachment, and save it when the caller asked for that.
	 *
	 * <p>
	 * Only the name it was saved under is reported, not the resolved path, so the
	 * caller is not handed the server side layout of the insight folder.
	 *
	 * @param part     the attachment
	 * @param download whether it is written into the insight
	 * @param insight  the insight this call is running under, or null
	 * @return the attachment as a map
	 * @throws MessagingException when the part cannot be read
	 * @throws IOException        when it cannot be written
	 */
	private Map<String, Object> describeAttachment(Part part, boolean download, Insight insight)
			throws MessagingException, IOException {
		String fileName = attachmentFileName(part);
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("name", fileName);
		if (part.getSize() > 0) {
			output.put("size", part.getSize());
		}
		putIfPresent(output, "contentType", MailProperties.trimToNull(part.getContentType()));

		if (download && insight == null) {
			classLogger.warn("Attachments can only be downloaded from within an insight that holds the files");
		} else if (download) {
			File saved = this.attachmentStore.save(fileName, part.getInputStream(), insight);
			if (saved != null) {
				output.put("savedAs", saved.getName());
			}
		}
		return output;
	}

	/**
	 * The name to give an attachment.
	 *
	 * <p>
	 * A name with non ascii characters arrives MIME encoded, so it is decoded first
	 * and then sanitized. A name that will not decode is sanitized as it stands
	 * rather than losing the attachment over it.
	 *
	 * @param part the attachment
	 * @return a file name that is safe to write
	 * @throws MessagingException when the part cannot be read
	 */
	private static String attachmentFileName(Part part) throws MessagingException {
		String rawName = part.getFileName();
		if (rawName != null) {
			try {
				rawName = MimeUtility.decodeText(rawName);
			} catch (UnsupportedEncodingException e) {
				classLogger.warn("Could not decode attachment name {}", rawName, e);
			}
		}
		return AttachmentStore.sanitizeName(rawName);
	}

	/**
	 * @param message    the message to read
	 * @param headerName the header to look for
	 * @return the first value of it, or null when the message does not carry it
	 * @throws MessagingException when the headers cannot be read
	 */
	public static String firstHeader(Message message, String headerName) throws MessagingException {
		String[] values = message.getHeader(headerName);
		return values == null || values.length == 0 ? null : MailProperties.trimToNull(values[0]);
	}

	/**
	 * @param addresses one set of addresses off a message
	 * @return them joined, or null when there are none
	 */
	public static String joinAddresses(Address[] addresses) {
		if (addresses == null || addresses.length == 0) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (Address address : addresses) {
			if (address != null) {
				values.add(address.toString());
			}
		}
		return values.isEmpty() ? null : String.join(", ", values);
	}

	/**
	 * @param date a date off a message, which may be null
	 * @return it written the way Graph writes its dates, so both mailboxes answer
	 *         in one format
	 */
	public static String formatDate(Date date) {
		return date == null ? null : DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(date.getTime()));
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
}
