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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;
import org.jsoup.safety.Safelist;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Part;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.InternetHeaders;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.OutgoingMail;

/**
 * Writes the messages Gmail sends: the MIME message itself, encoded the way the
 * Gmail API takes it, and the quoted original a reply or a forward carries
 * under what the user wrote.
 *
 * <p>
 * Gmail writes neither the quote nor the reply headers itself, the way Outlook
 * does, so they are written here the way Gmail's own app writes them, which is
 * what keeps a reply in its thread for everybody on it.
 * </p>
 */
final class GoogleGmailMime {

	/**
	 * What may be in html the user wrote above a quote, the same as Outlook allows.
	 */
	private static final Safelist AUTHORED_HTML = Safelist.relaxed().addTags("span", "h1", "h2", "h3", "s")
			.addAttributes(":all", "style").addAttributes("td", "colspan", "rowspan")
			.addAttributes("th", "colspan", "rowspan", "scope");

	private GoogleGmailMime() {

	}

	/**
	 * A file carried over from another message, such as the attachments of one
	 * being forwarded.
	 *
	 * @param name        the file name
	 * @param contentType the media type
	 * @param bytes       the file
	 */
	record CarriedFile(String name, String contentType, byte[] bytes) {

	}

	/**
	 * Write a message.
	 *
	 * @param from       the user's address, or null to let Gmail fill it in
	 * @param to         the recipients
	 * @param cc         the copied recipients
	 * @param bcc        the blind copied recipients
	 * @param subject    the subject line
	 * @param body       the body
	 * @param html       whether the body is html
	 * @param files      files to attach from the insight folder
	 * @param carried    files to attach from another message
	 * @param inReplyTo  the Message-ID being answered, or null
	 * @param references the thread's References, or null
	 * @return the message, base64url encoded
	 * @throws MessagingException when the message cannot be written
	 * @throws IOException        when a file cannot be read
	 */
	static String write(String from, List<String> to, List<String> cc, List<String> bcc, String subject, String body,
			boolean html, List<File> files, List<CarriedFile> carried, String inReplyTo, String references)
			throws MessagingException, IOException {
		MimeMessage message = new MimeMessage(Session.getInstance(new Properties()));
		if (from != null && !from.isBlank()) {
			message.setFrom(new InternetAddress(from));
		}
		setRecipients(message, Message.RecipientType.TO, to);
		setRecipients(message, Message.RecipientType.CC, cc);
		setRecipients(message, Message.RecipientType.BCC, bcc);
		message.setSubject(subject == null ? "" : subject, "UTF-8");
		if (inReplyTo != null && !inReplyTo.isBlank()) {
			message.setHeader("In-Reply-To", inReplyTo);
			message.setHeader("References", references == null || references.isBlank() ? inReplyTo : references);
		}

		String text = body == null ? "" : body;
		String subtype = html ? "html" : "plain";
		boolean hasFiles = (files != null && !files.isEmpty()) || (carried != null && !carried.isEmpty());
		if (!hasFiles) {
			message.setText(text, "UTF-8", subtype);
		} else {
			MimeMultipart mixed = new MimeMultipart("mixed");
			MimeBodyPart content = new MimeBodyPart();
			content.setText(text, "UTF-8", subtype);
			mixed.addBodyPart(content);
			if (files != null) {
				for (File file : files) {
					MimeBodyPart part = new MimeBodyPart();
					part.attachFile(file);
					part.setFileName(OutgoingMail.attachmentName(file));
					mixed.addBodyPart(part);
				}
			}
			if (carried != null) {
				for (CarriedFile file : carried) {
					InternetHeaders headers = new InternetHeaders();
					headers.addHeader("Content-Type",
							file.contentType() == null ? "application/octet-stream" : file.contentType());
					headers.addHeader("Content-Transfer-Encoding", "base64");
					MimeBodyPart part = new MimeBodyPart(headers, Base64.getMimeEncoder().encode(file.bytes()));
					part.setFileName(file.name());
					part.setDisposition(Part.ATTACHMENT);
					mixed.addBodyPart(part);
				}
			}
			message.setContent(mixed);
		}
		message.saveChanges();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		message.writeTo(out);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(out.toByteArray());
	}

	/**
	 * @param subject the subject being answered
	 * @return the subject of the reply, which does not gain a second Re:
	 */
	static String replySubject(String subject) {
		String original = subject == null ? "" : subject.trim();
		return original.toLowerCase(Locale.ROOT).startsWith("re:") ? original : "Re: " + original;
	}

	/**
	 * @param subject the subject being forwarded
	 * @return the subject of the forward, which does not gain a second Fwd:
	 */
	static String forwardSubject(String subject) {
		String original = subject == null ? "" : subject.trim();
		String lower = original.toLowerCase(Locale.ROOT);
		return lower.startsWith("fwd:") || lower.startsWith("fw:") ? original : "Fwd: " + original;
	}

	/**
	 * What the user wrote, with the message being answered quoted underneath.
	 *
	 * @param body         what the user wrote
	 * @param html         whether that is html
	 * @param original     the message being answered
	 * @param originalHtml its html body, or null when it has none
	 * @return the reply's body, html when the user wrote html
	 */
	static String quoteReply(String body, boolean html, MailMessage original, String originalHtml) {
		String attribution = "On " + dateOf(original) + ", " + senderOf(original) + " wrote:";
		if (!html) {
			StringBuilder quoted = new StringBuilder(body == null ? "" : body).append("\n\n").append(attribution)
					.append('\n');
			for (String line : (original.body() == null ? "" : original.body()).split("\n", -1)) {
				quoted.append("> ").append(line).append('\n');
			}
			return quoted.toString();
		}
		return cleanAuthored(body) + "<br><div class=\"gmail_quote\"><div>" + Entities.escape(attribution)
				+ "</div><blockquote class=\"gmail_quote\" style=\"margin:0 0 0 .8ex;border-left:1px #ccc solid;"
				+ "padding-left:1ex\">" + originalAsHtml(original, originalHtml) + "</blockquote></div>";
	}

	/**
	 * What the user wrote, with the message being forwarded underneath.
	 *
	 * @param body         what the user wrote, or null
	 * @param html         whether that is html
	 * @param original     the message being forwarded
	 * @param originalHtml its html body, or null when it has none
	 * @return the forward's body, html when the user wrote html
	 */
	static String quoteForward(String body, boolean html, MailMessage original, String originalHtml) {
		String[] lines = { "---------- Forwarded message ---------", "From: " + senderOf(original),
				"Date: " + dateOf(original), "Subject: " + (original.subject() == null ? "" : original.subject()),
				"To: " + String.join(", ", original.to()) };
		if (!html) {
			return (body == null ? "" : body) + "\n\n" + String.join("\n", lines) + "\n\n"
					+ (original.body() == null ? "" : original.body());
		}
		StringBuilder header = new StringBuilder();
		for (String line : lines) {
			header.append(Entities.escape(line)).append("<br>");
		}
		return cleanAuthored(body) + "<br><div class=\"gmail_quote\"><div>" + header + "</div><br>"
				+ originalAsHtml(original, originalHtml) + "</div>";
	}

	private static String originalAsHtml(MailMessage original, String originalHtml) {
		if (originalHtml != null) {
			Document document = Jsoup.parse(originalHtml);
			return document.body().html();
		}
		return Entities.escape(original.body() == null ? "" : original.body()).replace("\n", "<br>");
	}

	private static String cleanAuthored(String html) {
		return Jsoup.clean(html == null ? "" : html, "", AUTHORED_HTML,
				new Document.OutputSettings().prettyPrint(false));
	}

	private static String senderOf(MailMessage message) {
		if (message.fromName() != null && message.from() != null) {
			return message.fromName() + " <" + message.from() + ">";
		}
		return message.from() == null ? "" : message.from();
	}

	private static String dateOf(MailMessage message) {
		String sent = ConnectorTimes.format(message.sentDate() != null ? message.sentDate() : message.receivedDate());
		return sent == null ? "" : sent;
	}

	private static void setRecipients(MimeMessage message, Message.RecipientType type, List<String> addresses)
			throws MessagingException {
		if (addresses == null || addresses.isEmpty()) {
			return;
		}
		InternetAddress[] parsed = new InternetAddress[addresses.size()];
		for (int i = 0; i < parsed.length; i++) {
			parsed[i] = new InternetAddress(addresses.get(i));
		}
		message.setRecipients(type, parsed);
	}
}
