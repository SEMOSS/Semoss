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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailFolder;
import prerna.io.connector.mail.MailMessage;

class GoogleGmailMessageMapperUnitTests {

	private static final String ACCOUNT = "me@example.com";

	@Test
	void readsAMessageIntoTheSharedShape() {
		MailMessage message = GoogleGmailMessageMapper.toMailMessage(message(), ACCOUNT, true, false, true, true);

		assertEquals("m1", message.id());
		assertEquals("t1", message.conversationId());
		assertEquals("<abc@mail.example.com>", message.internetMessageId());
		assertEquals("ada@example.com", message.from());
		assertEquals("Ada Lovelace", message.fromName());
		assertEquals(List.of("me@example.com", "bob@example.com"), message.to());
		assertEquals(List.of("carol@example.com"), message.cc());
		assertEquals(Instant.parse("2026-10-05T14:00:00Z"), message.sentDate());
		assertTrue(message.unread());
		assertTrue(message.hasAttachments());
		assertTrue(message.body().startsWith("Hello"));
		assertTrue(message.uniqueBody().contains("Hello"));
		assertFalse(message.uniqueBody().contains("earlier"));
		assertTrue(message.webLink().endsWith("#all/t1"));

		// the part's own id, since Gmail hands out a new attachment id on every read
		MailAttachment attachment = message.attachments().get(0);
		assertEquals("1", attachment.id());
		assertEquals("plan.pdf", attachment.name());
		assertEquals(1234L, attachment.size());
		assertEquals(MailAttachment.FILE, attachment.kind());
		assertFalse(attachment.isInline());

		// the user is left off a reply to everybody
		assertEquals(List.of("ada@example.com", "bob@example.com"), message.replyRecipients().to());
		assertEquals(List.of("carol@example.com"), message.replyRecipients().cc());

		Map<String, Object> output = message.toMap(true, 0);
		assertEquals("2026-10-05T14:00:00Z", output.get("sentDate"));
		assertEquals(List.of("me@example.com", "bob@example.com"), output.get("to"));
	}

	@Test
	void aGroupAddressStandsForItsMembers() {
		assertEquals(List.of(), GoogleGmailMessageMapper.addresses("undisclosed-recipients:;"));
		assertEquals(List.of("a@example.com", "b@example.com"),
				GoogleGmailMessageMapper.addresses("team: a@example.com, b@example.com;"));
	}

	@Test
	void anUnreadLabelIsWhatMakesAMessageUnread() {
		Map<String, Object> read = new java.util.LinkedHashMap<>(message());
		read.put("labelIds", List.of("INBOX"));
		assertFalse(GoogleGmailMessageMapper.toMailMessage(read, ACCOUNT, false, false, false, false).unread());
	}

	@Test
	void labelsGmailKeepsReadAsFoldersByName() {
		assertEquals("Inbox", GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "INBOX", "name", "INBOX", "type", "system"), null, null, 0).name());
		assertEquals("Drafts", GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "DRAFT", "name", "DRAFT", "type", "system"), null, null, 0).name());
		assertFalse(GoogleGmailMessageMapper.isFiled(Map.of("id", "UNREAD", "type", "system")));
		assertTrue(GoogleGmailMessageMapper.isFiled(Map.of("id", "Label_1", "type", "user")));
	}

	@Test
	void onlyTheInboxGoesByTheWellKnownName() {
		assertEquals(MailFolder.INBOX, GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "INBOX", "name", "INBOX", "type", "system"), null, null, 0).wellKnownName());
		assertNull(GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "DRAFT", "name", "DRAFT", "type", "system"), null, null, 0).wellKnownName());
		assertNull(GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "Label_1", "name", "INBOX", "type", "user"), null, null, 0).wellKnownName());
		assertEquals("inbox",
				GoogleGmailMessageMapper
						.toMailFolder(Map.of("id", "INBOX", "name", "INBOX", "type", "system"), null, null, 0).toMap()
						.get("wellKnownName"));
		assertFalse(GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "Label_1", "name", "Clients", "type", "user"), null, null, 0).toMap()
				.containsKey("wellKnownName"));
	}

	@Test
	void nestedLabelIsNamedAfterTheLabelItSitsUnder() {
		MailFolder nested = GoogleGmailMessageMapper.toMailFolder(
				Map.of("id", "Label_2", "name", "Projects/Alpha/Specs", "type", "user"), "Label_1", "Projects", 1);
		assertEquals("Alpha/Specs", nested.name());
		assertEquals("Projects/Alpha/Specs", nested.path());
		assertEquals("Label_1", nested.parentId());
		assertEquals(1, nested.depth());

		MailFolder top = GoogleGmailMessageMapper
				.toMailFolder(Map.of("id", "Label_1", "name", "Projects", "type", "user"), null, null, 0);
		assertEquals("Projects", top.name());
		assertEquals("Projects", top.path());
		assertNull(top.parentId());
	}

	private static Map<String, Object> message() {
		return Map.of("id", "m1", "threadId", "t1", "labelIds", List.of("INBOX", "UNREAD"), "internalDate",
				"1790000000000", "payload",
				Map.of("mimeType", "multipart/mixed", "headers", List.of(
						header("From", "Ada Lovelace <ada@example.com>"),
						header("To", "me@example.com, bob@example.com"), header("Cc", "carol@example.com"),
						header("Subject", "Plans"), header("Date",
								"Mon, 5 Oct 2026 10:00:00 -0400"),
						header("Message-ID", "<abc@mail.example.com>")), "parts",
						List.of(Map.of("mimeType", "multipart/alternative", "parts", List.of(
								Map.of("mimeType", "text/plain", "body",
										Map.of("data",
												encode("Hello\n\nOn Sun, Oct 4, 2026 at 9:00 AM Bob "
														+ "<bob@example.com> wrote:\n> earlier"))),
								Map.of("mimeType", "text/html", "body",
										Map.of("data",
												encode("<div>Hello</div><div class=\"gmail_quote\">earlier</div>"))))),
								Map.of("partId", "1", "mimeType", "application/pdf", "filename", "plan.pdf", "headers",
										List.of(header("Content-Disposition", "attachment; filename=\"plan.pdf\"")),
										"body", Map.of("attachmentId", "att1", "size", 1234L)))));
	}

	@Test
	void findsThePartsTheBodyCanShowByContentIdWithOrWithoutAFileName() {
		Map<String, Object> message = Map.of("id", "m2", "payload",
				Map.of("mimeType", "multipart/related", "parts",
						List.of(Map.of("mimeType", "text/html", "body",
								Map.of("data", encode("<img src=\"cid:ii_logo\"><img src=\"cid:sig\">"))),
								Map.of("partId", "1", "mimeType", "image/png", "headers",
										List.of(header("Content-ID", "<ii_logo>")), "body",
										Map.of("attachmentId", "att-logo", "size", 10L)),
								Map.of("partId", "2", "mimeType", "image/jpeg", "filename", "sig.jpg", "headers",
										List.of(header("Content-ID", "<sig>"),
												header("Content-Disposition", "inline; filename=\"sig.jpg\"")),
										"body", Map.of("data", encode("jpeg"))),
								Map.of("partId", "3", "mimeType", "application/pdf", "filename", "plan.pdf", "body",
										Map.of("attachmentId", "att-plan")))));
		List<Map<?, ?>> parts = GoogleGmailMessageMapper.contentIdParts(message);
		assertEquals(List.of("1", "2"), parts.stream().map(part -> part.get("partId")).toList());
		assertEquals("<ii_logo>", GoogleGmailMessageMapper.contentId(parts.get(0)));
		assertNull(GoogleGmailMessageMapper.inlineBytes(parts.get(0)));
		assertEquals("att-logo", GoogleGmailMessageMapper.attachmentId(parts.get(0)));
		assertEquals("jpeg", new String(GoogleGmailMessageMapper.inlineBytes(parts.get(1)), StandardCharsets.UTF_8));
	}

	private static Map<String, Object> header(String name, String value) {
		return Map.of("name", name, "value", value);
	}

	private static String encode(String text) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
	}
}
