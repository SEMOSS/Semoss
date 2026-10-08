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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailMessage;

class MicrosoftOutlookMessageMapperUnitTests {

	@Test
	void readsAMessageIntoTheSharedShape() {
		MailMessage message = MicrosoftOutlookMessageMapper.toMailMessage(Map.ofEntries(Map.entry("id", "AAMk1"),
				Map.entry("internetMessageId", "<abc@mail.example.com>"), Map.entry("conversationId", "c1"),
				Map.entry("from", Map.of("emailAddress", Map.of("address", "ada@example.com", "name", "Ada Lovelace"))),
				Map.entry("toRecipients",
						List.of(Map.of("emailAddress", Map.of("address", "me@example.com")),
								Map.of("emailAddress", Map.of("address", "bob@example.com")))),
				Map.entry("subject", "Plans"), Map.entry("sentDateTime", "2026-10-05T14:00:00Z"),
				Map.entry("receivedDateTime", "2026-10-05T14:00:05Z"), Map.entry("isRead", false),
				Map.entry("hasAttachments", true),
				Map.entry("body", Map.of("contentType", "html", "content", "<p>Hi</p>")),
				Map.entry("webLink", "https://outlook.office.com/m")), null, null, null);

		Map<String, Object> output = message.toMap(true, 0);
		assertEquals("AAMk1", output.get("id"));
		assertEquals("<abc@mail.example.com>", output.get("internetMessageId"));
		assertEquals(List.of("me@example.com", "bob@example.com"), output.get("to"));
		assertEquals(List.of(), output.get("cc"));
		assertEquals("2026-10-05T14:00:00Z", output.get("sentDate"));
		assertEquals(true, output.get("unread"));
		assertEquals("Hi", output.get("body"));
		assertTrue(!output.containsKey("uid") && !output.containsKey("messageId"));
	}

	@Test
	void anAttachmentSaysWhatKindItIs() {
		assertEquals(MailAttachment.ITEM, MicrosoftOutlookMessageMapper
				.toMailAttachment(Map.of("id", "a1", "@odata.type", "#microsoft.graph.itemAttachment")).kind());
		assertEquals(MailAttachment.LINK, MicrosoftOutlookMessageMapper
				.toMailAttachment(Map.of("id", "a2", "@odata.type", "#microsoft.graph.referenceAttachment")).kind());
		assertEquals(MailAttachment.FILE,
				MicrosoftOutlookMessageMapper
						.toMailAttachment(
								Map.of("id", "a3", "@odata.type", "#microsoft.graph.fileAttachment", "size", 12.0))
						.kind());
	}

	@Test
	void aLongBodyIsCutShortWhenWrittenOut() {
		MailMessage message = MicrosoftOutlookMessageMapper.toMailMessage(
				Map.of("id", "AAMk2", "body", Map.of("contentType", "text", "content", "0123456789")), null, null,
				null);
		Map<String, Object> output = message.toMap(true, 4);
		assertEquals("0123 ... [truncated]", output.get("body"));
		assertEquals(true, output.get("bodyTruncated"));
	}
}
