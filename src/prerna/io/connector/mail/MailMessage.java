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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;
import prerna.io.connector.ConnectorTimes;

/**
 * One message, the same whichever mailbox it was read from.
 *
 * <p>
 * Bodies are held whole and cut short only when written out, since how much of
 * one a caller wants is the caller's to say. Times are written in UTC.
 * </p>
 *
 * @param id                the provider's id for the message, which every other
 *                          mail reactor takes
 * @param internetMessageId the {@code Message-ID} header
 * @param conversationId    what ties the messages of one thread together
 * @param from              the sender's address
 * @param fromName          the sender's display name
 * @param to                the recipients
 * @param cc                the copied recipients
 * @param subject           the subject line
 * @param sentDate          when it was sent
 * @param receivedDate      when it arrived
 * @param unread            whether nobody has opened it
 * @param hasAttachments    whether anything is attached
 * @param body              the readable text of the message
 * @param uniqueBody        the text of this message alone, without the earlier
 *                          messages it quotes, when a thread was read
 * @param webLink           where the message opens in the provider's own app
 * @param attachments       what is attached, when it was asked for
 * @param displayBody       the body as the client renders it, when it was asked
 *                          for
 * @param replyRecipients   who a reply to everybody would go to, when it was
 *                          asked for
 */
public record MailMessage(String id, String internetMessageId, String conversationId, String from, String fromName,
		List<String> to, List<String> cc, String subject, Instant sentDate, Instant receivedDate, boolean unread,
		boolean hasAttachments, String body, String uniqueBody, String webLink, List<MailAttachment> attachments,
		Map<String, Object> displayBody, MailRecipients replyRecipients) {

	public MailMessage {
		to = to == null ? List.of() : List.copyOf(to);
		cc = cc == null ? List.of() : List.copyOf(cc);
		attachments = attachments == null ? null : List.copyOf(attachments);
	}

	/**
	 * @param includeBody  whether the body comes back
	 * @param maxBodyChars the longest body to return before cutting it short, or 0
	 *                     to return whatever length it is
	 * @return the message as a reactor answers with it
	 */
	public Map<String, Object> toMap(boolean includeBody, int maxBodyChars) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "internetMessageId", this.internetMessageId);
		ConnectorOutput.putIfPresent(output, "conversationId", this.conversationId);
		ConnectorOutput.putIfPresent(output, "from", this.from);
		ConnectorOutput.putIfPresent(output, "fromName", this.fromName);
		output.put("to", this.to);
		output.put("cc", this.cc);
		ConnectorOutput.putIfPresent(output, "subject", this.subject);
		ConnectorOutput.putIfPresent(output, "sentDate", ConnectorTimes.format(this.sentDate));
		ConnectorOutput.putIfPresent(output, "receivedDate", ConnectorTimes.format(this.receivedDate));
		output.put("unread", this.unread);
		output.put("hasAttachments", this.hasAttachments);
		ConnectorOutput.putIfPresent(output, "webLink", this.webLink);
		if (includeBody) {
			ConnectorOutput.putText(output, "body", this.body, maxBodyChars);
			if (this.uniqueBody != null) {
				ConnectorOutput.putText(output, "uniqueBody", this.uniqueBody, maxBodyChars);
			}
		}
		if (this.attachments != null) {
			output.put("attachments", this.attachments.stream().map(MailAttachment::toMap).toList());
		}
		ConnectorOutput.putIfPresent(output, "displayBody", this.displayBody);
		if (this.replyRecipients != null) {
			output.put("replyRecipients", this.replyRecipients.toMap());
		}
		return output;
	}
}
