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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * A message as it was written to the provider: saved as a draft, or sent.
 *
 * <p>
 * This is what every reactor that writes mail answers with, so whoever asked,
 * and the conversation the request came from, can see what actually went out,
 * including anything the user changed before approving it.
 * </p>
 *
 * @param id             the provider's id for the draft or the sent message,
 *                       when it gave one
 * @param conversationId the thread the message belongs to, when the provider
 *                       said
 * @param webLink        where the message opens in the provider's own app
 * @param to             the recipients
 * @param cc             the copied recipients
 * @param bcc            the blind copied recipients
 * @param subject        the subject line
 * @param body           the body, as the caller wrote it
 * @param html           whether the body is html rather than plain text
 * @param attachments    the names of the files attached
 */
public record ComposedMail(String id, String conversationId, String webLink, List<String> to, List<String> cc,
		List<String> bcc, String subject, String body, boolean html, List<String> attachments) {

	public ComposedMail {
		to = to == null ? List.of() : List.copyOf(to);
		cc = cc == null ? List.of() : List.copyOf(cc);
		bcc = bcc == null ? List.of() : List.copyOf(bcc);
		attachments = attachments == null ? List.of() : List.copyOf(attachments);
	}

	/**
	 * @param mail           what the caller wrote
	 * @param id             the provider's id for it, or null
	 * @param conversationId its thread, or null
	 * @param webLink        where it opens, or null
	 * @return the message as written
	 */
	public static ComposedMail of(OutgoingMail mail, String id, String conversationId, String webLink) {
		return new ComposedMail(id, conversationId, webLink, mail.to(), mail.cc(), mail.bcc(), mail.subject(),
				mail.body(), mail.html(), mail.attachmentNames());
	}

	/**
	 * @param id             the new id, or null
	 * @param conversationId the thread, or null to keep this one's
	 * @param webLink        where it opens now, or null
	 * @return the same message under the ids it has now, since sending a draft
	 *         gives it new ones
	 */
	public ComposedMail withIds(String id, String conversationId, String webLink) {
		return new ComposedMail(id, conversationId == null ? this.conversationId : conversationId, webLink, this.to,
				this.cc, this.bcc, this.subject, this.body, this.html, this.attachments);
	}

	/**
	 * @return the message as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		ConnectorOutput.putIfPresent(output, "id", this.id);
		ConnectorOutput.putIfPresent(output, "conversationId", this.conversationId);
		ConnectorOutput.putIfPresent(output, "webLink", this.webLink);
		output.put("to", this.to);
		output.put("cc", this.cc);
		output.put("bcc", this.bcc);
		ConnectorOutput.putIfPresent(output, "subject", this.subject);
		output.put("body", this.body == null ? "" : this.body);
		output.put("html", this.html);
		output.put("attachments", this.attachments);
		return output;
	}
}
