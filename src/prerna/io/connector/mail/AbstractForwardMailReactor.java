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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Forwards a message, attachments and all, with the message quoted underneath.
 */
public abstract class AbstractForwardMailReactor extends AbstractDraftDeliveryReactor {

	private static final String[] KEYS = { ID, TO, BODY, AS_DRAFT, HTML, ATTACHMENTS };

	/**
	 * A forward the caller wrote.
	 *
	 * @param id          the message being forwarded
	 * @param to          who it goes to
	 * @param body        an optional note above the forwarded message
	 * @param html        whether the note is html rather than plain text
	 * @param attachments files to attach alongside the ones the message carries
	 */
	public record ForwardMailRequest(String id, List<String> to, String body, boolean html, List<File> attachments) {

		public ForwardMailRequest {
			to = List.copyOf(to);
			attachments = attachments == null ? List.of() : List.copyOf(attachments);
		}
	}

	protected AbstractForwardMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID, TO);
	}

	/**
	 * Write the forward as a draft, carrying the original's attachments, without
	 * sending it.
	 *
	 * @param user    the signed in user
	 * @param request the forward
	 * @return the draft as saved, with its recipients
	 * @throws Exception when the draft cannot be written
	 */
	protected abstract ComposedMail draftForward(User user, ForwardMailRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("forward the message", () -> {
			String id = requireId("forward a message");
			List<String> to = MailRecipientRules.validate(readValues(TO));
			if (to.isEmpty()) {
				throw new IllegalArgumentException(
						"At least one recipient in " + TO + " is required to forward a message.");
			}
			ForwardMailRequest request = new ForwardMailRequest(id, to, this.keyValue.get(BODY),
					readBoolean(HTML, false), readInsightFiles(ATTACHMENTS));
			ComposedMail draft = draftForward(this.insight.getUser(), request);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("forwarded", id);
			output.putAll(finish(draft, !readBoolean(AS_DRAFT, false)));
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "mail/compose?intent=forward";
	}

	@Override
	protected final String describe() {
		return "Forward a message from the signed in user's own " + mailbox()
				+ ", attachments and all, with the message quoted underneath.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case TO:
			return "Who to forward the message to, as a list of email addresses.";
		case BODY:
			return "Optional note added above the forwarded message.";
		case ATTACHMENTS:
			return "Optional files to attach alongside the ones the message already carries, as a list of paths "
					+ "relative to the insight folder.";
		default:
			return super.describeKey(key);
		}
	}
}
