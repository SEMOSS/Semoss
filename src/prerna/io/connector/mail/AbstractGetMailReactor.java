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

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads one message in full, with what is attached to it.
 *
 * <p>
 * Reading a message does not mark it read, for any mailbox, so an agent looking
 * at the user's mail leaves it the way the user left it. {@code MarkMailRead}
 * is what changes that.
 * </p>
 */
public abstract class AbstractGetMailReactor extends AbstractMailReactor {

	private static final String[] KEYS = { ID, MAX_BODY_CHARS, INCLUDE_ATTACHMENTS, INCLUDE_DISPLAY_BODY,
			INCLUDE_REPLY_RECIPIENTS };

	/**
	 * What to read about one message.
	 *
	 * @param id                     the message
	 * @param includeAttachments     whether what is attached is listed
	 * @param includeDisplayBody     whether the body for showing to a person comes
	 *                               back
	 * @param includeReplyRecipients whether who a reply to everybody would go to
	 *                               comes back
	 */
	public record GetMailRequest(String id, boolean includeAttachments, boolean includeDisplayBody,
			boolean includeReplyRecipients) {
	}

	protected AbstractGetMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Read one message without marking it read.
	 *
	 * @param user    the signed in user
	 * @param request the message and what to read about it
	 * @return the message
	 * @throws Exception when the message cannot be read
	 */
	protected abstract MailMessage getMail(User user, GetMailRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read the message", () -> {
			GetMailRequest request = new GetMailRequest(requireId("read a message"),
					readBoolean(INCLUDE_ATTACHMENTS, true), readBoolean(INCLUDE_DISPLAY_BODY, false),
					readBoolean(INCLUDE_REPLY_RECIPIENTS, false));
			int maxBodyChars = readCount(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);
			return getMail(this.insight.getUser(), request).toMap(true, maxBodyChars);
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "mail/message";
	}

	@Override
	protected final String describe() {
		return "Read one message in full from the signed in user's own " + mailbox()
				+ ", with what is attached to it. Reading it does not mark it read.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case INCLUDE_ATTACHMENTS:
			return "Optional boolean for whether what is attached to the message is listed by name and id. Defaults to true.";
		case INCLUDE_DISPLAY_BODY:
			return "Optional boolean to also return displayBody, the original body for showing to a person, with its "
					+ "content type. Defaults to false.";
		case INCLUDE_REPLY_RECIPIENTS:
			return "Optional boolean to also return replyRecipients, who a reply to everybody would go to, without "
					+ "the user's own address. Defaults to false.";
		default:
			return super.describeKey(key);
		}
	}
}
