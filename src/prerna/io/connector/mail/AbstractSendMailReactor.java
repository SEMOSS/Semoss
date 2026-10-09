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
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Sends a message as the signed in user.
 *
 * <p>
 * The message goes out as the signed in user because the token says who that
 * is, so this cannot be used to send as somebody else, and a copy lands in
 * their own sent mail where they can see what was sent on their behalf.
 * </p>
 */
public abstract class AbstractSendMailReactor extends AbstractComposeMailReactor {

	protected AbstractSendMailReactor(String... extraKeys) {
		super(extraKeys);
	}

	/**
	 * Send the message.
	 *
	 * @param user the signed in user
	 * @param mail what the caller wrote
	 * @return the message as sent, with whatever ids the provider gave it
	 * @throws Exception when the message cannot be sent
	 */
	protected abstract ComposedMail sendMail(User user, OutgoingMail mail) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("send the email", () -> {
			User user = this.insight.getUser();
			OutgoingMail mail = readOutgoing(true, "send an email");
			ComposedMail sent = deliver(ComposedMail.of(mail, null, null, null), () -> sendMail(user, mail));

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("sent", true);
			output.putAll(sent.toMap());
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "mail/compose?intent=send";
	}

	@Override
	protected final String describe() {
		return "Send an email as the signed in user from their own " + mailbox()
				+ ". For a message somebody should look at before it goes out, save it with " + reactorName("SaveDraft")
				+ " instead.";
	}

	@Override
	protected final String describeKey(String key) {
		if (TO.equals(key)) {
			return "Recipients of the email, as a list of email addresses. At least one of to, cc or bcc is required.";
		}
		return super.describeKey(key);
	}
}
