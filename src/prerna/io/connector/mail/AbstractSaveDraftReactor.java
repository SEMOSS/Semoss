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
 * Saves a message as a draft, for the user to finish and send. Nothing is
 * required, since a draft is meant to be finished by hand.
 */
public abstract class AbstractSaveDraftReactor extends AbstractComposeMailReactor {

	protected AbstractSaveDraftReactor(String... extraKeys) {
		super(extraKeys);
	}

	/**
	 * Save the message in the user's drafts.
	 *
	 * @param user the signed in user
	 * @param mail what the caller wrote
	 * @return the draft as saved, with the id that sends it
	 * @throws Exception when the draft cannot be saved
	 */
	protected abstract ComposedMail saveDraft(User user, OutgoingMail mail) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("save the draft", () -> {
			OutgoingMail mail = readOutgoing(false, "save a draft");
			ComposedMail draft = saveDraft(this.insight.getUser(), mail);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("draft", true);
			output.putAll(draft.toMap());
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "mail/compose?intent=draft";
	}

	@Override
	protected final String describe() {
		return "Save an email as a draft in the signed in user's own " + mailbox() + ", for them to review and send.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case TO:
			return "Optional recipients of the draft, as a list of email addresses, which can be left for the "
					+ "person reviewing it to fill in.";
		case SUBJECT:
			return "Optional subject line of the draft.";
		case BODY:
			return "Optional body of the draft.";
		default:
			return super.describeKey(key);
		}
	}
}
