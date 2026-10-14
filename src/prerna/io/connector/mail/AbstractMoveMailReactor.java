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
 * Moves a message into another folder, or in Gmail, under another label.
 */
public abstract class AbstractMoveMailReactor extends AbstractMailReactor {

	private static final String[] KEYS = { ID, FOLDER };

	protected AbstractMoveMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID, FOLDER);
	}

	/**
	 * Move the message.
	 *
	 * @param user   the signed in user
	 * @param id     the message
	 * @param folder where to move it, by well known name, name or id
	 * @return the id the message has after the move
	 * @throws Exception when the message cannot be moved
	 */
	protected abstract String moveMail(User user, String id, String folder) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("move the message", () -> {
			String id = requireId("move a message");
			String folder = requireString(FOLDER, "A " + FOLDER + " to move the message into is required.");
			String movedId = moveMail(this.insight.getUser(), id, folder);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("moved", true);
			// the id it had and the one it has now, since Outlook gives a moved message
			// a new one and a caller holding the old one has nothing to read with it
			output.put("previousId", id);
			output.put(ID, movedId == null ? id : movedId);
			output.put(FOLDER, folder);
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "mail/message?intent=move";
	}

	@Override
	protected final String describe() {
		return "Move a message into another folder of the signed in user's own " + mailbox() + ".";
	}

	@Override
	protected final String describeKey(String key) {
		if (FOLDER.equals(key)) {
			return "Where to move the message: inbox, archive, deleteditems or junkemail, or a folder id or name as "
					+ "returned by " + reactorName("ListMailFolders") + ".";
		}
		return super.describeKey(key);
	}
}
