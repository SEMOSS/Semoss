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
 * Deletes a message by moving it to the trash, from where it can still be
 * restored. Nothing here erases mail for good.
 */
public abstract class AbstractDeleteMailReactor extends AbstractMailReactor {

	private static final String[] KEYS = { ID };

	protected AbstractDeleteMailReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Move the message to the trash.
	 *
	 * @param user the signed in user
	 * @param id   the message
	 * @throws Exception when the message cannot be deleted
	 */
	protected abstract void deleteMail(User user, String id) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("delete the message", () -> {
			String id = requireId("delete a message");
			deleteMail(this.insight.getUser(), id);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("deleted", true);
			output.put(ID, id);
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "mail/message?intent=delete";
	}

	@Override
	protected final String describe() {
		return "Delete a message from the signed in user's own " + mailbox()
				+ ". It goes to the trash, from where it can still be restored.";
	}
}
