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
 * Marks a message read, or back to unread.
 */
public abstract class AbstractMarkMailReadReactor extends AbstractMailReactor {

	private static final String[] KEYS = { ID, READ };

	protected AbstractMarkMailReadReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Mark the message.
	 *
	 * @param user the signed in user
	 * @param id   the message
	 * @param read true to mark it read, false to mark it unread
	 * @throws Exception when the message cannot be marked
	 */
	protected abstract void markRead(User user, String id, boolean read) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("mark the message", () -> {
			String id = requireId("mark a message");
			// marking something read is what a caller almost always means, so that is
			// what happens when nothing says otherwise
			boolean read = readBoolean(READ, true);
			markRead(this.insight.getUser(), id, read);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(ID, id);
			output.put(READ, read);
			output.put("unread", !read);
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return null;
	}

	@Override
	protected final String describe() {
		return "Mark a message in the signed in user's own " + mailbox() + " as read, or back to unread.";
	}

	@Override
	protected final String describeKey(String key) {
		if (READ.equals(key)) {
			return "Optional boolean for whether the message is marked read. Defaults to true; pass false to mark it unread again.";
		}
		return super.describeKey(key);
	}
}
