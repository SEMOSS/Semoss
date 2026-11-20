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
import prerna.io.connector.ConnectorPage;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the places the signed in user's mail is filed: Outlook's folders, or
 * Gmail's labels.
 */
public abstract class AbstractListMailFoldersReactor extends AbstractMailReactor {

	/** How many folders come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 100;

	/** The most a caller can ask for at once. */
	private static final int MAX_LIMIT = 500;

	private static final String[] KEYS = { LIMIT, OFFSET };

	protected AbstractListMailFoldersReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet);
	}

	/**
	 * Read one page of folders.
	 *
	 * @param user   the signed in user
	 * @param offset how many folders to skip
	 * @param limit  how many folders the page holds
	 * @return the page
	 * @throws Exception when the mailbox cannot be read
	 */
	protected abstract ConnectorPage<MailFolder> listFolders(User user, int offset, int limit) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("list your mail folders", () -> {
			int offset = readOffset();
			ConnectorPage<MailFolder> page = listFolders(this.insight.getUser(), offset,
					readCount(LIMIT, DEFAULT_LIMIT, MAX_LIMIT));

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(OFFSET, offset);
			output.put("count", page.items().size());
			output.put("hasMore", page.hasMore());
			output.put("folders", page.items().stream().map(MailFolder::toMap).toList());
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
		return "List the folders the signed in user's own " + mailbox()
				+ " files mail in, which in Gmail are its labels.";
	}

	@Override
	protected final String describeKey(String key) {
		if (LIMIT.equals(key)) {
			return "Optional number of folders to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.describeKey(key);
	}
}
