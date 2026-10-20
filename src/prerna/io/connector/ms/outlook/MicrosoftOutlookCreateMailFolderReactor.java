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
package prerna.io.connector.ms.outlook;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.io.connector.mail.AbstractMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Finds a folder of the signed in user's own Microsoft 365 mailbox by name inside
 * another one, making it when it is not there, so calling it twice is safe.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.ReadWrite}.
 * </p>
 */
public class MicrosoftOutlookCreateMailFolderReactor extends AbstractMailReactor {

	private static final String NAME = "name";
	private static final String PARENT = "parent";

	public MicrosoftOutlookCreateMailFolderReactor() {
		this.keysToGet = new String[] { NAME, PARENT };
		this.keyRequired = requiredKeys(this.keysToGet, NAME);
	}

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("create the mail folder", () -> {
			String name = requireString(NAME, "A " + NAME + " for the folder is required.").trim();
			String parent = readString(PARENT);
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(this.insight.getUser());
			MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
			Map<String, Object> folder = helper.ensureChildFolder(accessToken, null, parent == null || parent.isBlank()
					? "msgfolderroot" : helper.resolveDestination(accessToken, null, parent), name);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(ID, folder.get("id"));
			output.put(NAME, folder.get("displayName"));
			output.put("created", folder.get("created"));
			return output;
		});
	}

	@Override
	protected MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected String getView() {
		return null;
	}

	@Override
	protected String describe() {
		return "Find or create a folder in the signed in user's own " + mailbox()
				+ ", returning its id; an existing folder of that name is reused.";
	}

	@Override
	protected String describeKey(String key) {
		if (NAME.equals(key)) {
			return "Name of the folder.";
		} else if (PARENT.equals(key)) {
			return "Optional folder to put it in: a well known name such as drafts or inbox, or a folder id or name "
					+ "as returned by " + reactorName("ListMailFolders") + ". Defaults to the top of the mailbox.";
		}
		return super.describeKey(key);
	}
}
