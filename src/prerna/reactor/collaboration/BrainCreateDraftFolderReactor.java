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
package prerna.reactor.collaboration;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainCreateDraftFolder(name=["Teamwork Drafts"], inInbox=[true]);
// Makes the Outlook folder, or finds the one of that name; the caller saves it as the draft folder setting
public class BrainCreateDraftFolderReactor extends AbstractCollaborationReactor {

	private static final String NAME = "name";
	private static final String IN_INBOX = "inInbox";

	public BrainCreateDraftFolderReactor() {
		this.keysToGet = new String[] { NAME, IN_INBOX };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String name = getString(NAME) == null ? "" : getString(NAME).trim();
		if (name.isEmpty() || name.length() > 100) {
			throw new IllegalArgumentException("Give the folder a name of up to 100 characters");
		}
		boolean inInbox = Boolean.TRUE.equals(getBoolean(IN_INBOX));
		try {
			Map<String, Object> folder = new MicrosoftOutlookMailHelper().createFolder(
					MicrosoftLoginUtils.getValidAccessToken(user), null, inInbox ? "inbox" : null, name);
			if (folder == null || !(folder.get("id") instanceof String id) || id.isBlank()) {
				throw new IllegalStateException("The folder could not be confirmed. Check Outlook before retrying.");
			}
			Map<String, Object> out = new LinkedHashMap<>();
			out.put("id", id);
			out.put("name", name);
			out.put("path", inInbox ? "Inbox / " + name : name);
			return mapResult(out);
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new SemossPixelException("The folder could not be created: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates an Outlook folder for Collaboration drafts, or returns the one that already has that name";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (NAME.equals(key)) {
			return "The folder name";
		} else if (IN_INBOX.equals(key)) {
			return "True to make it inside Inbox; otherwise it is made at the top level of the mailbox";
		}
		return super.getDescriptionForKey(key);
	}
}
