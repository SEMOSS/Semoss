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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListDraftFolders();
// The Outlook folders a draft can be left in: the top level and one level below it, without Drafts itself
public class BrainListDraftFoldersReactor extends AbstractCollaborationReactor {

	// subfolders are read for at most this many parents, so a large mailbox stays quick
	private static final int MAX_PARENTS = 25;

	public BrainListDraftFoldersReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		try {
			String token = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOutlookMailHelper mail = new MicrosoftOutlookMailHelper();
			String drafts = mail.wellKnownFolderId(token, null, "drafts");
			List<Map<String, Object>> folders = new ArrayList<>();
			int expanded = 0;
			for (Map<String, Object> top : mail.listFolderChildren(token, null, null)) {
				String id = (String) top.get("id");
				if (id == null || id.equals(drafts)) {
					continue;
				}
				String name = String.valueOf(top.get("displayName"));
				folders.add(folder(id, name, name));
				if (top.get("childFolderCount") instanceof Number count && count.intValue() > 0
						&& expanded++ < MAX_PARENTS) {
					for (Map<String, Object> child : mail.listFolderChildren(token, null, id)) {
						if (child.get("id") instanceof String childId) {
							String childName = String.valueOf(child.get("displayName"));
							folders.add(folder(childId, childName, name + " / " + childName));
						}
					}
				}
			}
			Map<String, Object> out = new LinkedHashMap<>();
			out.put("folders", folders);
			return mapResult(out);
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new SemossPixelException("The mail folders could not be read: " + e.getMessage());
		}
	}

	private static Map<String, Object> folder(String id, String name, String path) {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("id", id);
		row.put("name", name);
		row.put("path", path);
		return row;
	}

	@Override
	public String getReactorDescription() {
		return "Lists the signed-in user's Outlook folders that Collaboration drafts can be left in";
	}
}
