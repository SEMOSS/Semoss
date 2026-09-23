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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the mail folders of the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Read} for {@code GET /me/mailFolders}. {@code Mail.ReadWrite}
 * also satisfies it.</li>
 * </ul>
 *
 * <p>
 * This is how a caller finds out what a mailbox is actually organized into,
 * rather than guessing at folder names. The id of a folder here is what
 * {@code MicrosoftOutlookListMail} takes as its {@code folder} and what
 * {@code MicrosoftOutlookMoveMail} moves into.
 * </p>
 *
 * <p>
 * Only the top level of the mailbox comes back. A folder somebody nested inside
 * another is reachable by its id but is not listed here.
 * </p>
 */
public class MicrosoftOutlookListMailFoldersReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookListMailFoldersReactor.class);

	public MicrosoftOutlookListMailFoldersReactor() {
		this.keysToGet = new String[] {};
		this.keyRequired = new int[] {};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> found = new MicrosoftOutlookMailHelper().listFolders(accessToken, null);

			List<Map<String, Object>> folders = new ArrayList<>();
			for (Map<String, Object> folder : found) {
				Map<String, Object> described = new LinkedHashMap<>();
				described.put("id", folder.get("id"));
				MicrosoftOutlookMessageMapper.putIfPresent(described, "name", folder.get("displayName"));
				MicrosoftOutlookMessageMapper.putIfPresent(described, "totalItemCount", folder.get("totalItemCount"));
				folders.add(described);
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", folders.size());
			output.put("folders", folders);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the signed in user's mail folders", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list the signed in user's mail folders", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of folders. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the mail folders of the signed in user's own Microsoft 365 mailbox.";
	}
}
