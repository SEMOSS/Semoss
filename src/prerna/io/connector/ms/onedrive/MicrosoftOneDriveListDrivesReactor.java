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
package prerna.io.connector.ms.onedrive;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the drives the signed in user can reach.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive}. {@code Files.ReadWrite}
 * also satisfies it.</li>
 * <li>{@code Files.Read.All} for the shared files that say which other drives
 * the user can reach. {@code Files.ReadWrite.All} also satisfies it.</li>
 * </ul>
 *
 * <p>
 * The signed in user's own OneDrive comes first, marked {@code isMine}, and
 * after it come the drives that the files shared with them live in. There is no
 * Graph call that enumerates other people's drives, so the only ones that can
 * be named are the ones something was shared out of.
 * </p>
 */
public class MicrosoftOneDriveListDrivesReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveListDrivesReactor.class);

	public MicrosoftOneDriveListDrivesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), 0, Integer.MAX_VALUE);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> drives = MicrosoftOneDriveHelper.listDrives(accessToken, limit);
			return new NounMetadata(drives, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the OneDrive drives of the signed in user", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list the OneDrive drives of the signed in user", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of drives. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the OneDrive drives the signed in user can reach: their own, and the drives holding the files shared with them.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of drives to return. All drives are returned when omitted.";
		}
		return super.getDescriptionForKey(key);
	}
}
