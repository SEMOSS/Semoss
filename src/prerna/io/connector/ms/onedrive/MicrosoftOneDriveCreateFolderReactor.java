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
 * Creates a folder in OneDrive.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.ReadWrite} for {@code POST /me/drive/root/children}, or
 * {@code Files.ReadWrite.All} to create in a drive somebody else owns and
 * shared with write access.</li>
 * </ul>
 *
 * <p>
 * Graph creates the one folder it is asked for and not the folders above it, so
 * building a new path of several levels means calling this once per level. The
 * default behavior when the folder is already there is to fail rather than to
 * replace it, since replacing a folder discards what is inside it.
 * </p>
 */
public class MicrosoftOneDriveCreateFolderReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveCreateFolderReactor.class);

	public MicrosoftOneDriveCreateFolderReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), DRIVE_ID, ITEM_ID, PATH, CONFLICT_BEHAVIOR };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String name = trimToNull(this.keyValue.get(ReactorKeysEnum.NAME.getKey()));
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		String conflictBehavior = trimToNull(this.keyValue.get(CONFLICT_BEHAVIOR));

		if (name == null) {
			throw new SemossPixelException("A folder name is required to create a OneDrive folder.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			Map<String, Object> folder = MicrosoftOneDriveHelper.createFolder(accessToken, driveId, itemId, path, name,
					conflictBehavior);
			return new NounMetadata(folder, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a OneDrive folder", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to create a OneDrive folder", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to create a OneDrive folder", e);
			throw new SemossPixelException("An error occurred creating the folder. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create a folder in OneDrive, in the signed in user's own drive or in a folder shared with them.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Name of the folder to create.";
		} else if (key.equals(ITEM_ID)) {
			return "Optional id of the folder to create inside. The drive root is used when omitted.";
		} else if (key.equals(PATH)) {
			return "Optional path of the folder to create inside, relative to the drive root, or relative to the folder the item id names when both are given.";
		} else if (key.equals(CONFLICT_BEHAVIOR)) {
			return "Optional behavior when a folder of the same name is already there: fail, rename or replace. Defaults to fail, which leaves the existing folder and its contents alone.";
		}
		return super.getDescriptionForKey(key);
	}
}
