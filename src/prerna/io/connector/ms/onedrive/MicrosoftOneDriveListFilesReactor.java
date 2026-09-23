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

import java.util.LinkedHashMap;
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
 * Lists what is inside a OneDrive folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive/root/children} and the paths
 * beneath it, or {@code Files.Read.All} to list a folder in a drive somebody
 * else owns. {@code Files.ReadWrite} and {@code Files.ReadWrite.All} also
 * satisfy these.</li>
 * </ul>
 *
 * <p>
 * Naming no drive lists the signed in user's own OneDrive, and naming one lists
 * that drive, which is how a folder somebody shared is browsed: take the
 * {@code driveId} and {@code id} off the entry that
 * {@code MicrosoftOneDriveListSharedFiles} returned for the folder and pass
 * them here.
 * </p>
 */
public class MicrosoftOneDriveListFilesReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveListFilesReactor.class);

	/** How many items come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 100;

	/** The most a caller can ask for, so a pixel cannot pull a whole drive. */
	private static final int MAX_LIMIT = 500;

	public MicrosoftOneDriveListFilesReactor() {
		this.keysToGet = new String[] { DRIVE_ID, ITEM_ID, PATH, ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> files = MicrosoftOneDriveHelper.listFiles(accessToken, driveId, itemId, path,
					limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", files.size());
			output.put("files", files);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing OneDrive files", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to list OneDrive files", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to list OneDrive files", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of files. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the files and folders inside a OneDrive folder, in the signed in user's own drive or in a drive shared with them.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ITEM_ID)) {
			return "Optional id of the folder to list. The drive root is listed when omitted.";
		} else if (key.equals(PATH)) {
			return "Optional path of the folder to list, relative to the drive root, or relative to the folder the item id names when both are given.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of items to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
