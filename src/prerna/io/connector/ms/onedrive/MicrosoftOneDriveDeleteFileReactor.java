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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deletes a OneDrive file or folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.ReadWrite} for {@code DELETE /me/drive/items/{id}}, or
 * {@code Files.ReadWrite.All} to delete in a drive somebody else owns and
 * shared with write access.</li>
 * </ul>
 *
 * <p>
 * A folder goes with everything inside it, and what is deleted lands in the
 * drive's recycle bin rather than disappearing, so it can be restored from
 * OneDrive itself for as long as that bin keeps it. Naming neither an item nor
 * a path is refused rather than read as the drive root.
 * </p>
 */
public class MicrosoftOneDriveDeleteFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveDeleteFileReactor.class);

	public MicrosoftOneDriveDeleteFileReactor() {
		this.keysToGet = new String[] { DRIVE_ID, ITEM_ID, PATH };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));

		if (itemId == null && path == null) {
			throw new SemossPixelException("An item id or a path is required to delete a OneDrive item.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOneDriveHelper.deleteItem(accessToken, driveId, itemId, path);

			Map<String, Object> output = new LinkedHashMap<>();
			if (driveId != null) {
				output.put(DRIVE_ID, driveId);
			}
			if (itemId != null) {
				output.put(ITEM_ID, itemId);
			}
			if (path != null) {
				output.put(PATH, path);
			}
			output.put("success", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting a OneDrive item", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to delete a OneDrive item", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to delete a OneDrive item", e);
			throw new SemossPixelException("An error occurred deleting the item. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete a OneDrive file or folder, which sends it to the drive's recycle bin.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ITEM_ID)) {
			return "Id of the item to delete, as returned by the OneDrive listings. Not needed when a path is given.";
		} else if (key.equals(PATH)) {
			return "Path of the item to delete, relative to the drive root. Not needed when an item id is given.";
		}
		return super.getDescriptionForKey(key);
	}
}
