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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads what OneDrive knows about one file or folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive/items/{id}}, or
 * {@code Files.Read.All} for an item in a drive somebody else owns and for
 * {@code GET /shares/{id}/driveItem}. {@code Files.ReadWrite} and
 * {@code Files.ReadWrite.All} also satisfy these.</li>
 * </ul>
 *
 * <p>
 * A sharing link is the one way in that needs no drive: give the link somebody
 * sent and what comes back is the item it points at, carrying the
 * {@code driveId} and {@code id} that the download and listing reactors take.
 * </p>
 */
public class MicrosoftOneDriveGetFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveGetFileReactor.class);

	public MicrosoftOneDriveGetFileReactor() {
		this.keysToGet = new String[] { DRIVE_ID, ITEM_ID, PATH, SHARE_URL };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		String shareUrl = trimToNull(this.keyValue.get(SHARE_URL));

		if (itemId == null && path == null && shareUrl == null) {
			throw new SemossPixelException("An item id, a path or a sharing link is required to read a OneDrive item.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);

			Map<String, Object> item;
			if (shareUrl != null) {
				item = MicrosoftOneDriveHelper.resolveSharingLink(accessToken, shareUrl);
			} else {
				item = MicrosoftOneDriveHelper.getItem(accessToken, driveId, itemId, path);
			}
			return new NounMetadata(item, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading a OneDrive item", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read a OneDrive item", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read a OneDrive item", e);
			throw new SemossPixelException("An error occurred reading the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read the details of one OneDrive file or folder, by drive and item, by path, or by the sharing link somebody sent.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ITEM_ID)) {
			return "Id of the item to read, as returned by the OneDrive listings. Not needed when a path or a sharing link is given.";
		} else if (key.equals(PATH)) {
			return "Path of the item to read, relative to the drive root. Not needed when an item id or a sharing link is given.";
		}
		return super.getDescriptionForKey(key);
	}
}
