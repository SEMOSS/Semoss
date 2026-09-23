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

import java.io.File;
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
 * Downloads a OneDrive file into the insight folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive/items/{id}} and its content,
 * or {@code Files.Read.All} for a file in a drive somebody else owns and for
 * {@code GET /shares/{id}/driveItem}. {@code Files.ReadWrite} and
 * {@code Files.ReadWrite.All} also satisfy these.</li>
 * </ul>
 *
 * <p>
 * A file somebody shared is downloaded the same way as one of the user's own,
 * by passing the {@code driveId} and {@code itemId} that
 * {@code MicrosoftOneDriveListSharedFiles} returned. A file that arrived as a
 * link is downloaded by passing that link instead, which spares the caller
 * having to resolve it first.
 * </p>
 */
public class MicrosoftOneDriveDownloadFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveDownloadFileReactor.class);

	public MicrosoftOneDriveDownloadFileReactor() {
		this.keysToGet = new String[] { DRIVE_ID, ITEM_ID, PATH, SHARE_URL, ReactorKeysEnum.FILE_NAME.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		String shareUrl = trimToNull(this.keyValue.get(SHARE_URL));

		if (itemId == null && path == null && shareUrl == null) {
			throw new SemossPixelException(
					"An item id, a path or a sharing link is required to download a OneDrive file.");
		}
		// the file lands in the insight folder, so only the base name is honored. A
		// name carrying separators would otherwise write outside that folder
		String fileName = toBaseName(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);

			// create it up front: for an unsaved insight the folder may not exist yet, and
			// the download treats an existing directory as the destination rather than as
			// the file to write
			String insightFolder = this.insight.getInsightFolder();
			File insightFolderFile = new File(insightFolder);
			if (!insightFolderFile.exists() && !insightFolderFile.mkdirs()) {
				throw new SemossPixelException("Unable to create the insight folder at: " + insightFolder);
			}

			Map<String, Object> result = MicrosoftOneDriveHelper.downloadFile(accessToken, driveId, itemId, path,
					shareUrl, insightFolder, fileName);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while downloading a OneDrive file", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to download a OneDrive file", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to download a OneDrive file", e);
			throw new SemossPixelException("An error occurred downloading the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Download a OneDrive file into the insight folder, from the signed in user's own drive, from a drive shared with them, or from a sharing link.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ITEM_ID)) {
			return "Id of the file to download, as returned by the OneDrive listings. Not needed when a path or a sharing link is given.";
		} else if (key.equals(PATH)) {
			return "Path of the file to download, relative to the drive root. Not needed when an item id or a sharing link is given.";
		} else if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Optional name to save the file as in the insight folder. The name it carries in the drive is used when omitted.";
		}
		return super.getDescriptionForKey(key);
	}

	/**
	 * Reduces a requested file name to its base name, so the download cannot be
	 * steered outside the insight folder.
	 *
	 * @param fileName the requested name, may be null or blank
	 * @return the base name, or null when nothing usable was supplied
	 */
	private static String toBaseName(String fileName) {
		if (fileName == null) {
			return null;
		}
		String trimmed = fileName.trim().replace('\\', '/');
		int lastSlash = trimmed.lastIndexOf('/');
		if (lastSlash >= 0) {
			trimmed = trimmed.substring(lastSlash + 1);
		}
		trimmed = trimmed.trim();
		return trimmed.isEmpty() || trimmed.equals(".") || trimmed.equals("..") ? null : trimmed;
	}
}
