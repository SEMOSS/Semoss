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
import prerna.util.Utility;

/**
 * Uploads a file from the insight folder into OneDrive.
 *
 * <p>
 * The insight folder is the root the file path is resolved against, so callers
 * name a file relative to it rather than by an absolute path.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.ReadWrite} for {@code PUT /me/drive/root:/{path}:/content}
 * and for {@code POST .../createUploadSession}, or {@code Files.ReadWrite.All}
 * to upload into a drive somebody else owns and shared with write access.</li>
 * </ul>
 *
 * <p>
 * Writing into a folder somebody shared works the same way as writing into the
 * user's own drive, by naming that folder's {@code driveId} and {@code itemId}.
 * Whether it is allowed is Graph's decision: a share given as read only refuses
 * the upload, which is the answer the caller should get.
 * </p>
 *
 * <p>
 * The destination folder has to exist. Graph answers a path addressed upload
 * into a folder that is not there with a 404 rather than creating it, so create
 * it with {@code MicrosoftOneDriveCreateFolder} first.
 * </p>
 */
public class MicrosoftOneDriveUploadFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveUploadFileReactor.class);

	public MicrosoftOneDriveUploadFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NAME.getKey(), DRIVE_ID,
				ITEM_ID, PATH, CONFLICT_BEHAVIOR };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String filePath = trimToNull(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		String name = trimToNull(this.keyValue.get(ReactorKeysEnum.NAME.getKey()));
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		String conflictBehavior = trimToNull(this.keyValue.get(CONFLICT_BEHAVIOR));

		if (filePath == null) {
			throw new SemossPixelException("A file path is required to upload to OneDrive.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);

			File sourceFile = resolveInsightFile(filePath);
			// default the name in the drive to whatever the source file is called
			if (name == null) {
				name = sourceFile.getName();
			}

			Map<String, Object> result = MicrosoftOneDriveHelper.uploadFile(accessToken, driveId, itemId, path, name,
					sourceFile.getAbsolutePath(), conflictBehavior);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while uploading a file to OneDrive", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to upload a file to OneDrive", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to upload a file to OneDrive", e);
			throw new SemossPixelException("An error occurred uploading the file. Error message: " + e.getMessage());
		}
	}

	/**
	 * Resolves a path against the insight folder, which is the root this reactor
	 * uploads from.
	 *
	 * <p>
	 * {@link Utility#normalizePath} collapses any {@code ..} segments and rejects a
	 * path that climbs above its root, which is the same guard the insight asset
	 * reactors use. Stripping a leading slash afterwards keeps an absolute path
	 * from being read as one, so it resolves under the insight folder like any
	 * other relative path.
	 * </p>
	 *
	 * @param requestedPath path relative to the insight folder
	 * @return the resolved, readable file
	 * @throws SemossPixelException if the path is not valid or does not point at a
	 *                              readable file
	 */
	private File resolveInsightFile(String requestedPath) {
		String insightFolder = this.insight.getInsightFolder();
		File insightFolderFile = new File(insightFolder);

		String relativePath;
		try {
			relativePath = Utility.normalizePath(requestedPath);
		} catch (IllegalArgumentException e) {
			classLogger.error("Rejected an invalid OneDrive upload path '{}'", requestedPath, e);
			throw new SemossPixelException("The file path is not valid: " + requestedPath);
		}
		while (relativePath.startsWith("/")) {
			relativePath = relativePath.substring(1);
		}
		if (relativePath.isEmpty()) {
			throw new SemossPixelException("A file path is required to upload to OneDrive.");
		}

		File sourceFile = new File(insightFolderFile, relativePath);
		if (!sourceFile.exists() || !sourceFile.isFile()) {
			throw new SemossPixelException("No file exists in the insight folder at: " + relativePath);
		}
		return sourceFile;
	}

	@Override
	public String getReactorDescription() {
		return "Upload a file from the insight folder into OneDrive, into the signed in user's own drive or into a folder shared with them.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path of the file to upload, relative to the insight folder.";
		} else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Optional name to give the file in the drive. The source file name is used when omitted.";
		} else if (key.equals(ITEM_ID)) {
			return "Optional id of the folder to upload into. The drive root is used when omitted.";
		} else if (key.equals(PATH)) {
			return "Optional path of the folder to upload into, relative to the drive root, or relative to the folder the item id names when both are given. The folder has to exist already.";
		} else if (key.equals(CONFLICT_BEHAVIOR)) {
			return "Optional behavior when a file of the same name is already there: fail, rename or replace. Defaults to rename.";
		}
		return super.getDescriptionForKey(key);
	}
}
