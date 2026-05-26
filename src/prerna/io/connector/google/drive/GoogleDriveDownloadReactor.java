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
package prerna.io.connector.google.drive;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDriveDownloadReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleDriveDownloadReactor.class);

	private static final String ID = "id";
	private static final String SUCCESS = "success";
	private static final String FILE_NAME = "fileName";

	public GoogleDriveDownloadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), FILE_NAME };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String fileId = this.keyValue.get(this.keysToGet[0]);
		String path = this.keyValue.get(this.keysToGet[1]);
		String fileName = null;
		if (fileId == null || fileId.trim().isEmpty()) {
			throw new SemossPixelException("File ID is required to download a Google Drive file.");
		}
		if (path == null || path.trim().isEmpty()) {
			throw new SemossPixelException("Destination path is required to download a Google Drive file.");
		}

		if (this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty()) {
			fileName = this.keyValue.get(this.keysToGet[2]);
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			GoogleDriveHelper.downloadFile(accessToken, fileId, path, fileName);
			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS, true);
			result.put(ID, fileId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while downloading a Google Drive file", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to download a Google Drive file", e);
			throw new SemossPixelException("An error occurred downloading the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Download a file from Google Drive to a local path.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Google Drive file ID for the file to download.";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Local path where the file should be downloaded.";
		} else if (key.equals(FILE_NAME)) {
			return "Optional local file name to use for the downloaded file.";
		}
		return super.getDescriptionForKey(key);
	}
}
