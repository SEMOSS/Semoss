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
package prerna.io.connector.ms.teams;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Downloads a file from a channel into the insight folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read.All} for {@code GET /drives/{id}/items/{id}} and the
 * file content, plus {@code GET /teams/{id}/channels/{id}/filesFolder} when the
 * drive ID is not supplied. {@code Files.ReadWrite.All} also satisfies
 * these.</li>
 * </ul>
 */
public class MicrosoftTeamsDownloadFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsDownloadFileReactor.class);

	private static final String TEAM_ID = "teamId";
	private static final String DRIVE_ID = "driveId";
	private static final String FILE_NAME = "fileName";
	private static final String CHANNEL_ID = "channelId";

	public MicrosoftTeamsDownloadFileReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, ReactorKeysEnum.ID.getKey(), FILE_NAME, DRIVE_ID };
		this.keyRequired = new int[] { 0, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = this.keyValue.get(this.keysToGet[0]);
		String channelId = this.keyValue.get(this.keysToGet[1]);
		String fileId = this.keyValue.get(this.keysToGet[2]);
		String fileName = this.keyValue.get(this.keysToGet[3]);
		String driveId = this.keyValue.get(this.keysToGet[4]);

		if (fileId == null || fileId.trim().isEmpty()) {
			throw new SemossPixelException("File ID is required to download a Microsoft Teams file.");
		}
		// the drive id shortcuts the channel lookup, so it stands in for the team and
		// channel ids when supplied
		boolean hasDriveId = driveId != null && !driveId.trim().isEmpty();
		if (!hasDriveId
				&& (teamId == null || teamId.trim().isEmpty() || channelId == null || channelId.trim().isEmpty())) {
			throw new SemossPixelException(
					"Either the drive ID or both the team ID and the channel ID are required to download a Microsoft Teams file.");
		}
		// the file lands in the insight folder, so only the base name is honored. A
		// name carrying separators would otherwise write outside that folder
		fileName = toBaseName(fileName);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);

			// create it up front: for an unsaved insight the folder may not exist yet, and
			// the download treats an existing directory as the destination rather than as
			// the file to write
			String insightFolder = this.insight.getInsightFolder();
			File insightFolderFile = new File(insightFolder);
			if (!insightFolderFile.exists() && !insightFolderFile.mkdirs()) {
				throw new SemossPixelException("Unable to create the insight folder at: " + insightFolder);
			}

			Map<String, Object> result = MicrosoftTeamsHelper.downloadFile(accessToken, teamId, channelId, driveId,
					fileId, insightFolder, fileName);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while downloading a Microsoft Teams file", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to download a Microsoft Teams file", e);
			throw new SemossPixelException("An error occurred downloading the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Download a file from a Microsoft Teams channel into the insight folder.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TEAM_ID)) {
			return "Microsoft Teams team ID that owns the channel. Not needed when the drive ID is supplied.";
		} else if (key.equals(CHANNEL_ID)) {
			return "Microsoft Teams channel ID that holds the file. Not needed when the drive ID is supplied.";
		} else if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Drive item ID of the file to download, as returned by MicrosoftTeamsListFiles.";
		} else if (key.equals(FILE_NAME)) {
			return "Optional name to save the file as in the insight folder. The name it carries in the channel is used when omitted.";
		} else if (key.equals(DRIVE_ID)) {
			return "Optional SharePoint drive ID returned by MicrosoftTeamsListFiles, which skips the channel lookup.";
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
