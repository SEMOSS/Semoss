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
import prerna.util.Utility;

/**
 * Uploads a file from the insight folder into a channel.
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
 * <li>{@code Files.ReadWrite.All} for
 * {@code PUT /drives/{id}/items/{id}:/{path}:/content} and for
 * {@code POST .../createUploadSession}, plus
 * {@code GET /teams/{id}/channels/{id}/filesFolder} to resolve the target
 * folder.</li>
 * </ul>
 */
public class MicrosoftTeamsUploadFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsUploadFileReactor.class);

	private static final String TEAM_ID = "teamId";
	private static final String CHANNEL_ID = "channelId";
	private static final String FOLDER_PATH = "folderPath";
	private static final String CONFLICT_BEHAVIOR = "conflictBehavior";

	public MicrosoftTeamsUploadFileReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), FOLDER_PATH, CONFLICT_BEHAVIOR };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = this.keyValue.get(this.keysToGet[0]);
		String channelId = this.keyValue.get(this.keysToGet[1]);
		String name = this.keyValue.get(this.keysToGet[2]);
		String path = this.keyValue.get(this.keysToGet[3]);
		String folderPath = this.keyValue.get(this.keysToGet[4]);
		String conflictBehavior = this.keyValue.get(this.keysToGet[5]);

		if (teamId == null || teamId.trim().isEmpty()) {
			throw new SemossPixelException("Team ID is required to upload to a Microsoft Teams channel.");
		}
		if (channelId == null || channelId.trim().isEmpty()) {
			throw new SemossPixelException("Channel ID is required to upload to a Microsoft Teams channel.");
		}
		if (path == null || path.trim().isEmpty()) {
			throw new SemossPixelException("File path is required to upload to a Microsoft Teams channel.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);

			File sourceFile = resolveInsightFile(path);
			// default the name in the channel to whatever the source file is called
			if (name == null || name.trim().isEmpty()) {
				name = sourceFile.getName();
			}

			Map<String, Object> result = MicrosoftTeamsHelper.uploadFile(accessToken, teamId, channelId, folderPath,
					name, sourceFile.getAbsolutePath(), conflictBehavior);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while uploading a file to a Microsoft Teams channel", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to upload a file to a Microsoft Teams channel", e);
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
			relativePath = Utility.normalizePath(requestedPath.trim());
		} catch (IllegalArgumentException e) {
			classLogger.error("Rejected an invalid Microsoft Teams upload path '{}'", requestedPath, e);
			throw new SemossPixelException("The file path is not valid: " + requestedPath);
		}
		while (relativePath.startsWith("/")) {
			relativePath = relativePath.substring(1);
		}
		if (relativePath.isEmpty()) {
			throw new SemossPixelException("File path is required to upload to a Microsoft Teams channel.");
		}

		File sourceFile = new File(insightFolderFile, relativePath);
		if (!sourceFile.exists() || !sourceFile.isFile()) {
			throw new SemossPixelException("No file exists in the insight folder at: " + relativePath);
		}
		return sourceFile;
	}

	@Override
	public String getReactorDescription() {
		return "Upload a file from the insight folder into a Microsoft Teams channel.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TEAM_ID)) {
			return "Microsoft Teams team ID that owns the channel.";
		} else if (key.equals(CHANNEL_ID)) {
			return "Microsoft Teams channel ID to upload into.";
		} else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Optional name to assign to the uploaded file in the channel. The source file name is used when omitted.";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path of the file to upload, relative to the insight folder.";
		} else if (key.equals(FOLDER_PATH)) {
			return "Optional folder path relative to the channel root folder. The file lands in the channel root folder when omitted.";
		} else if (key.equals(CONFLICT_BEHAVIOR)) {
			return "Optional behavior when a file of the same name exists: fail, rename or replace. Defaults to rename.";
		}
		return super.getDescriptionForKey(key);
	}
}
