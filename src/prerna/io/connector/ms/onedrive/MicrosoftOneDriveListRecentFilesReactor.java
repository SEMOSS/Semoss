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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the files that changed lately in the signed in user's own OneDrive.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive/root/delta}.
 * {@code Files.Read.All}, {@code Files.ReadWrite} and
 * {@code Files.ReadWrite.All} also satisfy it.</li>
 * </ul>
 *
 * <p>
 * This reads the drive's change feed rather than {@code /me/drive/recent},
 * which Microsoft has deprecated and documents to stop returning data in
 * November 2026. The feed covers the user's own drive only: a file they changed
 * in a SharePoint library or a Teams channel is not in it. It takes a timestamp
 * only on OneDrive for Business, so a personal Microsoft account is answered
 * with an error.
 * </p>
 *
 * <p>
 * Every entry carries the {@code driveId} and {@code id} that address it, as
 * the other OneDrive listings do.
 * </p>
 */
public class MicrosoftOneDriveListRecentFilesReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveListRecentFilesReactor.class);

	/** How many days back a change still counts as recent. */
	private static final int RECENT_DAYS = 7;

	/** How many files come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 20;

	/** The most a caller can ask for. */
	private static final int MAX_LIMIT = 100;

	public MicrosoftOneDriveListRecentFilesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			List<Map<String, Object>> files = MicrosoftOneDriveHelper.listRecentFiles(accessToken,
					Instant.now().minus(RECENT_DAYS, ChronoUnit.DAYS), limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", files.size());
			output.put("files", files);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the recently changed OneDrive files", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to list the recently changed OneDrive files", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to list the recently changed OneDrive files", e);
			throw new SemossPixelException(
					"An error occurred retrieving the recently changed files. Error message: " + e.getMessage());
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("List the files changed in the signed in user's own OneDrive in the last " + RECENT_DAYS
						+ " days, the most recently changed first.")
				.key(ReactorKeysEnum.LIMIT, "Optional maximum number of files to return. Defaults to " + DEFAULT_LIMIT
						+ " and is capped at " + MAX_LIMIT + ".");
	}
}
