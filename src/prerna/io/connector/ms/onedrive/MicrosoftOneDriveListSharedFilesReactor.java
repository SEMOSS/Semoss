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
 * Lists the files other people have shared with the signed in user.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.Read.All} for {@code GET /me/drive/sharedWithMe} and for
 * {@code POST /search/query}. {@code Files.ReadWrite.All} also satisfies both,
 * and Microsoft documents that the narrower {@code Files.Read} answers the
 * shared listing with fields missing and without access to the items
 * themselves.</li>
 * </ul>
 *
 * <p>
 * Two sources are read and merged, because the obvious one is going away.
 * Microsoft has deprecated {@code sharedWithMe}: it is documented to stop
 * returning data in November 2026 and already answers with fewer items than the
 * OneDrive web app shows. The search index is what Microsoft points at instead,
 * and it covers every drive the user can read. Asking both means the listing
 * neither empties out at that cutoff nor loses the shares only the older call
 * knows about.
 * </p>
 * 
 * <p>
 * Every entry carries the {@code driveId} and {@code id} that address it, which
 * is what {@code MicrosoftOneDriveListFiles},
 * {@code MicrosoftOneDriveDownloadFile} and {@code MicrosoftOneDriveGetFile}
 * take, so a shared file is worked with exactly like one of the user's own.
 * </p>
 */
public class MicrosoftOneDriveListSharedFilesReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveListSharedFilesReactor.class);

	/** How many files come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for. */
	private static final int MAX_LIMIT = 200;

	public MicrosoftOneDriveListSharedFilesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String search = trimToNull(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey()));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			List<Map<String, Object>> files = MicrosoftOneDriveHelper.listSharedFiles(accessToken, search, limit);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("count", files.size());
			output.put("files", files);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing the files shared with the signed in user", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to list the files shared with the signed in user", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to list the files shared with the signed in user", e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of shared files. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "List the OneDrive and SharePoint files other people have shared with the signed in user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "Optional text the file has to match. Everything shared with the signed in user is returned when omitted.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of files to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
