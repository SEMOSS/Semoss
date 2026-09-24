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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
 * Finds files in OneDrive by name and by contents.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Files.Read} for {@code GET /me/drive/root/search(q=)}, or
 * {@code Files.Read.All} to search a drive somebody else owns.</li>
 * <li>{@code Files.Read.All} for {@code POST /search/query}, which is what
 * reaches across drives. {@code Files.ReadWrite.All} also satisfies it.</li>
 * </ul>
 *
 * <p>
 * Where the search looks is the {@code scope}, and it matters because Graph has
 * no one call that both searches everything and can be pointed at a folder:
 * </p>
 * <ul>
 * <li>{@code all} asks the search index, which covers the signed in user's own
 * OneDrive and every drive and SharePoint library shared with them. This is the
 * default when no drive or folder is named.</li>
 * <li>{@code drive} searches one drive, the user's own unless a {@code driveId}
 * names another, and can be narrowed to a folder. This is the default when a
 * drive, item or path is named.</li>
 * <li>{@code shared} asks the same index as {@code all} and then keeps only
 * what is not in the user's own drive, which answers "who sent me that file"
 * rather than "where is that file".</li>
 * </ul>
 */
public class MicrosoftOneDriveSearchFilesReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveSearchFilesReactor.class);

	private static final String SCOPE = "scope";

	private static final String SCOPE_ALL = "all";
	private static final String SCOPE_DRIVE = "drive";
	private static final String SCOPE_SHARED = "shared";
	private static final List<String> SCOPES = Arrays.asList(SCOPE_ALL, SCOPE_DRIVE, SCOPE_SHARED);

	/** How many files come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for. */
	private static final int MAX_LIMIT = 200;

	public MicrosoftOneDriveSearchFilesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), SCOPE, DRIVE_ID, ITEM_ID, PATH,
				ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String search = trimToNull(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey()));
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		int limit = positiveInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT, MAX_LIMIT);

		if (search == null) {
			throw new SemossPixelException("Search text is required to search OneDrive.");
		}
		// naming a drive or a folder is itself a statement about where to look, so it
		// picks the scope that can honor it
		boolean addressed = driveId != null || itemId != null || path != null;
		String scope = scope(addressed ? SCOPE_DRIVE : SCOPE_ALL);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);

			List<Map<String, Object>> files;
			if (SCOPE_DRIVE.equals(scope)) {
				files = MicrosoftOneDriveHelper.searchDrive(accessToken, driveId, itemId, path, search, limit);
			} else if (SCOPE_SHARED.equals(scope)) {
				files = MicrosoftOneDriveHelper.listSharedFiles(accessToken, search, limit);
			} else {
				files = MicrosoftOneDriveHelper.searchEverywhere(accessToken, search, limit);
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("search", search);
			output.put(SCOPE, scope);
			output.put("count", files.size());
			output.put("files", files);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while searching OneDrive for '{}'", search, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to search OneDrive", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to search OneDrive for '{}'", search, e);
			throw new SemossPixelException("An error occurred searching your files. Error message: " + e.getMessage());
		}
	}

	/**
	 * Read the scope, matched however the caller happened to capitalize it.
	 *
	 * @param fallback the scope to use when the caller left the key out
	 * @return the scope to search in
	 */
	private String scope(String fallback) {
		String value = trimToNull(this.keyValue.get(SCOPE));
		if (value == null) {
			return fallback;
		}
		String normalized = value.toLowerCase(Locale.ROOT);
		if (!SCOPES.contains(normalized)) {
			throw new SemossPixelException(SCOPE + " must be one of " + SCOPES + " but received: " + value);
		}
		return normalized;
	}

	@Override
	public String getReactorDescription() {
		return "Search the signed in user's OneDrive files, the files shared with them, or both.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "Text to look for in the file name and contents.";
		} else if (key.equals(SCOPE)) {
			return "Optional place to search, one of " + SCOPES
					+ ". 'all' covers the signed in user's own drive and everything shared with them, 'drive' searches one drive, and 'shared' keeps only what somebody else owns. Defaults to 'drive' when a drive, item or path is named and to 'all' otherwise.";
		} else if (key.equals(ITEM_ID)) {
			return "Optional id of the folder to search under, which only applies to the 'drive' scope. The whole drive is searched when omitted.";
		} else if (key.equals(PATH)) {
			return "Optional path of the folder to search under, which only applies to the 'drive' scope.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional maximum number of files to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
