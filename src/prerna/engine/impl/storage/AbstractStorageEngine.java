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
package prerna.engine.impl.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.util.Utility;

public abstract class AbstractStorageEngine extends AbstractEngine implements IStorageEngine {

	protected static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	public static final String VERSIONING_ENABLED_KEY = "VERSIONING_ENABLED";

	protected boolean versioningEnabled = false;

	/**
	 * Init the general storage values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		String versioningStr = smssProp.getProperty(VERSIONING_ENABLED_KEY);
		if (versioningStr != null && !versioningStr.isEmpty()) {
			this.versioningEnabled = Boolean.parseBoolean(versioningStr);
		}
	}

	/**
	 * Check if versioning is enabled for this storage engine.
	 * 
	 * @return true if versioning is enabled
	 */
	@Override
	public boolean isVersioningEnabled() {
		return this.versioningEnabled;
	}

	/**
	 * Converts comma-separated local file/folder paths to List<Path>
	 * 
	 * @param commaSeparatedPaths
	 * @return
	 * @throws Exception
	 */
	protected List<Path> parseLocalPaths(String commaSeparatedPaths) throws Exception {
		List<Path> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				result.add(Paths.get(trimmed));
			}
		}

		return result;
	}

	/**
	 * Converts comma-separated cloud storage object paths to normalized String list
	 * 
	 * @param commaSeparatedPaths
	 * @return
	 */
	protected List<String> parseStorageObjectPaths(String commaSeparatedPaths) {
		List<String> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				// Normalize the path using the utility method
				String normalized = Utility.normalizePath(trimmed);
				// Remove the leading slash if present
				if (normalized.startsWith("/")) {
					normalized = normalized.substring(1);
				}
				result.add(normalized);
			}
		}

		return result;
	}

	/**
	 * Normalizes a requested storage path for prefix matching.
	 *
	 * @param storagePath path provided by caller
	 * @return normalized path with no leading/trailing slash
	 */
	protected String normalizeStoragePrefixPath(String storagePath) {
		if (storagePath == null) {
			return "";
		}

		String normalized = Utility.normalizePath(storagePath).trim().replace("\\", "/");
		while (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		while (!normalized.isEmpty() && normalized.endsWith("/")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		return normalized;
	}

	/**
	 * Resolves a storage object key to a path relative to the requested storage
	 * path.
	 *
	 * @param storageObjectKey full object key from cloud provider
	 * @param requestedPath    user-requested path (file or folder)
	 * @return relative path, or null when key does not belong to the requested
	 *         scope
	 */
	protected String resolveRelativeStoragePath(String storageObjectKey, String requestedPath) {
		if (storageObjectKey == null || storageObjectKey.trim().isEmpty()) {
			return null;
		}

		String normalizedObjectKey = storageObjectKey.trim().replace("\\", "/");
		while (normalizedObjectKey.startsWith("/")) {
			normalizedObjectKey = normalizedObjectKey.substring(1);
		}
		if (normalizedObjectKey.isEmpty()) {
			return null;
		}

		String normalizedRequestedPath = normalizeStoragePrefixPath(requestedPath);
		String relativePath;
		if (normalizedRequestedPath.isEmpty()) {
			relativePath = normalizedObjectKey;
		} else if (normalizedObjectKey.equals(normalizedRequestedPath)) {
			int slashIdx = normalizedObjectKey.lastIndexOf('/');
			relativePath = slashIdx >= 0 ? normalizedObjectKey.substring(slashIdx + 1) : normalizedObjectKey;
		} else {
			String folderPrefix = normalizedRequestedPath + "/";
			if (!normalizedObjectKey.startsWith(folderPrefix)) {
				return null;
			}
			relativePath = normalizedObjectKey.substring(folderPrefix.length());
		}

		while (relativePath.startsWith("/")) {
			relativePath = relativePath.substring(1);
		}
		if (relativePath.isEmpty() || relativePath.endsWith("/")) {
			return null;
		}
		return relativePath;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.STORAGE;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getStorageType().toString();
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}
