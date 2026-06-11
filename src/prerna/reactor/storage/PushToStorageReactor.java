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
package prerna.reactor.storage;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.storage.AbstractStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/**
 * Push files from a local path to a storage path.
 *
 * Pixel usage: PushToStorage(storage=["<id>"], storagePath=["<path>"], filePath=["<local>"], metadata=[{}]);
 *
 * Parameters:
 *   storage     (String, required) - The storage engine instance or id
 *   storagePath (String, required) - The storage path to upload files to
 *   filePath    (String, required) - The local path(s) to upload from
 *   space       (String, optional) - The project space context
 *   metadata    (Map, optional)    - Metadata to attach to uploaded objects
 *
 * Returns: MAP - containing success status and versionId (if versioning enabled), or BOOLEAN true.
 */
public class PushToStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PushToStorageReactor.class);

	public PushToStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.METADATA.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		// check that the user can edit the engine
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
			throw new IllegalArgumentException("User does not have permission to push into the remote storage");
		}
		String storageFolderPath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if (!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}

		Map<String, Object> metadata = getMetadata();
		try {
			// If this engine supports versioning, use the versioned upload
			if (storage instanceof AbstractStorageEngine
					&& ((AbstractStorageEngine) storage).isVersioningEnabled()) {
				String versionId = storage.copyToStorageVersioned(fileLocation, storageFolderPath, metadata);
				if (versionId != null && !versionId.isEmpty()) {
					Map<String, Object> result = new HashMap<>();
					result.put("success", true);
					result.put("versionId", versionId);
					return new NounMetadata(result, PixelDataType.MAP);
				}
			} else {
				storage.copyToStorage(fileLocation, storageFolderPath, metadata);
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred uploading local file to storage");
		}
	}

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			IStorageEngine storage = null;
			if (grs.get(0) instanceof String) {
				String storageId = (String) grs.get(0);
				storage = Utility.getStorage(storageId);
			} else {
				storage = (IStorageEngine) grs.get(0);
			}
			return storage;
		}

		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if (storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}

		throw new NullPointerException("No storage engine defined");
	}

	private Map<String, Object> getMetadata() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.METADATA.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Push files from a local path to a storage path";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if (key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The storage path to upload files to";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The local path(s) to upload from";
		} else if (key.equals(ReactorKeysEnum.METADATA.getKey())) {
			return "Optional metadata map to attach to uploaded objects";
		}
		return super.getDescriptionForKey(key);
	}

}
