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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/**
 * Pull files from a storage path to a local path.
 *
 * Pixel usage: PullFromStorage(storage=["<id>"], storagePath=["<path>"], filePath=["<local>"], version=["<versionId>"]);
 *
 * Parameters:
 *   storage     (String, required) - The storage engine instance or id
 *   storagePath (String, required) - The storage path(s) to download from
 *   filePath    (String, required) - The local path to download files to
 *   space       (String, optional) - The project space context
 *   version     (String, optional) - The version ID to download a specific object version
 *
 * Returns: BOOLEAN - true on success.
 */
public class PullFromStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PullFromStorageReactor.class);

	public PullFromStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.VERSION.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if (!(new File(fileLocation).isDirectory())) {
			new File(fileLocation).mkdirs();
		}

		String versionId = this.keyValue.get(ReactorKeysEnum.VERSION.getKey());

		try {
			if (versionId != null && !versionId.isEmpty()) {
				storage.copyToLocal(storagePath, fileLocation, versionId);
			} else {
				storage.copyToLocal(storagePath, fileLocation);
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading storage file to local");
		}
	}

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			IStorageEngine storage = null;
			if (grs.get(0) instanceof String) {
				String storageId = (String) grs.get(0);
				if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), storageId)) {
					throw new IllegalArgumentException(
							"Storage " + storageId + " does not exist or user does not have access to storage");
				}
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

	@Override
	public String getReactorDescription() {
		return "Pull files from a storage path to a local path";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if (key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The storage path(s) to download from";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The local path to download files to";
		} else if (key.equals(ReactorKeysEnum.VERSION.getKey())) {
			return "Optional version ID to download a specific object version (S3 versionId or GCS generation)";
		}
		return super.getDescriptionForKey(key);
	}

}
