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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UpdateStorageFileMetadataReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateStorageFileMetadataReactor.class);

	public UpdateStorageFileMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				ReactorKeysEnum.METADATA.getKey(), };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
			throw new IllegalArgumentException("User does not have permission to access this storage engine");
		}

		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		if (storagePath == null || storagePath.isEmpty()) {
			throw new IllegalArgumentException("Storage path is required");
		}

		Map<String, Object> metadata = getMapFromKeyOrCurRow(ReactorKeysEnum.METADATA.getKey());

		try {
			storage.updateBlobMetadata(storagePath, metadata);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error("Failed to apply metadata to storagePath={} on storage engine={}", storagePath,
					storage.getEngineId(), e);
			throw new IllegalArgumentException("Error occurred applying metadata", e);
		}
	}

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			if (grs.get(0) instanceof String) {
				return Utility.getStorage((String) grs.get(0));
			}
			return (IStorageEngine) grs.get(0);
		}

		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if (storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}

		throw new NullPointerException("No storage engine defined");
	}

	@Override
	public String getReactorDescription() {
		return "Sets the metadata on a file already in storage. This replaces the metadata on the file "
				+ "rather than merging into it, so pass every key the file should end up with.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if (key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The path of the file in storage to set the metadata on";
		} else if (key.equals(ReactorKeysEnum.METADATA.getKey())) {
			return "The metadata to set on the file, as a map. Values are stored as strings, and engines "
					+ "with nowhere to keep user metadata ignore it";
		}
		return super.getDescriptionForKey(key);
	}
}
