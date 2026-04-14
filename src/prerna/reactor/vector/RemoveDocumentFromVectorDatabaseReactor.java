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
package prerna.reactor.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class RemoveDocumentFromVectorDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoveDocumentFromVectorDatabaseReactor.class);

	public RemoveDocumentFromVectorDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "fileNames",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to this engine");
		}

		List<String> fileNames = getFiles();
		Map<String, Object> paramMap = getMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		paramMap.put(Constants.INSIGHT, this.insight);
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		try {
			eng.removeDocument(fileNames, paramMap);
		} catch (Exception e) {
			classLogger.error("Error removing documents from vector database {}", engineId, e);
			throw new IllegalArgumentException(
					"Error occurred attempting to delete the files. Detailed message = " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}

	/**
	 * @return list of files to delete
	 */
	public List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			filePaths.add(this.curRow.get(i).toString());
		}
		return filePaths;
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(keysToGet[2]);
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
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals("fileNames")) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Removes documents from a vector database. \
				Deletes the specified files and their associated embeddings from the vector database. \
				The file names should match the source names that were used when the documents were added.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The vector database engine ID to remove documents from.";
		} else if (key.equals("fileNames")) {
			return "The list of source identifiers to remove from the vector database (typically document file names).";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Optional engine-specific parameters (for example index/collection selectors when supported).";
		}
		return super.getDescriptionForKey(key);
	}
}
