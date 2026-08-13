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
package prerna.reactor.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.stream.JsonReader;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class LoadEngineMetadataReactor extends AbstractSetMetadataReactor {

	private static final Logger classLogger = LogManager.getLogger(LoadEngineMetadataReactor.class);

	public LoadEngineMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if (!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}

		Map<String, Object> metadata = null;
		JsonReader jReader = null;
		BufferedReader fReader = null;
		try {
			fReader = Files.newBufferedReader(Paths.get(fileLocation), StandardCharsets.UTF_8);
			jReader = new JsonReader(fReader);
			metadata = GSON.fromJson(jReader, Map.class);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (fReader != null) {
				try {
					fReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (jReader != null) {
				try {
					jReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		String engineId = (String) metadata.remove("engineId");
		if (engineId == null) {
			// assume its in the filename
			String engineAliasAndId = FilenameUtils.getBaseName(fileLocation);
			if (engineAliasAndId.contains("__")) {
				engineId = engineAliasAndId.split("__")[1];
			} else {
				engineId = engineAliasAndId;
			}
		}

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
		}

		// check for invalid metakeys
		List<String> validMetakeys = SecurityEngineUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityEngineUtils.updateEngineMetadata(engineId, metadata);
		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);
		UserAuditTrailUtils.recordEngineLifecycle(this.insight.getUser(), "ENGINE_UPDATE",
				engineType == null ? "ENGINE" : engineType.name(), engineId,
				SecurityEngineUtils.getEngineDisplayNameForId(engineId),
				Map.of("field", "metadata", "metadataKeys", List.copyOf(metadata.keySet()), "source", "load"));
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the engine"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Define metadata on an engine through a JSON file";
	}

}
