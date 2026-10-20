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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.Utility;

/**
 * Overwrite a model engine's stored metadata with the meta/model.json catalog
 * entry it corresponds to. This backs two actions on the model settings screen:
 * "reset to defaults", which reapplies the entry already associated with the
 * engine (falling back to its model id), and "match to a catalog entry", which
 * passes a catalogKey to associate the engine with an entry picked by hand and
 * apply it in one step.
 * <p>
 * Every property the catalog defines for the entry replaces the stored value.
 * Properties the catalog cannot speak to - the serving provider, built-in
 * tools, and description - are left as they are. Pass dryRun to get the list
 * of fields that would change without writing anything, which is what the
 * settings screen shows in its confirmation dialog.
 */
public class ApplyModelCatalogMetadataReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ApplyModelCatalogMetadataReactor.class);

	static final String CATALOG_KEY = "catalogKey";
	static final String DRY_RUN_KEY = "dryRun";

	public ApplyModelCatalogMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), CATALOG_KEY, DRY_RUN_KEY };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must input a model engine id");
		}

		User user = this.insight.getUser();
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Model engine does not exist or user does not have access to edit it");
		}
		if (SecurityEngineUtils.getEngineType(engineId) != IEngine.CATALOG_TYPE.MODEL) {
			throw new IllegalArgumentException("Engine is not a model engine");
		}

		String catalogKey = resolveCatalogKey();
		boolean dryRun = Boolean.parseBoolean(this.keyValue.get(DRY_RUN_KEY) + "");

		Map<String, Object> result = SecurityModelMetadataUtils.applyCatalogMetadata(engineId, catalogKey, dryRun);
		String status = String.valueOf(result.get("status"));
		if ("NO_MODEL_ID".equals(status)) {
			throw new IllegalArgumentException("The engine has no model id to look up in the model catalog");
		}
		if ("NO_CATALOG_ENTRY".equals(status)) {
			throw new IllegalArgumentException("The model catalog has no entry for this model. "
					+ "Pass a catalogKey to associate it with an entry first");
		}
		if ("ERROR".equals(status)) {
			throw new IllegalStateException("Failed to apply the model catalog metadata");
		}

		String reloadWarning = null;
		if ("UPDATED".equals(status) && Utility.engineLoaded(engineId)) {
			ReentrantLock lock = EngineSyncUtility.getEngineLock(engineId);
			lock.lock();
			try {
				IModelEngine modelEngine = Utility.getModel(engineId);
				modelEngine.close();
				modelEngine.open(modelEngine.getSmssFilePath());
			} catch (Exception e) {
				classLogger.error("Failed to reload model engine '{}' after applying the catalog metadata",
						Utility.cleanLogString(engineId), e);
				reloadWarning = "The catalog metadata was applied but the engine could not be reloaded. "
						+ "The new settings will not take effect until the engine is reloaded. Detailed message = "
						+ e.getMessage();
			} finally {
				lock.unlock();
			}
		}

		NounMetadata noun = new NounMetadata(result, PixelDataType.MAP);
		if (reloadWarning != null) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage(reloadWarning));
		} else if (!dryRun) {
			String message = "NO_CHANGE".equals(status) ? "The model settings already match the catalog entry"
					: "Successfully applied the catalog metadata to the model settings";
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(message));
		}
		return noun;
	}

	/**
	 * The exact catalog key the requested entry resolves to, or null when the
	 * caller did not name one. The key is resolved the same way a model id is, so
	 * a provider id that maps onto a catalog key is accepted and stored in its
	 * canonical form.
	 */
	private String resolveCatalogKey() {
		String catalogKey = this.keyValue.get(CATALOG_KEY);
		if (catalogKey == null || catalogKey.trim().isEmpty()) {
			return null;
		}
		catalogKey = catalogKey.trim();

		Path metadataFile = StaticModelMetadataCatalog.getMetadataFile();
		if (!Files.isRegularFile(metadataFile)) {
			throw new IllegalStateException("The model catalog file is not available on this install");
		}
		String resolvedKey = StaticModelMetadataCatalog.findModelKey(metadataFile, catalogKey);
		if (resolvedKey == null) {
			throw new IllegalArgumentException("The model catalog has no entry '" + catalogKey + "'");
		}
		return resolvedKey;
	}

	@Override
	public String getReactorDescription() {
		return "Overwrites a model engine's stored metadata with its meta/model.json catalog entry, "
				+ "optionally associating the engine with a different entry first";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CATALOG_KEY)) {
			return "Optional catalog entry to associate with the engine and apply. "
					+ "Defaults to the entry already associated, falling back to the engine's model id";
		}
		if (key.equals(DRY_RUN_KEY)) {
			return "Report the fields that would change without writing anything. Default false";
		}
		return super.getDescriptionForKey(key);
	}
}
