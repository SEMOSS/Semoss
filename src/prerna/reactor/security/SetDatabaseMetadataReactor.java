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

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.UploadInputUtility;

@Deprecated
public class SetDatabaseMetadataReactor extends AbstractSetMetadataReactor {

	@Deprecated
	public SetDatabaseMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), META,
				ReactorKeysEnum.JSON_CLEANUP.getKey() };
	}

	@Deprecated
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to edit");
		}

		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityEngineUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityEngineUtils.updateEngineMetadata(databaseId, metadata);
		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(databaseId);
		UserAuditTrailUtils.recordEngineLifecycle(this.insight.getUser(), "ENGINE_UPDATE",
				engineType == null ? "DATABASE" : engineType.name(), databaseId,
				SecurityEngineUtils.getEngineDisplayNameForId(databaseId),
				Map.of("field", "metadata", "metadataKeys", List.copyOf(metadata.keySet())));
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the database"));
		return noun;
	}

	@Deprecated
	@Override
	public String getReactorDescription() {
		return "Define metadata on a datasource";
	}

	@Deprecated
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the database. The list of values will determine the order that is defined for field";
		} else if (key.equals(ReactorKeysEnum.JSON_CLEANUP.getKey())) {
			return "Legacy compatibility flag for older clients that sent escaped JSON strings. Modern clients should not set this.";
		}
		return super.getDescriptionForKey(key);
	}

}
