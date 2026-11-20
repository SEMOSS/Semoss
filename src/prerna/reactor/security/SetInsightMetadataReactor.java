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

import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightMetadataReactor extends AbstractInsightReactor {

	private static final String META = "meta";

	public SetInsightMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), META,
				ReactorKeysEnum.JSON_CLEANUP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.getProject();
		String insightId = this.getRdbmsId();

		if (!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, insightId)) {
			throw new IllegalArgumentException("Insight does not exist or user does not have access to edit");
		}

		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityInsightUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityInsightUtils.updateInsightMetadata(projectId, insightId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the insight"));
		return noun;
	}

	/**
	 * Get the metadata map while handling encoded input paths.
	 * <p>
	 * Note: {@code jsonCleanup} is a legacy compatibility option for older escaped
	 * payloads and should not be used by modern clients.
	 * 
	 * @return parsed metadata map
	 */
	protected Map<String, Object> getMetaMap() {
		Boolean jsonCleanup = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.JSON_CLEANUP.getKey()) + "");
		GenRowStruct metaGrs = this.store.getGenRowStruct(META);
		return AbstractSetMetadataReactor.parseMetaMap(metaGrs, this.curRow, jsonCleanup);
	}

	@Override
	public String getReactorDescription() {
		return "Define metadata for an insight";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the insight. The list of values will determine the order that is defined for field";
		} else if (key.equals(ReactorKeysEnum.JSON_CLEANUP.getKey())) {
			return "Legacy compatibility flag for older clients that sent escaped JSON strings. Modern clients should not set this.";
		}
		return super.getDescriptionForKey(key);
	}
}
