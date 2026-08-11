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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *******************************************************************************/
package prerna.reactor.security;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateModelMetadataReactor extends AbstractReactor {

	public UpdateModelMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
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

		Map<String, Object> updates = this.<String, Object>getGenericMap(ReactorKeysEnum.MAP.getKey(), null);
		if (updates == null) {
			throw new IllegalArgumentException("Must input the model metadata to update");
		}

		SecurityModelMetadataUtils.updateModelMetadata(engineId, updates);
		Map<String, Object> updatedMetadata = SecurityModelMetadataUtils.getModelMetadata(engineId);
		NounMetadata noun = new NounMetadata(updatedMetadata, PixelDataType.CUSTOM_DATA_STRUCTURE);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated the model capabilities"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Updates editable provider, capability, modality, token, reasoning, and built-in tool metadata for a model engine";
	}
}
