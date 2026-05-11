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
package prerna.reactor.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class NERReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(NERReactor.class);

	public NERReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PROMPT.getKey(),
				ReactorKeysEnum.ENTITIES.getKey(), ReactorKeysEnum.MASK_ENTITIES.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		String prompt = this.keyValue.get(this.keysToGet[1]);
		List<String> entities = this.getListInput("entities");
		List<String> maskEntities = this.getListInput("maskEntities");

		Map<String, Object> paramMap = getMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// CASTING TO CORRECT ENGINE.. NER is not abstracted
		IModelEngine targetModel = Utility.getModel(engineId);
		NEREngine targetEngine = (NEREngine) targetModel;

		NerModelEngineResponse output = targetEngine.predict(prompt, entities, maskEntities, this.insight, paramMap);

		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);

	}

	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(keysToGet[4]);
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

	private List<String> getListInput(String noun) {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getGenRowStruct(noun);
		if (colGRS != null) {
			for (int i = 0; i < colGRS.size(); i++) {
				String stringValue = colGRS.get(i).toString();
				colInputs.add(stringValue);
			}
		}
		return colInputs;
	}

}
