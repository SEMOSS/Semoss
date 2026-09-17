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
package prerna.engine.impl.model.openai;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.reactor.security.MyEnginesReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Helper class for serving the OpenAI {@code /v1/models} endpoints out of the
 * model engines the user can view.
 */
public final class OpenAIModelsHelper {

	/**
	 * List every model engine the user can view, in the OpenAI models list shape:
	 * {@code {object: "list", data: [...]}}.
	 *
	 * @param user the user whose visible models are listed
	 * @return the OpenAI-shaped model list payload
	 */
	public static Map<String, Object> listModels(User user) {
		Map<String, Object> returnObject = new HashMap<>();
		returnObject.put("object", "list");
		returnObject.put("data", queryModels(user, null));
		return returnObject;
	}

	/**
	 * Retrieve a single model engine in the OpenAI model shape.
	 *
	 * @param user    the user requesting the model
	 * @param modelId the engine id being retrieved
	 * @return the OpenAI-shaped model object, or {@code null} if the model does not
	 *         exist or the user cannot view it
	 */
	public static Map<String, Object> retrieveModel(User user, String modelId) {
		List<Map<String, Object>> models = queryModels(user, modelId);
		if (models == null || models.isEmpty()) {
			return null;
		}
		return models.get(0);
	}

	/**
	 * Run {@code MyEngines} for the model catalog type and convert the output into
	 * the OpenAI model shape.
	 *
	 * @param user    the user whose visible models are queried
	 * @param modelId a specific engine id to filter on, or {@code null} for all
	 * @return the OpenAI-shaped model objects
	 */
	private static List<Map<String, Object>> queryModels(User user, String modelId) {
		MyEnginesReactor reactor = new MyEnginesReactor();
		reactor.In();
		Insight temp = new Insight();
		temp.setUser(user);
		reactor.setInsight(temp);
		if (modelId != null) {
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(modelId, PixelDataType.CONST_STRING));
			reactor.getNounStore().addNoun(ReactorKeysEnum.ENGINE.getKey(), struct);
		}
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(IEngine.CATALOG_TYPE.MODEL.name(), PixelDataType.CONST_STRING));
			reactor.getNounStore().addNoun(ReactorKeysEnum.ENGINE_TYPE.getKey(), struct);
		}
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(true, PixelDataType.BOOLEAN));
			reactor.getNounStore().addNoun(ReactorKeysEnum.NO_META.getKey(), struct);
		}
		{
			GenRowStruct struct = new GenRowStruct();
			struct.add(new NounMetadata(false, PixelDataType.BOOLEAN));
			reactor.getNounStore().addNoun(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(), struct);
		}

		return processModelList(reactor.execute());
	}

	/**
	 * Process the MyEngines output format to OpenAi format
	 *
	 * @param outputNoun the noun returned by {@code MyEnginesReactor}
	 * @return the OpenAI-shaped model objects
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> processModelList(NounMetadata outputNoun) {
		List<Map<String, Object>> enginesList = (List<Map<String, Object>>) outputNoun.getValue();
		// we will convert our object to the openai spec
		List<Map<String, Object>> openAiResponse = new ArrayList<>(enginesList.size());
		for (Map<String, Object> engines : enginesList) {
			Map<String, Object> newMap = new HashMap<>();
			newMap.put("object", "model");
			newMap.put("id", engines.get("database_id"));
			newMap.put("alias", engines.get("database_name"));
			newMap.put("owned_by", engines.get("database_created_by"));
			SemossDate dateCreated = (SemossDate) engines.get("database_date_created");
			if (dateCreated != null) {
				ZonedDateTime zdt = dateCreated.getZonedDateTime();
				if (zdt != null) {
					newMap.put("created", zdt.toEpochSecond());
				}
			}
			openAiResponse.add(newMap);
		}
		return openAiResponse;
	}

	private OpenAIModelsHelper() {
	}
}
