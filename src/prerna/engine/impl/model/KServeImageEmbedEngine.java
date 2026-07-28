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
package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;

public class KServeImageEmbedEngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(KServeImageEmbedEngine.class);
	
	@Override
	public EmbeddingsModelEngineResponse embeddingsCall(List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters) {
		classLogger.debug("Handling KServeImageEmbed Request..");
		
		JSONObject payload = new JSONObject();
		
		payload.put("image", imagesToEmbed);
		
	    if (parameters != null && parameters.containsKey("pooling_strategy")) {
	        String poolingStrategy = (String) parameters.get("pooling_strategy");
	        payload.put("pooling_strategy", poolingStrategy);
	    }
	    
		classLogger.debug("KServeVision embeddingsCall payload: {}", payload.toString(2));

		try {
            JSONObject modelResponse = makeModelRequest(payload);
            if (modelResponse != null) {
                return EmbeddingsModelEngineResponse.fromJson(modelResponse);
            } else {
                classLogger.error("Received null response from model");
                List<List<Double>> emptyEmbeddings = new ArrayList<>();
                return new EmbeddingsModelEngineResponse(emptyEmbeddings, 0, 0);
            }
		} catch (Exception e) {
            classLogger.error("Error making model request", e);
            List<List<Double>> emptyEmbeddings = new ArrayList<>();
            return new EmbeddingsModelEngineResponse(emptyEmbeddings, 0, 0);
        }
	}
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_IMAGE_EMBED;
	}

}
