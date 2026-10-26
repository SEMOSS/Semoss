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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskImageModelEngineResponse;
import prerna.om.Insight;

public class KServeImageEngine extends AbstractRemoteModelEngine {

	private static final Logger classLogger = LogManager.getLogger(KServeImageEngine.class);

	@Override
	public AskImageModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> hyperParameters) {
		classLogger.debug("Handling KServeImage Request..");

		JSONObject payload = new JSONObject();

		payload.put("prompt", inputMessage.getFullInputPrompt());

		if (hyperParameters != null) {
			if (hyperParameters.containsKey("negative_prompt")) {
				String negativePrompt = (String) hyperParameters.get("negative_prompt");
				payload.put("negative_prompt", negativePrompt);
			}

			if (hyperParameters.containsKey("height")) {
				Object heightObj = hyperParameters.get("height");
				payload.put("height", heightObj.toString());
			}

			if (hyperParameters.containsKey("width")) {
				Object widthObj = hyperParameters.get("width");
				payload.put("width", widthObj.toString());
			}

			if (hyperParameters.containsKey("num_inference_steps")) {
				Object stepsObj = hyperParameters.get("num_inference_steps");
				payload.put("num_inference_steps", stepsObj.toString());
			}

			if (hyperParameters.containsKey("guidance_scale")) {
				Object scaleObj = hyperParameters.get("guidance_scale");
				payload.put("guidance_scale", scaleObj.toString());
			}

			if (hyperParameters.containsKey("num_images")) {
				Object numImagesObj = hyperParameters.get("num_images");
				payload.put("num_images", numImagesObj.toString());
			}
		}

		try {
			JSONObject modelResponse = makeModelRequest(payload);
			if (modelResponse != null) {
				return AskImageModelEngineResponse.getKServeImageResponse(modelResponse);
			} else {
				classLogger.error("Received null response from model");
				Map<String, Object> responseMap = new HashMap<>();
				responseMap.put("output", "Error creating image.");
				return new AskImageModelEngineResponse(responseMap, 0, 0);
			}
		} catch (Exception e) {
			classLogger.error("Error making model request", e);
			Map<String, Object> responseMap = new HashMap<>();
			responseMap.put("output", "Error creating image.");
			return new AskImageModelEngineResponse(responseMap, 0, 0);
		}
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_IMAGE;
	}

}
