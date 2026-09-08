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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.om.Insight;

public class KServeVisionEngine extends AbstractRemoteModelEngine {

	private static final Logger classLogger = LogManager.getLogger(KServeVisionEngine.class);

	@Override
	public AskStringModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> hyperParameters) {
		classLogger.debug("Handling KServeVision Request..");

		JSONObject payload = new JSONObject();

		// Vision models require images in the payload..
		if (hyperParameters != null && hyperParameters.containsKey("image_url")) {
			String imageUrl = (String) hyperParameters.get("image_url");
			payload.put("image", imageUrl);
		} else {
			classLogger.warn("No image_url found in hyperParameters");
			AskStringModelEngineResponse response = new AskStringModelEngineResponse(
					"Please provide an image parameter.", 0, 0);
			return response;
		}

		payload.put("text", inputMessage.getFullInputPrompt());

		classLogger.debug("KServeVision askCall payload: {}", payload.toString(2));

		try {
			JSONObject modelResponse = makeModelRequest(payload);
			if (modelResponse != null) {
				return AskStringModelEngineResponse.fromJson(modelResponse);
			} else {
				classLogger.error("Received null response from model");
				return new AskStringModelEngineResponse("Error processing image.", 0, 0);
			}
		} catch (Exception e) {
			classLogger.error("Error making model request", e);
			return new AskStringModelEngineResponse("An error occurred while processing the request: " + e.getMessage(),
					0, 10);
		}
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_VISION;
	}

}
