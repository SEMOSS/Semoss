/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskTTSModelEngineResponse;
import prerna.om.Insight;

public class KServeTTSEngine extends AbstractRemoteModelEngine {

	private static final Logger classLogger = LogManager.getLogger(KServeTTSEngine.class);

	@Override
	public AskTTSModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			Map<String, Object> parameters) {
		classLogger.debug("Handling KServeTTS Request for text: {}", question);

		JSONObject payload = new JSONObject();

		payload.put("text", question);

		if (parameters != null) {
			if (parameters.containsKey("voice")) {
				String voice = (String) parameters.get("voice");
				payload.put("voice", voice);
				classLogger.debug("Using voice: {}", voice);
			}

			if (parameters.containsKey("speed")) {
				String speed = (String) parameters.get("speed");
				payload.put("speed", speed);
				classLogger.debug("Using speed: {}", speed);
			}
		}

		try {
			JSONObject modelResponse = makeModelRequest(payload);
			if (modelResponse != null) {
				AskTTSModelEngineResponse ttsResponse = AskTTSModelEngineResponse.getKServeTTSResponse(modelResponse);

				classLogger.debug("TTS response received - Format: {}, Duration: {}, Voice: {}",
						ttsResponse.getAudioFormat(), ttsResponse.getDuration(), ttsResponse.getVoice());

				if (ttsResponse.hasError()) {
					classLogger.error("TTS model returned error: {}", ttsResponse.getErrorMessage());
				}

				return ttsResponse;
			} else {
				classLogger.error("Received null response from TTS model");
				Map<String, Object> errorResponse = new HashMap<>();
				errorResponse.put("status", "error");
				errorResponse.put("message", "Null response from TTS model");
				return new AskTTSModelEngineResponse(errorResponse, 0, 0);
			}
		} catch (Exception e) {
			classLogger.error("Error processing TTS request: {}", e.getMessage(), e);
			Map<String, Object> errorResponse = new HashMap<>();
			errorResponse.put("status", "error");
			errorResponse.put("message", "Error processing TTS request: " + e.getMessage());
			return new AskTTSModelEngineResponse(errorResponse, 0, 0);
		}
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.KSERVE_TTS;
	}
}
