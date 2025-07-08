package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.responses.AskTTSModelEngineResponse;
import prerna.om.Insight;

public class KServeTTSEngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(KServeTTSEngine.class);

	@Override
	public AskTTSModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> parameters) {
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
					ttsResponse.getAudioFormat(), 
					ttsResponse.getDuration(), 
					ttsResponse.getVoice());
				
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
}