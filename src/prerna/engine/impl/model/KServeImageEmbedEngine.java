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
