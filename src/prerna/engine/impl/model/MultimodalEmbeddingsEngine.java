package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;

public class MultimodalEmbeddingsEngine extends AbstractRESTModelEngine {

    private static final Logger classLogger = LogManager.getLogger(MultimodalEmbeddingsEngine.class);

    private static final String ENDPOINT = "ENDPOINT";
    private static final String BATCH_SIZE = "BATCH_SIZE";

    private int batchSize;
    private String endpoint;

    @Override
    public void open(Properties smssProp) throws Exception {
        super.open(smssProp);

        this.endpoint = this.smssProp.getProperty(ENDPOINT);
        if (this.endpoint == null || (this.endpoint = this.endpoint.trim()).isEmpty()) {
            throw new IllegalArgumentException("This model requires a valid value for " + ENDPOINT);
        }

        this.batchSize = 32;
        String batchSizeStr = this.smssProp.getProperty(BATCH_SIZE);
        if (batchSizeStr != null && !(batchSizeStr = batchSizeStr.trim()).isEmpty()) {
            try {
                this.batchSize = Integer.parseInt(batchSizeStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid value for " + BATCH_SIZE + ". Must be an integer but found " + batchSizeStr
                );
            }
        }
    }

    @Override
    protected EmbeddingsModelEngineResponse multimodalEmbeddingsCall(
        List<Map<String, Object>> inputsToEmbed,
        Insight insight,
        Map<String, Object> parameters
    ) {
        classLogger.debug("Handling Multimodal Embeddings Request with {} inputs", inputsToEmbed.size());

        // Validate each input has required "type" and "content" fields
        for (Map<String, Object> input : inputsToEmbed) {
            if (!input.containsKey("type") || !input.containsKey("content")) {
                throw new IllegalArgumentException(
                    "Each multimodal input must contain 'type' and 'content' keys"
                );
            }
            String type = (String) input.get("type");
            if (!List.of("text", "image", "video", "audio").contains(type)) {
                throw new IllegalArgumentException(
                    "Unsupported modality type: " + type + ". Supported: text, image, video, audio"
                );
            }
        }
        
	    // Delegate API call to Python embedder;
	    // Java handles batching and request/response flow
        List<List<Double>> embeddings = new ArrayList<>();
        
        List<List<Map<String, Object>>> inputBatches = new ArrayList<>();
        for (int i = 0; i < inputsToEmbed.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, inputsToEmbed.size());
            inputBatches.add(inputsToEmbed.subList(i, endIndex));
        }
        
        for (List<Map<String, Object>> batch : inputBatches) {
            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("inputs", batch);
            // Optional parameters (e.g., task_type, output_dimensionality)
            if (parameters != null && !parameters.isEmpty()) {
                bodyMap.putAll(parameters);
            }
            String output = HttpHelperUtility.postRequestStringBody(this.endpoint, null, new Gson().toJson(bodyMap),
					ContentType.APPLICATION_JSON, null, null, null);

			List<List<Double>> outputParsed = new Gson().fromJson(output, new TypeToken<List<List<Double>>>() {
			}.getType());
			embeddings.addAll(outputParsed);
		}

		EmbeddingsModelEngineResponse embeddingsResponse = new EmbeddingsModelEngineResponse(embeddings, 0, 0);

		return embeddingsResponse;

        //throw new UnsupportedOperationException("Provider-specific implementation required");
    }

    @Override
    public EmbeddingsModelEngineResponse embeddingsCall(
        List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters
    ) {
        // Delegate text-only calls to multimodal with type="text"
        List<Map<String, Object>> multimodalInputs = new ArrayList<>();
        for (String s : stringsToEmbed) {
            Map<String, Object> input = new HashMap<>();
            input.put("type", "text");
            input.put("content", s);
            multimodalInputs.add(input);
        }
        return multimodalEmbeddingsCall(multimodalInputs, insight, parameters);
    }

    @Override
    public EmbeddingsModelEngineResponse imageEmbeddingsCall(
        List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters
    ) {
        // Delegate image-only calls to multimodal with type="image"
        List<Map<String, Object>> multimodalInputs = new ArrayList<>();
        for (String img : imagesToEmbed) {
            Map<String, Object> input = new HashMap<>();
            input.put("type", "image");
            input.put("content", img);
            multimodalInputs.add(input);
        }
        return multimodalEmbeddingsCall(multimodalInputs, insight, parameters);
    }

    @Override
    protected AskModelEngineResponse askCall(
        String question, Object fullPrompt, String context,
        Insight insight, String roomId, Map<String, Object> parameters
    ) {
        return new AskStringModelEngineResponse(
            "This model does not support text generation.", 0, 0
        );
    }

    @Override
    public ModelTypeEnum getModelType() {
        return ModelTypeEnum.MULTIMODAL_EMBEDDINGS;
    }

    @Override
    protected void resetAfterTimeout() {
        // nothing to reset
    }
}