package prerna.engine.impl.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.http.entity.ContentType;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.security.AbstractHttpHelper;
import prerna.util.Utility;

public class TextEmbeddingsEngine extends AbstractModelEngine {

	private static final String ENDPOINT = "ENDPOINT";
	private static final String BATCH_SIZE = "BATCH_SIZE";
	
	private int batchSize;
	private String endpoint;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		if (!this.smssProp.containsKey(ENDPOINT)) {
			throw new IllegalArgumentException("This model requires an endpoint.");
		}
		endpoint = this.smssProp.getProperty(ENDPOINT);
		
		batchSize = 32;
		if (this.smssProp.containsKey(BATCH_SIZE)) {
			batchSize = Integer.valueOf(this.smssProp.getProperty(ENDPOINT));
		}
	}

	@Override
	public Map<String, Object> askQuestion(String question, String context, Insight insight,
			Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		HashMap<String, Object> response = new HashMap<String, Object>();
		response.put(RESPONSE, "This model does not support text generation.");
		response.put(NUMBER_OF_TOKENS_IN_PROMPT, 0.0);
		response.put(NUMBER_OF_TOKENS_IN_RESPONSE, 0.0);
		
		return response;
	}

	@Override
	public Object embeddings(List<String> stringsToEncode, Insight insight, Map <String, Object> parameters) {
		List<List<Double>> embeddings = new ArrayList<>();
		
		List<List<String>> sentenceSublists = new ArrayList<>();

		for (int i = 0; i < stringsToEncode.size(); i += batchSize) {
		    int endIndex = Math.min(i +  batchSize, stringsToEncode.size());
		    List<String> sublist = stringsToEncode.subList(i, endIndex);
		    sentenceSublists.add(sublist);
		}

		LocalDateTime inputTime = LocalDateTime.now();
		for(List<String> sublist : sentenceSublists) {
			Map<String, Object> bodyMap = new HashMap<>();
			bodyMap.put("inputs", sublist);
			bodyMap.put("truncate", true);
			String output = AbstractHttpHelper.postRequestStringBody(this.endpoint, null, new Gson().toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null);
			
			System.out.println(output);
			List<List<Double>> outputParsed = new Gson().fromJson(output, new TypeToken<List<List<Double>>>() {}.getType());
			embeddings.addAll(outputParsed);
		}
		LocalDateTime outputTime = LocalDateTime.now();
		
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put(RESPONSE, embeddings);
		output.put(NUMBER_OF_TOKENS_IN_PROMPT, 0.0);
		output.put(NUMBER_OF_TOKENS_IN_RESPONSE, 0.0);
		
		if (Utility.isModelInferenceLogsEnabled()) {
			String messageId = UUID.randomUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId, 
					"embeddings", 
					this, 
					insight, 
					null,
					"",
					getTokens(output.get(NUMBER_OF_TOKENS_IN_PROMPT)),
					inputTime, 
					"",
					getTokens(output.get(NUMBER_OF_TOKENS_IN_RESPONSE)),
					outputTime
			));
			inferenceRecorder.start();
		}

		return output;
	}
	
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_EMBEDDINGS;
	}
}