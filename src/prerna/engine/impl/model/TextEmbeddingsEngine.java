package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class TextEmbeddingsEngine extends AbstractRESTModelEngine {

	private static final Logger classLogger = LogManager.getLogger(TextEmbeddingsEngine.class);

	private static final String ENDPOINT = "ENDPOINT";
	private static final String BATCH_SIZE = "BATCH_SIZE";
	
	private int batchSize;
	private String endpoint;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.endpoint = this.smssProp.getProperty(ENDPOINT);
		if(this.endpoint == null || (this.endpoint=this.endpoint.trim()).isEmpty()) {
			throw new IllegalArgumentException("This model requires a valid value for " + ENDPOINT);
		}
//		Utility.checkIfValidDomain(this.endpoint);
		
		this.batchSize = 32;
		String batchSizeStr = null;
		try {
			batchSizeStr = this.smssProp.getProperty(BATCH_SIZE);
			if(batchSizeStr != null && !(batchSizeStr=batchSizeStr.trim()).isEmpty()) {
				this.batchSize = Integer.valueOf(batchSizeStr);
			}
		} catch(NumberFormatException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("The SMSS has an invalid value for " + BATCH_SIZE +". Must be an integer but found " + batchSizeStr);
		}
	}

	@Override
	public EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEncode, Insight insight, Map <String, Object> parameters) {
		List<List<Double>> embeddings = new ArrayList<>();
		
		List<List<String>> sentenceSublists = new ArrayList<>();

		for (int i = 0; i < stringsToEncode.size(); i += batchSize) {
		    int endIndex = Math.min(i +  batchSize, stringsToEncode.size());
		    List<String> sublist = stringsToEncode.subList(i, endIndex);
		    sentenceSublists.add(sublist);
		}
				
		for(List<String> sublist : sentenceSublists) {
			Map<String, Object> bodyMap = new HashMap<>();
			bodyMap.put("inputs", sublist);
			bodyMap.put("truncate", true);
			String output = HttpHelperUtility.postRequestStringBody(this.endpoint, null, new Gson().toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null);
			
			List<List<Double>> outputParsed = new Gson().fromJson(output, new TypeToken<List<List<Double>>>() {}.getType());
			embeddings.addAll(outputParsed);
		}
		
		
		EmbeddingsModelEngineResponse embeddingsResponse = new EmbeddingsModelEngineResponse(embeddings, 0, 0);

		return embeddingsResponse;
	}
	
	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> parameters) {
		return new AskModelEngineResponse("This model does not support text generation.", 0, 0);
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		return "This model does have an model method defined.";
	}
	

	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_EMBEDDINGS;
	}

	@Override
	protected void resetAfterTimeout() {
		// nothing to reset currently
	}
}