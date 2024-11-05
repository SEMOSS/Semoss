package prerna.engine.impl.model;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.RemoteModelStateEnum;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.config.RequestConfig;
import org.json.JSONObject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;



import prerna.engine.api.ModelTypeEnum;

public class NEREngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	public String predict(String text, List<String> entities, List<String> maskEntities, Insight insight, Map <String, Object> parameters) {
		JSONObject payload = new JSONObject();
		payload.put("text", text);
		payload.put("labels", entities);
		payload.put("model", this.model);
		
		JSONObject response = this.makeModelRequest(payload);
		
		if (response != null) {
			classLogger.info("Response from model: {}", response.toString());
			return "SUCCESS";
		} else {
			classLogger.error("Error making model request");
			return "ERROR";
        }
		
	}
	
	
	// Check if the model is active and if not attempt to start the model
	public String getModelStatus() {
		try {
		RemoteModelStateEnum currentState = getCurrentModelState();
        classLogger.info("Current state for engineId {} is: {}", this.engineId, currentState);
        // Turn it into a string
		String modelState = currentState.name();
        return modelState;
		} catch (Exception e) {
			classLogger.error("Error getting model state", e);
			return "ERROR";
		}
		
	}
	
	
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.NER;
	}

}
