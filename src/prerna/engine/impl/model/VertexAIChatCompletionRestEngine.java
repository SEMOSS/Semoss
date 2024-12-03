package prerna.engine.impl.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.gson.annotations.Expose;

import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.util.insight.TextHelper;

public class VertexAIChatCompletionRestEngine extends OpenAiChatCompletionRestEngine {

    private GoogleCredentials credentials = null;
	private static final Logger logger = LogManager.getLogger(VertexAIChatCompletionRestEngine.class);


    @Override
    protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> parameters) {
        // Refresh the access token
        String accessToken = getVertexAccessToken();
        if (accessToken != null) {
            this.headersMap.put("Authorization", "Bearer " + accessToken);
        }
        // add safety_settings to parameters if present
        String safetySettings = this.smssProp.getProperty("SAFETY_SETTINGS");
        if (safetySettings != null && !safetySettings.isEmpty())
        {
        	parameters.put("safety_settings", getVertexAiSafetySettings(safetySettings));		
        }
        
        // Call the superclass's askCall method
        return super.askCall(question, fullPrompt, context, insight, parameters);
    }

    private String getVertexAccessToken() {
        try {
            // Initialize credentials if they are not already initialized
            if (credentials == null) {
                String serviceAccountKeyFile = this.smssProp.getProperty("SERVICE_ACCOUNT_KEY_FILE");
                if (serviceAccountKeyFile == null || serviceAccountKeyFile.trim().isEmpty()) {
                    throw new IllegalArgumentException("Service account key file path is not provided.");
                }

                credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(Paths.get(serviceAccountKeyFile)))
                        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            // Refresh credentials if expired
            credentials.refreshIfExpired();
            return credentials.getAccessToken().getTokenValue();

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private List<SafetySetting> getVertexAiSafetySettings(String safetyParam) {
    	List<SafetySetting> safetySettings = new ArrayList<>();
    	try {
    		 
    		Map<String, String> map = TextHelper.convertJsonStringToHashMap(safetyParam);
   	     	for (Map.Entry<String,String> entry : map.entrySet()) 
   	     	{
   	     		SafetySetting safetySetting = new SafetySetting();
   	     		safetySetting.setCategory(entry.getKey());
   	     		safetySetting.setThresold(entry.getValue());
   	     		safetySettings.add(safetySetting);
   	     	}
                   
    		return safetySettings;
    	}
    	catch (Exception e) {
			logger.warn("Unable to set safety_settings", e);
            return null;
    		
    	}
    	
    }
	protected class SafetySetting{
		@Expose
		String category;
		@Expose
		String thresold;
		
		public void setCategory(String category) {
			this.category = category;
		}
		
		public void setThresold(String thresold) {
			this.thresold = thresold;
		}
		
		public String getCategory() {
			return this.category;
		}
		public String setThresold() {
			return this.thresold;
		}
	}
}