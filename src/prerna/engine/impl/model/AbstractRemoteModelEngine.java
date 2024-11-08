package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.config.RequestConfig;
import org.json.JSONObject;
import java.nio.charset.StandardCharsets;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Settings;
import prerna.engine.api.RemoteModelStateEnum;
import prerna.cluster.util.RemoteClientServerZK;

 /**
 * This is a class used to be extended by models running on a RemoteClientServer ONLY.
 * It contains methods for deploying the model to the cluster and making HTTP requests to the model.
 * See https://github.com/SEMOSS/remote-client-server for RemoteClientServer implementation.
 * See https://github.com/SEMOSS/kubernetes-model-scaler for Kubernetes model scaling.
 */

public class AbstractRemoteModelEngine extends AbstractModelEngine {
    private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
    
    protected String model;
    private String deployerEndpoint;
    private RemoteClientServerZK zkClient;
    
    @Override
    public void open(Properties smssProp) throws Exception {
        super.open(smssProp);
        
        if(this.smssProp.containsKey(Settings.DEPLOYER_ENDPOINT)) {
            this.deployerEndpoint = this.smssProp.getProperty(Settings.DEPLOYER_ENDPOINT);
        } else {
            throw new IllegalArgumentException("Deployer endpoint is not defined in SMSS file.");
        }
        
        if (this.smssProp.containsKey(Settings.MODEL)) {
            this.model = this.smssProp.getProperty(Settings.MODEL).trim();
        } else {
            throw new IllegalArgumentException("Model is not defined in SMSS file.");
        }
        
        this.zkClient = RemoteClientServerZK.getInstance();
    }
    
    protected boolean initiateAndWaitForDeployment(long timeoutMs) throws Exception {
        // First check if the model is already active
        if (zkClient.isModelActive(this.engineId)) {
            classLogger.info("Model {} is already active", this.engineId);
            return true;
        }
        
        if (zkClient.isModelWarming(this.engineId)) {
            classLogger.info("Model {} is already warming, waiting for activation", this.engineId);
            return zkClient.waitForModelActive(this.engineId, timeoutMs);
        }
        
        // Initiate deployment through HTTP request
        CompletableFuture<Boolean> deploymentFuture = CompletableFuture.supplyAsync(() -> {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost httpPost = new HttpPost(this.deployerEndpoint);
                
                JSONObject payload = new JSONObject();
                payload.put("model_id", this.engineId);
                payload.put("model", this.model);
                
                StringEntity entity = new StringEntity(
                    payload.toString(),
                    ContentType.APPLICATION_JSON
                );
                httpPost.setEntity(entity);

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    
                    if (statusCode >= 200 && statusCode < 300) {
                        classLogger.info("Successfully initiated deployment for model ID: {}", this.engineId);
                        return true;
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        classLogger.error("Failed to deploy model ID: {}. Status code: {}, Response: {}", 
                            this.engineId, statusCode, responseBody);
                        return false;
                    }
                }
            } catch (Exception e) {
                classLogger.error("Error deploying model ID: " + this.engineId, e);
                return false;
            }
        });
        
        // Wait for deployment initiation to complete
        if (!deploymentFuture.get(30, TimeUnit.SECONDS)) {
            classLogger.error("Failed to initiate deployment for model {}", this.engineId);
            return false;
        }
        
        return zkClient.waitForModelActive(this.engineId, timeoutMs);
    }
    
    protected JSONObject makeModelRequest(JSONObject requestPayload) throws Exception {
        // Get current state and handle warming/cold states
        RemoteModelStateEnum currentState = zkClient.getModelState(this.engineId);
        
        // If cold, try to deploy
        if (currentState == RemoteModelStateEnum.COLD) {
            boolean deployed = initiateAndWaitForDeployment(120000); // 2 minute timeout for deployment
            if (!deployed) {
                classLogger.error("Failed to deploy model {}", this.engineId);
                return null;
            }
            currentState = zkClient.getModelState(this.engineId);
        }
        
        // Always wait for active state, whether it started as WARMING or just became WARMING after deployment
        if (currentState == RemoteModelStateEnum.WARMING) {
            classLogger.info("Model {} is warming, waiting for activation...", this.engineId);
            boolean becameActive = zkClient.waitForModelActive(this.engineId, 300000); // 5 minute timeout
            if (!becameActive) {
                classLogger.error("Model {} failed to become active after warming", this.engineId);
                return null;
            }
            currentState = zkClient.getModelState(this.engineId);
        }

        // If not active after handling warming/cold states, return null
        if (currentState != RemoteModelStateEnum.ACTIVE) {
            classLogger.error("Model {} is not active. Current state: {}", this.engineId, currentState);
            return null;
        }

        String clusterIp = zkClient.getModelClusterIp(this.engineId);
        if (clusterIp == null) {
            classLogger.error("No cluster IP available for model {}", this.engineId);
            return null;
        }

        // Make the actual HTTP request
        return makeHttpRequest(clusterIp, requestPayload);
    }
    
    private JSONObject makeHttpRequest(String clusterIp, JSONObject requestPayload) {
        String url = "http://localhost:8888/api/generate"; // TEMP FOR LOCAL DEVELOPMENT
        // String url = String.format("http://%s:8888/api/generate", clusterIp);
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(30000)
                .setSocketTimeout(900000)
                .build();

        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
                
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Accept", "text/event-stream");
            httpPost.setHeader("Content-Type", "application/json");
            
            StringEntity entity = new StringEntity(requestPayload.toString(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    classLogger.error("Request failed with status code: {}", statusCode);
                    return null;
                }

                HttpEntity responseEntity = response.getEntity();
                if (responseEntity == null) {
                    classLogger.error("No response entity received");
                    return null;
                }

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(responseEntity.getContent(), StandardCharsets.UTF_8))) {
                    
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("data:")) {
                            String dataStr = line.substring(5).trim();
                            if (!dataStr.isEmpty()) {
                                JSONObject data = new JSONObject(dataStr);
                                String status = data.optString("status");
                                String message = data.optString("message");

                                classLogger.info("Status Update: {} - {}", status, message);

                                switch (status) {
                                    case "complete":
                                        return data;
                                    case "error":
                                    case "cancelled":
                                    case "timeout":
                                        classLogger.error("Job {}: {}", status, message);
                                        return null;
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            classLogger.error("Error making HTTP request", e);
        }
        return null;
    }

	@Override
	public ModelTypeEnum getModelType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			Map<String, Object> hyperParameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}
}
