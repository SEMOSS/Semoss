package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.cluster.util.RemoteClientServerZK;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.api.RemoteModelStateEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;

/**
 * This is a class used to be extended by models running on a RemoteClientServer ONLY.
 * It contains methods for deploying the model to the cluster and making HTTP requests to the model.
 * See https://github.com/SEMOSS/remote-client-server for RemoteClientServer implementation.
 * See https://github.com/SEMOSS/kubernetes-model-scaler for Kubernetes model scaling.
 */

public class AbstractRemoteModelEngine extends AbstractModelEngine {
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);

	protected String model;
	protected String modelRepoId;
	protected String modelType;
	private RemoteClientServerZK zkClient;
	private Boolean devPortFowarding = true;

	private AbstractModelEngine implementingEngineClass = null;

	private final String INIT_PREFIX = "INIT_";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		if (this.smssProp.containsKey(Settings.MODEL)) {
			this.model = this.smssProp.getProperty(Settings.MODEL).trim();
		} else {
			throw new IllegalArgumentException("Model is not defined in SMSS file.");
		}

		if (this.smssProp.containsKey(Settings.MODEL_REPO_ID)) {
			this.modelRepoId = this.smssProp.getProperty(Settings.MODEL_REPO_ID).trim();
		} else {
			throw new IllegalArgumentException("Model Repo ID is not defined in SMSS file.");
		}

		if (this.smssProp.containsKey(Settings.MODEL_TYPE)) {
			this.modelType = this.smssProp.getProperty(Settings.MODEL_TYPE).trim();
		} else {
			throw new IllegalArgumentException("Model Type is not defined in SMSS file.");
		}

		this.zkClient = RemoteClientServerZK.getInstance();

		// THIS EQUALS == INIT_ENGINE_TYPE
		String initEngineTypeKey = INIT_PREFIX+Constants.ENGINE_TYPE;
		String initEngineType = smssProp.getProperty(initEngineTypeKey);

		if (initEngineType != null && !initEngineType.isEmpty()){
			implementingEngineClass = (AbstractModelEngine) Class.forName(initEngineType).newInstance();
			Properties implEngineSmss = new Properties();
			for(Object key : smssProp.keySet()) {
				String keyStr = (String) key;
				if(keyStr.equals(Constants.ENGINE_TYPE)) {
					implEngineSmss.put(Constants.ENGINE_TYPE, initEngineType);
				} else {
					implEngineSmss.put(keyStr, smssProp.getProperty(keyStr));
				}
			}
			String engineId = smssProp.getProperty(Constants.ENGINE);
			implementingEngineClass.open(implEngineSmss);
		}
	}
	
	protected boolean initiateAndWaitForDeployment(long timeoutMs) throws Exception {
	    if (zkClient.isModelActive(this.engineId)) {
	        classLogger.info("Model {} is already active", this.engineId);
	        return true;
	    }

	    if (zkClient.isModelWarming(this.engineId)) {
	        classLogger.info("Model {} is already warming, waiting for activation", this.engineId);
	        return zkClient.waitForModelActive(this.engineId, timeoutMs);
	    }

	    String modelScalerIp = zkClient.getModelScalerIp();
	    if (modelScalerIp == null) {
	        classLogger.error("Unable to get model scaler IP from ZooKeeper");
	        return false;
	    }

	    // Construct the deployment endpoint URL
	    String deploymentUrl;
	    if (devPortFowarding) {
	        deploymentUrl = "http://localhost:8000/api/start";
	    } else {
	        deploymentUrl = String.format("http://%s/api/start", modelScalerIp);
	    }

	    // Deployment request in separate thread
	    CompletableFuture<Void> deploymentFuture = CompletableFuture.runAsync(() -> {
	        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
	            HttpPost httpPost = new HttpPost(deploymentUrl);
	            
	            JSONObject payload = new JSONObject();
	            payload.put("model_id", this.engineId);
	            payload.put("model", this.model);
	            payload.put("model_repo_id", this.modelRepoId);
	            payload.put("model_type", this.modelType);

	            StringEntity entity = new StringEntity(
	                payload.toString(),
	                ContentType.APPLICATION_JSON
	            );
	            httpPost.setEntity(entity);

	            httpClient.execute(httpPost).close();
	        } catch (Exception e) {
	            // I'm not hanging the main thread on this request, I'll just monitor ZK for the model status
	            classLogger.warn("HTTP request to model scaler stilling progress, dropping connection but continuing to check ZooKeeper status", e);
	        }
	    });

	    long startTime = System.currentTimeMillis();
	    long warmingTimeout = Math.min(30000, timeoutMs); // 30 seconds or remaining timeout
	    
	    while (System.currentTimeMillis() - startTime < warmingTimeout) {
	        if (zkClient.isModelWarming(this.engineId)) {
	            classLogger.info("Model {} has entered warming state, waiting for activation", this.engineId);
	            // Canceling the HTTP request if still running
	            deploymentFuture.cancel(true);
	            return zkClient.waitForModelActive(this.engineId, timeoutMs - (System.currentTimeMillis() - startTime));
	        }
	        Thread.sleep(1000); // 1 sec polling
	    }

	    classLogger.error("Timeout waiting for model {} to enter warming state", this.engineId);
	    return false;
	}

	protected JSONObject makeModelRequest(JSONObject requestPayload) throws Exception {
		// Get current state and handle warming/cold states
		RemoteModelStateEnum currentState = zkClient.getModelState(this.engineId);

		// If cold try deploy
		if (currentState == RemoteModelStateEnum.COLD) {
			boolean deployed = initiateAndWaitForDeployment(120000); // 2 min
			if (!deployed) {
				classLogger.error("Failed to deploy model {}", this.engineId);
				return null;
			}
			currentState = zkClient.getModelState(this.engineId);
		}

		// Always wait for active state whether it started as WARMING or just became WARMING after deployment
		if (currentState == RemoteModelStateEnum.WARMING) {
			classLogger.info("Model {} is warming, waiting for activation...", this.engineId);
			boolean becameActive = zkClient.waitForModelActive(this.engineId, 300000); // 5 min
			if (!becameActive) {
				classLogger.error("Model {} failed to become active after warming", this.engineId);
				return null;
			}
			currentState = zkClient.getModelState(this.engineId);
		}

		if (currentState != RemoteModelStateEnum.ACTIVE) {
			classLogger.error("Model {} is not active. Current state: {}", this.engineId, currentState);
			return null;
		}

		String clusterIp = zkClient.getModelClusterIp(this.engineId);
		if (clusterIp == null) {
			classLogger.error("No cluster IP available for model {}", this.engineId);
			return null;
		}

		return makeGenerateRequest(clusterIp, requestPayload);
	}

	// For models that don't go through the OpenAI API (ie. NER, etc.)
	private JSONObject makeGenerateRequest(String clusterIp, JSONObject requestPayload) {
		String url = "";
		if (devPortFowarding) {
			url = "http://localhost:8888/api/generate";
		} else {
			url = String.format("http://%s/api/generate", clusterIp);
		}

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

	/**
	 * 
	 * @throws Exception
	 */
	private void checkModelUp() throws Exception {
		RemoteModelStateEnum currentState = zkClient.getModelState(this.engineId);
		// If not active after handling warming/cold states, return null
		if (currentState != RemoteModelStateEnum.ACTIVE) {
			initiateAndWaitForDeployment(300000);
		}
	}
	
	private String getModelUrl() {
		String clusterAddress = zkClient.getModelClusterIp(this.engineId);
		if (this.devPortFowarding) {
			return "http://localhost:8888/api";
		}
		return String.format("http://%s/api", clusterAddress);
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> hyperParameters)  {
		try {
			checkModelUp();
		} catch(Exception e) {
			classLogger.error("Error deploying model", e);
		}
		String modelUrl = getModelUrl();
		classLogger.info("Adding cluster address to parameters: {}", modelUrl);
		if (hyperParameters != null) {
			hyperParameters.put("base_url", modelUrl);
		} else {
			hyperParameters = new HashMap<>();
			hyperParameters.put("base_url", modelUrl);
		}
		return implementingEngineClass.askCall(question, fullPrompt, context, insight, hyperParameters);
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
		try {
			checkModelUp();
		} catch(Exception e) {
			classLogger.error("Error deploying model", e);
		}
		String modelUrl = getModelUrl();
		classLogger.info("Adding cluster address to parameters: {}", modelUrl);
		if (parameters != null) {
		    parameters.put("base_url", modelUrl);
		} else {
		    parameters = new HashMap<>();
		    parameters.put("base_url", modelUrl);
		}
		return implementingEngineClass.embeddingsCall(stringsToEmbed, insight, parameters);
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		try {
			checkModelUp();
		} catch(Exception e) {
			classLogger.error("Error deploying model", e);
		}
		// we are up
		// use the base impl to make the call
		return implementingEngineClass.modelCall(input, insight, parameters);
	}

	@Override
	protected InstructModelEngineResponse instructCall(String task, String context, List<Map<String, Object>> projectData, Insight insight, Map<String, Object> hyperParameters) {
		try {
			checkModelUp();
		} catch(Exception e) {
			classLogger.error("Error deploying model", e);
		}
		return implementingEngineClass.instructCall(task, context, projectData, insight, hyperParameters);
	}
}
