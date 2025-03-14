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
import prerna.engine.impl.model.kserve.KServeAdapter;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.api.RemoteModelStateEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;


/**
 * This is a class used to be extended by models running on KServe.
 * It contains methods for deploying the model to the cluster and making HTTP requests to the model.
 * See https://github.com/SEMOSS/kubernetes-model-scaler for Kubernetes model scaling.
 */

public class AbstractRemoteModelEngine extends AbstractModelEngine {
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);

	protected String model;
	protected String modelRepoId;
	protected String modelType;
	private RemoteClientServerZK zkClient;
	// Use this to simulate the cluster environment
	private Boolean devPortFowarding = false;
	// For normal development
	private String kmsIngressUrl = null;
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
		
		this.kmsIngressUrl = System.getenv("KMS_INGRESS");
		if (this.kmsIngressUrl != null && !this.kmsIngressUrl.isEmpty()) {
			classLogger.info("Using KMS_INGRESS from environment: {}", this.kmsIngressUrl);
			if (!this.kmsIngressUrl.endsWith("/")) {
				this.kmsIngressUrl += "/";
			}
		} else if (this.devPortFowarding) {
			classLogger.info("Using devPortforwarding for KMS URL with localhost:8000");
		} else {
			classLogger.info("KMS_INGRESS environment variable not found and devPortforwarding not set, using ZooKeeper for KMS IP resolution. This is correct for production deployments.");
		}

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
	
	public boolean initiateAndWaitForDeployment(long timeoutMs) throws Exception {
	    if (zkClient.isModelActive(this.engineId)) {
	        classLogger.info("Model {} is already active", this.engineId);
	        return true;
	    }

	    if (zkClient.isModelWarming(this.engineId)) {
	        classLogger.info("Model {} is already warming, waiting for activation", this.engineId);
	        return zkClient.waitForModelActive(this.engineId, timeoutMs);
	    }

	    String deploymentUrl;
	    // Priority order: 
	    // 1. Use devPortForwarding if enabled
	    // 2. Use kmsIngressUrl if available
	    // 3. Use modelScalerIp from ZooKeeper
	    if (devPortFowarding) {
	        deploymentUrl = "http://localhost:8000/api/v2/start";
	    } else if (kmsIngressUrl != null) {
	        deploymentUrl = kmsIngressUrl + "api/v2/start";
	    } else {
	        String modelScalerIp = zkClient.getModelScalerIp();
	        if (modelScalerIp == null) {
	            classLogger.error("Unable to get model scaler IP from ZooKeeper");
	            return false;
	        }
	        deploymentUrl = String.format("http://%s/api/v2/start", modelScalerIp);
	    }
	    
	    classLogger.info("Using deployment URL: {}", deploymentUrl);


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
	
	public String shutdownModelRequest() throws Exception {
	    RemoteModelStateEnum currentState = zkClient.getModelState(this.engineId);
	    
	    if (currentState == RemoteModelStateEnum.COLD) {
	        classLogger.info("Model {} is already cold", this.engineId);
	        return String.format("Model %s is already cold", this.engineId);
	    }
	    
	    String shutdownUrl;
	    // Priority order: 
	    // 1. Use devPortForwarding if enabled
	    // 2. Use kmsIngressUrl if available
	    // 3. Use modelScalerIp from ZooKeeper
	    if (devPortFowarding) {
	        shutdownUrl = String.format("http://localhost:8000/api/v2/stop?model_id=%s&model=%s", 
	            this.engineId, this.model);
	    } else if (kmsIngressUrl != null) {
	        shutdownUrl = String.format("%sapi/v2/stop?model_id=%s&model=%s", 
	            kmsIngressUrl, this.engineId, this.model);
	    } else {
	        String modelScalerIp = zkClient.getModelScalerIp();
	        if (modelScalerIp == null) {
	            classLogger.error("Unable to get model scaler IP from ZooKeeper");
	            return "Failed to get model scaler IP";
	        }
	        shutdownUrl = String.format("http://%s/api/v2/stop?model_id=%s&model=%s", 
	            modelScalerIp, this.engineId, this.model);
	    }
	    
	    classLogger.debug("Using KMS shutdown URL: {}", shutdownUrl);

	    RequestConfig requestConfig = RequestConfig.custom()
	            .setConnectTimeout(30000)
	            .setSocketTimeout(30000)
	            .build();

	    try (CloseableHttpClient httpClient = HttpClients.custom()
	            .setDefaultRequestConfig(requestConfig)
	            .build()) {

	    	HttpPost httpPost = new HttpPost(shutdownUrl);

	        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
	            int statusCode = response.getStatusLine().getStatusCode();
	            if (statusCode != 200) {
	                String error = String.format("Shutdown request failed with status code: %d", statusCode);
	                classLogger.error(error);
	                return error;
	            }

	            String message = String.format("Successfully initiated shutdown for model %s", this.engineId);
	            classLogger.info(message);
	            return message;
	        }
	    } catch (Exception e) {
	        String error = String.format("Error making shutdown request for model %s: %s", 
	            this.engineId, e.getMessage());
	        classLogger.error(error, e);
	        return error;
	    }
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
			// TEMP SOLUTION TO CURATOR DESYNC ISSUE
			classLogger.error("Model {} is not active in conncurrent hashmap. Current state: {}. Checking path directly", this.engineId, currentState);
			Boolean modelActive = zkClient.isModelActive(this.engineId);
			if (!modelActive) {
				classLogger.error("Model {} is not active in ZooKeeper", this.engineId);
				return null;
			}
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
		// Formatting the payload into KServe Protocol format
		JSONObject kservePayload = KServeAdapter.toKServeRequest(requestPayload);
		
		classLogger.debug("Sending KServe payload: {}", kservePayload.toString(2));
		
		classLogger.info("Sending request to model {} at cluster IP {}", this.engineId, clusterIp);
		
		String url = "";
		if (devPortFowarding) {
			url = String.format("http://localhost:8080/v2/models/%s/infer", this.model);
		} else {
			url = String.format("http://%s/v2/models/%s/infer", clusterIp, this.model);
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
			httpPost.setHeader("Inference-Header-Content-Length", "2000");

			StringEntity entity = new StringEntity(kservePayload.toString(), ContentType.APPLICATION_JSON);
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
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    
                    JSONObject kserveResponse = new JSONObject(sb.toString());
                    
                    JSONObject modelResponse = KServeAdapter.formatKServeResponse(kserveResponse);
                    
                    return modelResponse;
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
	protected EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters) {
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
		return implementingEngineClass.imageEmbeddingsCall(imagesToEmbed, insight, parameters);
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
