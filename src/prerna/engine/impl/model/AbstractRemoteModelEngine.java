/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

import prerna.cluster.util.IRemoteClientServer;
import prerna.cluster.util.RemoteClientServerZK;
import prerna.cluster.util.RemoteClientServerZKRESTProxy;
import prerna.cluster.util.ZKClientFactory;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.api.RemoteModelStateEnum;
import prerna.engine.impl.model.kserve.KServeAdapter;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;

/**
 * This is a class used to be extended by models running on KServe. It contains
 * methods for deploying the model to the cluster and making HTTP requests to
 * the model. See https://github.com/SEMOSS/kubernetes-model-scaler for
 * Kubernetes model scaling.
 */

public class AbstractRemoteModelEngine extends AbstractModelEngine {
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);

	protected String model;
	protected String modelRepoId;
	protected String modelType;
	private IRemoteClientServer zkClient;
	// Use this to simulate the cluster environment
	private Boolean devPortForwarding = false;
	// For normal development
	private String kmsIngressUrl = null;
	private String modelIngressUrl = null;
	private AbstractModelEngine implementingEngineClass = null;

	private final String INIT_PREFIX = "INIT_";

	private enum Services {
		KMS_START, // Kubernetes Model Scaler Start
		KMS_SHUTDOWN, // Kubernetes Model Scaler Shutdown
		MODEL // Model Specific Deployment
	}

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

		// Get the appropriate ZK client implementation based on environment
		this.zkClient = ZKClientFactory.getZKClient(this.devPortForwarding);

		// Check if we're using the REST proxy (for KMS_INGRESS validation)
		boolean usingRestProxy = this.zkClient instanceof RemoteClientServerZKRESTProxy;

		this.kmsIngressUrl = System.getenv("KMS_INGRESS");
		if (this.kmsIngressUrl != null && !this.kmsIngressUrl.isEmpty()) {
			classLogger.info("Using KMS_INGRESS from environment: {}", this.kmsIngressUrl);
			if (!this.kmsIngressUrl.endsWith("/")) {
				this.kmsIngressUrl += "/";
			}
		} else if (this.devPortForwarding) {
			classLogger.info("Using devPortforwarding for KMS URL with localhost:8000/");
		} else {
			classLogger.info(
					"KMS_INGRESS environment variable not found and devPortforwarding not set, using ZooKeeper for KMS IP resolution. This is correct for production deployments.");
		}

		this.modelIngressUrl = System.getenv("MODEL_INGRESS");
		if (this.modelIngressUrl != null && !this.modelIngressUrl.isEmpty()) {
			classLogger.info("Using MODEL_INGRESS from environment: {}", this.modelIngressUrl);
			if (!this.modelIngressUrl.endsWith("/")) {
				this.modelIngressUrl += "/";
			}
		} else if (this.devPortForwarding) {
			classLogger.info("Using devPortForwarding for model URLs with localhost:8888/");
		} else {
			classLogger.info(
					"MODEL_INGRESS environment variable not found and devPortforwarding not set, using ZooKeeper for Model IP resolution. This is correct for production deployments.");
		}

		String initEngineTypeKey = INIT_PREFIX + Constants.ENGINE_TYPE;
		String initEngineType = smssProp.getProperty(initEngineTypeKey);

		if (initEngineType != null && !initEngineType.isEmpty()) {
			implementingEngineClass = (AbstractModelEngine) Class.forName(initEngineType).newInstance();
			Properties implEngineSmss = new Properties();
			for (Object key : smssProp.keySet()) {
				String keyStr = (String) key;
				if (keyStr.equals(Constants.ENGINE_TYPE)) {
					implEngineSmss.put(Constants.ENGINE_TYPE, initEngineType);
				} else {
					implEngineSmss.put(keyStr, smssProp.getProperty(keyStr));
				}
			}
			String engineId = smssProp.getProperty(Constants.ENGINE);
			implementingEngineClass.open(implEngineSmss);
		}
	}

	private String createServiceUrl(Services service) throws Exception {
		// Priority order:
		// 1. Use devPortForwarding if enabled
		// 2. Use kmsIngressUrl if available
		// 3. Use service ip from ZooKeeper
		String serviceUrl;

		// KMS SHUTDOWN
		if (service == Services.KMS_SHUTDOWN) {
			if (devPortForwarding) {
				serviceUrl = String.format("http://localhost:8000/api/v2/stop?model_id=%s&model=%s", this.engineId,
						this.model);
			} else if (kmsIngressUrl != null) {
				serviceUrl = String.format("%sapi/v2/stop?model_id=%s&model=%s", kmsIngressUrl, this.engineId,
						this.model);
			} else {
				if (zkClient instanceof RemoteClientServerZKRESTProxy) {
					throw new IllegalStateException(
							"KMS_INGRESS environment variable must be set when using ZK REST Proxy");
				}

				RemoteClientServerZK directZkClient = (RemoteClientServerZK) zkClient;
				String modelScalerIp = directZkClient.getModelScalerIp();
				if (modelScalerIp == null) {
					classLogger.error("Unable to get model scaler IP from ZooKeeper");
					throw new IllegalStateException(
							"Unable to get model scaler IP from ZooKeeper for shutdown operation.");
				}
				serviceUrl = String.format("http://%s/api/v2/stop?model_id=%s&model=%s", modelScalerIp, this.engineId,
						this.model);
			}
			// KMS START
		} else if (service == Services.KMS_START) {
			if (devPortForwarding) {
				serviceUrl = "http://localhost:8000/api/v2/start";
			} else if (kmsIngressUrl != null) {
				serviceUrl = kmsIngressUrl + "api/v2/start";
			} else {
				if (zkClient instanceof RemoteClientServerZKRESTProxy) {
					throw new IllegalStateException(
							"KMS_INGRESS environment variable must be set when using ZK REST Proxy");
				}

				RemoteClientServerZK directZkClient = (RemoteClientServerZK) zkClient;
				String modelScalerIp = directZkClient.getModelScalerIp();
				if (modelScalerIp == null) {
					classLogger.error("Unable to get model scaler IP from ZooKeeper");
					throw new IllegalStateException(
							"Unable to get model scaler IP from ZooKeeper for deployment operation.");
				}
				serviceUrl = String.format("http://%s/api/v2/start", modelScalerIp);
			}
			// MODEL INFERENCE
		} else if (service == Services.MODEL) {
			// Grabbing the cluster IP in all situations since it should always have one
			String clusterIp = zkClient.getModelClusterIp(this.engineId);
			Boolean isModelTypeOpenAI = this.modelType.equals("OPEN_AI");
			if (clusterIp == null) {
				classLogger.error("No cluster IP available for model {}", this.engineId);
				throw new IllegalStateException("Unable to get cluster ip for model.");
			}
			// LOCAL DEV W/ PF
			if (devPortForwarding) {
				if (isModelTypeOpenAI) {
					serviceUrl = "http://localhost:8080/openai/v1";
				} else {
					serviceUrl = String.format("http://localhost:8080/v2/models/%s/infer", this.model);
				}
				// LOCAL DEV W/ INGRESS
			} else if (this.modelIngressUrl != null) {
				if (isModelTypeOpenAI) {
					serviceUrl = this.modelIngressUrl + this.model + "/openai/v1";
				} else {
					serviceUrl = this.modelIngressUrl + this.model + "/v2/models/" + this.model + "/infer";
				}
			}
			// DEPLOYMENT
			else {
				if (isModelTypeOpenAI) {
					serviceUrl = String.format("http://%s/openai/v1", clusterIp);
				} else {
					serviceUrl = String.format("http://%s/v2/models/%s/infer", clusterIp, this.model);
				}
			}
		} else {
			throw new IllegalArgumentException("Unsupported service: " + service);
		}

		return serviceUrl;
	}

	// STARTING A MODEL..
	public boolean initiateAndWaitForDeployment(long timeoutMs) throws Exception {
		if (zkClient.isModelActive(this.engineId)) {
			classLogger.info("Model {} is already active", this.engineId);
			return true;
		}

		if (zkClient.isModelWarming(this.engineId)) {
			classLogger.info("Model {} is already warming, waiting for activation", this.engineId);
			return zkClient.waitForModelActive(this.engineId, timeoutMs);
		}

		String deploymentUrl = this.createServiceUrl(Services.KMS_START);
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

				StringEntity entity = new StringEntity(payload.toString(), ContentType.APPLICATION_JSON);
				httpPost.setEntity(entity);

				httpClient.execute(httpPost).close();
			} catch (Exception e) {
				// I'm not hanging the main thread on this request, I'll just monitor ZK for the
				// model status
				classLogger.warn(
						"HTTP request to model scaler stilling progress, dropping connection but continuing to check ZooKeeper status",
						e);
			}
		});

		long startTime = System.currentTimeMillis();
		long warmingTimeout = Math.min(120000, timeoutMs);

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
			classLogger.info("Model {} is already shutdown", this.engineId);
			return String.format("Model %s is already shutdown", this.engineId);
		}

		String shutdownUrl = this.createServiceUrl(Services.KMS_SHUTDOWN);

		classLogger.debug("Using KMS shutdown URL: {}", shutdownUrl);

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(30000).setSocketTimeout(30000).build();

		try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

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
			String error = String.format("Error making shutdown request for model %s: %s", this.engineId,
					e.getMessage());
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

		// Always wait for active state whether it started as WARMING or just became
		// WARMING after deployment
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
			classLogger.error(
					"Model {} is not active in conncurrent hashmap. Current state: {}. Checking path directly",
					this.engineId, currentState);
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

		return makeInferenceRequest(clusterIp, requestPayload);
	}

	// For models that don't go through the OpenAI API (ie. NER, etc.)
	private JSONObject makeInferenceRequest(String clusterIp, JSONObject requestPayload) throws Exception {
		// Formatting the payload into KServe Protocol format
		JSONObject kservePayload = KServeAdapter.toKServeRequest(requestPayload);

		classLogger.debug("Sending KServe payload: {}", kservePayload.toString(2));
		classLogger.info("Sending request to model {} at cluster IP {}", this.engineId, clusterIp);

		String url = this.createServiceUrl(Services.MODEL);

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(30000).setSocketTimeout(900000).build();

		try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

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

	private String getModelUrl() throws Exception {
		return this.createServiceUrl(Services.MODEL);

	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight,
			String roomId, Map<String, Object> hyperParameters) {
		try {
			checkModelUp();
			String modelUrl = getModelUrl();

			if (modelUrl.endsWith("/infer")) {
				String pattern = "/v2/models/[^/]+/infer$";
				modelUrl = modelUrl.replaceAll(pattern, "/v1");
			}

			classLogger.info("Adding cluster address to parameters: {}", modelUrl);
			if (hyperParameters != null) {
				hyperParameters.put("base_url", modelUrl);
			} else {
				hyperParameters = new HashMap<>();
				hyperParameters.put("base_url", modelUrl);
			}

			hyperParameters.put("stream", false);
			return implementingEngineClass.askCall(question, fullPrompt, context, insight, roomId, hyperParameters);
		} catch (Exception e) {
			classLogger.error("Error getting model URL or deploying model", e);
			return null;
		}
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {
		try {
			checkModelUp();
			String modelUrl = getModelUrl();
			if (modelUrl.endsWith("/infer")) {
				String pattern = "/v2/models/[^/]+/infer$";
				modelUrl = modelUrl.replaceAll(pattern, "/v1");
			}
			classLogger.info("Adding cluster address to parameters: {}", modelUrl);
			if (parameters != null) {
				parameters.put("base_url", modelUrl);
			} else {
				parameters = new HashMap<>();
				parameters.put("base_url", modelUrl);
			}
			parameters.put("stream", false);
			return implementingEngineClass.embeddingsCall(stringsToEmbed, insight, parameters);
		} catch (Exception e) {
			classLogger.error("Error getting model URL or deploying model", e);
			return null;
		}
	}

	@Override
	protected EmbeddingsModelEngineResponse imageEmbeddingsCall(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		try {
			checkModelUp();
			String modelUrl = getModelUrl();
			if (modelUrl.endsWith("/infer")) {
				String pattern = "/v2/models/[^/]+/infer$";
				modelUrl = modelUrl.replaceAll(pattern, "/v1");
			}
			classLogger.info("Adding cluster address to parameters: {}", modelUrl);
			if (parameters != null) {
				parameters.put("base_url", modelUrl);
			} else {
				parameters = new HashMap<>();
				parameters.put("base_url", modelUrl);
			}
			parameters.put("stream", false);
			return implementingEngineClass.imageEmbeddingsCall(imagesToEmbed, insight, parameters);
		} catch (Exception e) {
			classLogger.error("Error getting model URL or deploying model", e);
			return null;
		}
	}

}
