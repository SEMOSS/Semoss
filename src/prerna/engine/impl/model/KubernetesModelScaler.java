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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.cluster.util.IRemoteClientServer;
import prerna.cluster.util.RemoteClientServerZKRESTProxy;
import prerna.cluster.util.ZKClientFactory;

public final class KubernetesModelScaler {

	private static final Logger classLogger = LogManager.getLogger(KubernetesModelScaler.class);

	private static volatile KubernetesModelScaler instance;

	private KubernetesModelScaler() {
		classLogger.info("KubernetesModelScaler being initialized...");
	}

	public static KubernetesModelScaler getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (KubernetesModelScaler.class) {
				if (instance == null) {
					instance = new KubernetesModelScaler();
					instance.init();
				}
			}
		}
		return instance;
	}

	private IRemoteClientServer zkClient;
	// Use this to simulate the cluster environment
	private Boolean devPortForwarding = false;
	// For normal development
	private String kmsUrl = null;

	private void init() {

		// Get the appropriate ZK client implementation based on environment
		this.zkClient = ZKClientFactory.getZKClient(this.devPortForwarding);

		// Check if we're using the REST proxy (for KMS_INGRESS validation)
		boolean usingRestProxy = this.zkClient instanceof RemoteClientServerZKRESTProxy;
		// Check if we have an ingress env var
		String kmsIngressUrl = System.getenv("KMS_INGRESS");

		if (kmsIngressUrl != null && !kmsIngressUrl.isEmpty()) {
			classLogger.info("Using KMS_INGRESS from environment: {}", kmsIngressUrl);
			if (kmsIngressUrl.endsWith("/")) {
				kmsIngressUrl += "/";
			}
			this.kmsUrl = kmsIngressUrl;
		} else if (this.devPortForwarding) {
			classLogger.info("Using devPortforwarding for KMS URL with localhost:8000/");
			this.kmsUrl = "http://localhost:8000/";
		} else {
			classLogger.info(
					"KMS_INGRESS environment variable not found and devPortforwarding not set, using ZooKeeper for KMS IP resolution. This is correct for production deployments.");
			String zkModelScalerIp = zkClient.getModelScalerIp();

			if (zkModelScalerIp == null || zkModelScalerIp.trim().isEmpty()) {
				throw new RuntimeException(
						"Unable to determine KMS URL: ZooKeeper returned null or empty model scaler IP");
			}

			if (!zkModelScalerIp.startsWith("http://") && !zkModelScalerIp.startsWith("https://")) {
				zkModelScalerIp = "http://" + zkModelScalerIp;
			}

			this.kmsUrl = zkModelScalerIp;
		}
	}

	public Map<String, Object> getNodePoolsInfo() throws Exception {
		String serviceUrl = this.kmsUrl + "/api/resources/node-pools-info";

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build();

		try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

			HttpGet httpGet = new HttpGet(serviceUrl);
			httpGet.setHeader("Accept", "application/json");

			try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
				int statusCode = response.getStatusLine().getStatusCode();
				HttpEntity responseEntity = response.getEntity();
				String responseString = EntityUtils.toString(responseEntity);

				if (statusCode == 200) {
					JSONObject jsonResponse = new JSONObject(responseString);

					Map<String, Object> result = new HashMap<>();

					if (jsonResponse.has("message")) {
						result.put("message", jsonResponse.getString("message"));
					}

					if (jsonResponse.has("zk_info")) {
						JSONObject zkInfoObj = jsonResponse.getJSONObject("zk_info");
						Map<String, Object> zkInfoMap = jsonToMap(zkInfoObj);
						result.put("zk_info", zkInfoMap);
					}

					if (jsonResponse.has("pools")) {
						JSONArray poolsArray = jsonResponse.getJSONArray("pools");
						List<Map<String, Object>> poolsList = new ArrayList<>();

						for (int i = 0; i < poolsArray.length(); i++) {
							JSONObject poolObject = poolsArray.getJSONObject(i);
							Map<String, Object> poolMap = jsonToMap(poolObject);
							poolsList.add(poolMap);
						}

						result.put("pools", poolsList);
					}

					if (jsonResponse.has("active_models_actual")) {
						JSONObject activeModelsObj = jsonResponse.getJSONObject("active_models_actual");
						Map<String, Object> activeModelsMap = jsonToMap(activeModelsObj);
						result.put("active_models_actual", activeModelsMap);
					}

					classLogger.info("Successfully retrieved node pool information");
					return result;
				} else {
					classLogger.error("Error retrieving node pool information: {} (Status: {})", responseString,
							statusCode);
					throw new RuntimeException("Failed to retrieve node pool information: " + responseString);
				}
			}
		} catch (Exception e) {
			classLogger.error("Error making request to model scaler for node pool info: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to retrieve node pool information", e);
		}
	}

	private Map<String, Object> jsonToMap(JSONObject json) {
		Map<String, Object> map = new HashMap<>();

		for (String key : json.keySet()) {
			Object value = json.get(key);

			if (value instanceof JSONObject) {
				map.put(key, jsonToMap((JSONObject) value));
			} else if (value instanceof JSONArray) {
				JSONArray array = (JSONArray) value;
				List<Object> list = new ArrayList<>();

				for (int i = 0; i < array.length(); i++) {
					Object item = array.get(i);
					if (item instanceof JSONObject) {
						list.add(jsonToMap((JSONObject) item));
					} else {
						list.add(item);
					}
				}

				map.put(key, list);
			} else {
				map.put(key, value);
			}
		}

		return map;
	}

	/**
	 * Retrieve the cached (or freshly re-loaded) KServe deployment configs from the
	 * model-scaler service.
	 *
	 * @param refresh if {@code true}, tells the micro-service to reload the configs
	 *                from GCS before returning the response.
	 * @return a Map keyed by model-name, where each value is a nested Map
	 *         representing the InferenceService spec for that model.
	 * @throws Exception on any HTTP or parse failure.
	 */
	public Map<String, Object> getModelDeploymentConfigs(boolean refresh) throws Exception {
		// Build the URL – keep the same /api/resources prefix you use elsewhere
		String endpoint = "/api/resources/model-deploy-configs";
		String serviceUrl = this.kmsUrl + endpoint + (refresh ? "?refresh=true" : "");

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build();

		try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

			HttpGet httpGet = new HttpGet(serviceUrl);
			httpGet.setHeader("Accept", "application/json");

			try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
				int statusCode = response.getStatusLine().getStatusCode();
				String responseBody = EntityUtils.toString(response.getEntity());

				if (statusCode == 200) {
					JSONObject jsonResponse = new JSONObject(responseBody);
					Map<String, Object> configs = jsonToMap(jsonResponse);

					classLogger.info("Retrieved {} model deployment configs (refresh={})", configs.size(), refresh);
					return configs;
				} else {
					classLogger.error("Failed to fetch model deployment configs: {} (status {})", responseBody,
							statusCode);
					throw new RuntimeException("Failed to retrieve model deployment configs: " + responseBody);
				}
			}
		} catch (Exception e) {
			classLogger.error("Error contacting model-scaler for deployment configs: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to retrieve model deployment configs", e);
		}
	}

	public Map<String, Object> canItRun(String hfModelId) throws Exception {

		String serviceUrl = this.kmsUrl + "/api/can-it-run";

		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build();

		try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

			HttpPost httpPost = new HttpPost(serviceUrl);
			httpPost.setHeader("Content-Type", "application/json");

			JSONObject requestBody = new JSONObject();
			requestBody.put("model_id", hfModelId);

			StringEntity entity = new StringEntity(requestBody.toString());
			httpPost.setEntity(entity);

			try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
				int statusCode = response.getStatusLine().getStatusCode();
				HttpEntity responseEntity = response.getEntity();
				String responseString = EntityUtils.toString(responseEntity);

				if (statusCode == 200) {
					JSONObject jsonResponse = new JSONObject(responseString);

					Map<String, Object> result = new HashMap<>();
					for (String key : jsonResponse.keySet()) {
						result.put(key, jsonResponse.get(key));
					}

					classLogger.info("Successfully checked compatibility for model: {} - Can run: {}", hfModelId,
							result.get("can_run"));
					return result;
				} else {
					JSONObject errorResponse = new JSONObject(responseString);
					String errorMessage = errorResponse.getJSONObject("detail").getString("message");
					classLogger.error("Error checking model compatibility: {} (Status: {})", errorMessage, statusCode);
					throw new RuntimeException("Failed to check model compatibility: " + errorMessage);
				}
			}
		} catch (Exception e) {
			classLogger.error("Error making request to model scaler: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to check model compatibility", e);
		}
	}

}
