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
package prerna.cluster.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.RemoteModelStateEnum;

/**
 * This class provides the same functionality as RemoteClientServerZK but connects to
 * the REST proxy for ZooKeeper instead of connecting directly to ZooKeeper.
 * It is designed for local development environments when direct ZooKeeper access isn't available.
 */
public class RemoteClientServerZKRESTProxy implements IRemoteClientServer {
    
    private static final Logger classLogger = LogManager.getLogger(RemoteClientServerZKRESTProxy.class);
    
    private static volatile RemoteClientServerZKRESTProxy instance;
    
    private static final String WARMING_PATH = "/models/warming";
    private static final String ACTIVE_PATH = "/models/active";
    private static final String MODEL_SCALER_PATH = "/services/kube-model-deployer";
    
    private String zkRestProxyBaseUrl;
    
    private final ConcurrentMap<String, RemoteModelStateEnum> modelStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> modelClusterIps = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> modelNames = new ConcurrentHashMap<>();
    
    // Periodic refreshes
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    private RemoteClientServerZKRESTProxy() {
        classLogger.info("RemoteClientServerZKRestProxy being initialized...");
    }
    
    public static RemoteClientServerZKRESTProxy getInstance() {
        if (instance == null) {
            synchronized (RemoteClientServerZKRESTProxy.class) {
                if (instance == null) {
                    // Fully initialize before publishing to the volatile field so other
                    // threads never observe a half-constructed singleton (mid-init()).
                    RemoteClientServerZKRESTProxy proxy = new RemoteClientServerZKRESTProxy();
                    proxy.init();
                    instance = proxy;
                }
            }
        }
        return instance;
    }
    
    private void init() {
        try {
            Map<String, String> env = System.getenv();
            if (env.containsKey("ZK_INGRESS")) {
                zkRestProxyBaseUrl = env.get("ZK_INGRESS");
                if (!zkRestProxyBaseUrl.endsWith("/")) {
                    zkRestProxyBaseUrl += "/";
                }
                classLogger.info("Using ZK_INGRESS from environment: {}", zkRestProxyBaseUrl);
            } else {
                throw new IllegalStateException("ZK_INGRESS environment variable is required for ZK REST Proxy mode");
            }
            
            // Validate connection to ZK REST Proxy
            validateConnection();
            
            // Initial load of model states
            loadInitialState();
            
            // Setup periodic refresh
            setupCacheRefresher();
            
        } catch (Exception e) {
            classLogger.error("Failed to initialize ZooKeeper REST Proxy connection", e);
            throw new RuntimeException("Failed to initialize ZooKeeper REST Proxy connection", e);
        }
    }
    
    private void validateConnection() throws Exception {
        String healthCheckUrl = zkRestProxyBaseUrl + "health";
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            
            HttpGet httpGet = new HttpGet(healthCheckUrl);
            
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IllegalStateException("ZooKeeper REST Proxy health check failed with status code: " + statusCode);
                }
                
                classLogger.info("Successfully connected to ZooKeeper REST Proxy");
            }
        } catch (IOException e) {
            classLogger.error("Failed to connect to ZooKeeper REST Proxy", e);
            throw new IllegalStateException("Failed to connect to ZooKeeper REST Proxy: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets ZNode data as a JSONObject from the REST proxy response.
     * This method extracts the nested "data" object from the response.
     * 
     * @param path The ZNode path
     * @return JSONObject containing the data field contents, or null if error/not found
     */
    private JSONObject getZNodeDataAsJson(String path) {
        String formattedPath = path;
        if (formattedPath.startsWith("/")) {
            formattedPath = formattedPath.substring(1);
        }
        
        String url = zkRestProxyBaseUrl + "znodes/v1/" + formattedPath;
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            
            HttpGet httpGet = new HttpGet(url);
            
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 404) {
                    classLogger.warn("ZNode not found at path: {}", path);
                    return null;
                }
                
                if (statusCode != 200) {
                    classLogger.error("Failed to get ZNode data, status code: {}", statusCode);
                    return null;
                }
                
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonResponse = EntityUtils.toString(entity);
                    JSONObject responseObj = new JSONObject(jsonResponse);
                    
                    if (responseObj.has("data") && !responseObj.isNull("data")) {
                        // The data field is a nested JSON object
                        JSONObject dataObj = responseObj.getJSONObject("data");
                        return dataObj;
                    }
                }
            }
            
        } catch (Exception e) {
            classLogger.error("Error getting ZNode data for path {}: {}", path, e.getMessage());
        }
        
        return null;
    }
    
    private void loadInitialState() {
        try {
            String modelScalerIpData = getZNodeData(MODEL_SCALER_PATH);
            if (modelScalerIpData != null) {
                classLogger.info("Model scaler IP from REST proxy: {}", modelScalerIpData);
            }
            
            List<String> activeModels = getZNodeChildren(ACTIVE_PATH);
            for (String modelId : activeModels) {
                String modelPath = ACTIVE_PATH + "/" + modelId;
                classLogger.info("Loading data for active model at path: {}", modelPath);
                
                JSONObject modelData = getZNodeDataAsJson(modelPath);
                if (modelData != null) {
                    String clusterIp = modelData.getString("ip");
                    String modelName = modelData.getString("model_name");
                    
                    modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
                    modelClusterIps.put(modelId, clusterIp);
                    modelNames.put(modelId, modelName);
                    
                    classLogger.info("Loaded active model {} ({}) with IP: {} from ZK REST Proxy.", 
                            modelId, modelName, clusterIp);
                }
            }
            
            List<String> warmingModels = getZNodeChildren(WARMING_PATH);
            for (String modelId : warmingModels) {
                modelStates.put(modelId, RemoteModelStateEnum.WARMING);
                classLogger.info("Loaded warming model: {}", modelId);
            }
            
        } catch (Exception e) {
            classLogger.error("Error loading initial state from ZK REST Proxy", e);
        }
    }
    
    private void setupCacheRefresher() {
        classLogger.info("Setting up cache refresher for remote models...");
        scheduler.scheduleAtFixedRate(() -> {
            try {
                refreshModelStates();
            } catch (Exception e) {
                classLogger.error("Error refreshing model states", e);
            }
        }, 30, 30, TimeUnit.SECONDS); // Refresh every 30 seconds
    }
    
    private void refreshModelStates() {
        classLogger.debug("Refreshing model states from ZK REST Proxy...");
        
        try {
            List<String> activeModels = getZNodeChildren(ACTIVE_PATH);
            for (String modelId : activeModels) {
                modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
            }
            
            List<String> warmingModels = getZNodeChildren(WARMING_PATH);
            for (String modelId : warmingModels) {
                if (!modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD).equals(RemoteModelStateEnum.ACTIVE)) {
                    modelStates.put(modelId, RemoteModelStateEnum.WARMING);
                }
            }
            
            for (String modelId : activeModels) {
                String path = ACTIVE_PATH + "/" + modelId;
                updateModelInfoFromZNode(modelId, path);
            }
            
            // Clean up stale entries
            for (String modelId : new ArrayList<>(modelStates.keySet())) {
                if (!activeModels.contains(modelId) && !warmingModels.contains(modelId)) {
                    modelStates.put(modelId, RemoteModelStateEnum.COLD);
                }
            }
            
            classLogger.debug("Refreshed model states, current map: {}", modelStates);
            
        } catch (Exception e) {
            classLogger.error("Error refreshing model states from ZK REST Proxy", e);
        }
    }
    
    private void updateModelInfoFromZNode(String modelId, String path) {
        JSONObject modelData = getZNodeDataAsJson(path);
        
        if (modelData != null) {
            try {
                String clusterIp = modelData.getString("ip");
                String modelName = modelData.getString("model_name");
                
                // Update cache maps
                modelClusterIps.put(modelId, clusterIp);
                modelNames.put(modelId, modelName);
                
                classLogger.debug("Updated model info for {}: name={}, ip={}", 
                    modelId, modelName, clusterIp);
            } catch (Exception e) {
                classLogger.error("Error extracting model data fields for {}: {}", modelId, e.getMessage());
            }
        }
    }
    
    private String getZNodeData(String path) {
        String formattedPath = path;
        if (formattedPath.startsWith("/")) {
            formattedPath = formattedPath.substring(1);
        }
        
        String url = zkRestProxyBaseUrl + "znodes/v1/" + formattedPath;
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            
            HttpGet httpGet = new HttpGet(url);
            
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 404) {
                    classLogger.warn("ZNode not found at path: {}", path);
                    return null;
                }
                
                if (statusCode != 200) {
                    classLogger.error("Failed to get ZNode data, status code: {}", statusCode);
                    return null;
                }
                
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonResponse = EntityUtils.toString(entity);
                    JSONObject responseObj = new JSONObject(jsonResponse);
                    
                    if (responseObj.has("data") && !responseObj.isNull("data")) {
                        return responseObj.getString("data");
                    }
                }
            }
            
        } catch (Exception e) {
            classLogger.error("Error getting ZNode data for path {}: {}", path, e.getMessage());
        }
        
        return null;
    }
    
    private List<String> getZNodeChildren(String path) {
        List<String> children = new ArrayList<>();
        
        String formattedPath = path;
        if (formattedPath.startsWith("/")) {
            formattedPath = formattedPath.substring(1);
        }
        
        String url = zkRestProxyBaseUrl + "znodes/v1/" + formattedPath + "?view=children";
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            
            HttpGet httpGet = new HttpGet(url);
            
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 404) {
                    classLogger.warn("ZNode not found at path: {}", path);
                    return children;
                }
                
                if (statusCode != 200) {
                    classLogger.error("Failed to get ZNode children, status code: {}", statusCode);
                    return children;
                }
                
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonResponse = EntityUtils.toString(entity);
                    JSONObject responseObj = new JSONObject(jsonResponse);
                    
                    if (responseObj.has("children")) {
                        JSONArray childrenArray = responseObj.getJSONArray("children");
                        for (int i = 0; i < childrenArray.length(); i++) {
                            children.add(childrenArray.getString(i));
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            classLogger.error("Error getting ZNode children for path {}: {}", path, e.getMessage());
        }
        
        return children;
    }
    
    private boolean checkZNodeExists(String path) {
        String formattedPath = path;
        if (formattedPath.startsWith("/")) {
            formattedPath = formattedPath.substring(1);
        }
        
        String url = zkRestProxyBaseUrl + "znodes/v1/" + formattedPath;
        
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(3000)
                .setSocketTimeout(3000)
                .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            
            HttpHead httpHead = new HttpHead(url);
            
            try (CloseableHttpResponse response = httpClient.execute(httpHead)) {
                return response.getStatusLine().getStatusCode() == 200;
            }
            
        } catch (Exception e) {
            classLogger.error("Error checking if ZNode exists at path {}: {}", path, e.getMessage());
        }
        
        return false;
    }
    
    public void close() {
        try {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
        } catch (Exception e) {
            classLogger.error("Error closing resources", e);
        }
    }
    
    @Override
    public RemoteModelStateEnum getModelState(String modelId) {
        return modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD);
    }
    
    @Override
    public String getModelClusterIp(String modelId) {
        String clusterIp = modelClusterIps.get(modelId);
        
        if (clusterIp == null) {
            String path = ACTIVE_PATH + "/" + modelId;
            JSONObject modelData = getZNodeDataAsJson(path);
            
            if (modelData != null && !modelData.isEmpty()) {
                try {
                    classLogger.info("modelData: {}", modelData);
                    clusterIp = modelData.getString("ip");
                    String modelName = modelData.getString("model_name");
                    
                    modelClusterIps.put(modelId, clusterIp);
                    modelNames.put(modelId, modelName);
                } catch (Exception e) {
                    classLogger.error("Error parsing model data for {}: {}", modelId, e.getMessage());
                }
            }
        }
        
        return clusterIp;
    }
    
    @Override
    public String getModelName(String modelId) {
        return modelNames.get(modelId);
    }
    
    @Override
    public boolean isModelWarming(String modelId) {
        String path = WARMING_PATH + "/" + modelId;
        return checkZNodeExists(path);
    }
    
    @Override
    public boolean isModelActive(String modelId) {
        String path = ACTIVE_PATH + "/" + modelId;
        return checkZNodeExists(path);
    }
    
    @Override
    public boolean waitForModelActive(String modelId, long timeoutMs) {
        try {
            long startTime = System.currentTimeMillis();
   
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                if (isModelActive(modelId)) {
                    classLogger.info("Model {} was found in the active path", modelId);
                    modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
                    return true;
                } else {
                    classLogger.info("Model {} in a warming wait loop..", modelId);
                }
                Thread.sleep(3000);
            }
            classLogger.warn("Timeout waiting for model {} to appear in active path after {}ms", 
                    modelId, timeoutMs);
            return false;

        } catch (Exception e) {
            classLogger.error("Error waiting for model {} to become active", modelId, e);
            return false;
        }
    }
    
    @Override
    public boolean waitForState(String modelId, RemoteModelStateEnum desiredState, long timeoutMs) {
        try {
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                RemoteModelStateEnum currentState = getModelState(modelId);
                if (currentState == desiredState) {
                    return true;
                }
                
                // If we're waiting for ACTIVE but the model is COLD, try to check active path directly
                if (desiredState == RemoteModelStateEnum.ACTIVE && currentState == RemoteModelStateEnum.COLD) {
                    if (isModelActive(modelId)) {
                        modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
                        return true;
                    }
                }
                
                Thread.sleep(1000); // 1 second
            }
            
            classLogger.warn("Timeout waiting for model {} to reach state {} after {}ms", 
                    modelId, desiredState, timeoutMs);
            return false;
            
        } catch (Exception e) {
            classLogger.error("Error waiting for model {} to reach state {}", modelId, desiredState, e);
            return false;
        }
    }
    

    @Override
    public List<RemoteModelInfo> getActiveModels() {
        List<RemoteModelInfo> activeModels = new ArrayList<>();
        
        try {
            List<String> activeModelIds = getZNodeChildren(ACTIVE_PATH);
            
            for (String modelId : activeModelIds) {
                String name = modelNames.get(modelId);
                RemoteModelStateEnum state = modelStates.getOrDefault(modelId, RemoteModelStateEnum.ACTIVE);
                
                if (name != null) {
                    activeModels.add(new RemoteModelInfo(
                            modelId,
                            name,
                            state
                    ));
                } else {
                    String path = ACTIVE_PATH + "/" + modelId;
                    JSONObject modelData = getZNodeDataAsJson(path);
                    
                    if (modelData != null) {
                        name = modelData.getString("model_name");
                        
                        activeModels.add(new RemoteModelInfo(
                                modelId,
                                name,
                                state
                        ));
                        
                        modelNames.put(modelId, name);
                    } else {
                        activeModels.add(new RemoteModelInfo(
                                modelId,
                                "Unknown",
                                state
                        ));
                    }
                }
            }
            
        } catch (Exception e) {
            classLogger.error("Error getting active models with details", e);
        }
        
        return activeModels;
    }
    
    @Override
    public List<RemoteModelInfo> getWarmingModels() {
        List<RemoteModelInfo> warmingModels = new ArrayList<>();
        
        try {
            List<String> warmingModelIds = getZNodeChildren(WARMING_PATH);
            
            for (String modelId : warmingModelIds) {
                String name = modelNames.get(modelId);
                RemoteModelStateEnum state = modelStates.getOrDefault(modelId, RemoteModelStateEnum.WARMING);
                
                warmingModels.add(new RemoteModelInfo(
                        modelId,
                        name != null ? name : "Warming...",
                        state
                ));
            }
            
        } catch (Exception e) {
            classLogger.error("Error getting warming models with details", e);
        }
        
        return warmingModels;
    }
    
	public String getModelScalerIp() {
		return System.getenv("KMS_INGRESS");
	}
}