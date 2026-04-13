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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.RemoteModelStateEnum;

/**
 * This is a Singleton class used to exclusively manage a connection to
 * ZooKeeper for tracking RemoteClientServer deployments on our cluster. This
 * will track the state of each model deployment, including whether it is
 * warming or active. This class handles holding requests until models are in an
 * active state by performing health checks on the FastAPI service running in
 * the container. This is required to give the FastAPI service a grace time
 * between when the model is added to the active path and the time it requires
 * to start up in the container. This is used by engines that extend the
 * AbstractRemoteModelEngine IE: NEREngine..
 */
public final class RemoteClientServerZK implements IRemoteClientServer {

	private static final Logger classLogger = LogManager.getLogger(RemoteClientServerZK.class);

	private static volatile RemoteClientServerZK instance;

	private static final String WARMING_PATH = "/models/warming";
	private static final String ACTIVE_PATH = "/models/active";
	private static final String MODEL_SCALER_PATH = "/services/kube-model-deployer";

	// Connection-related fields
	private CuratorFramework client;
	private String zkServer = "localhost:2181";
	private boolean connected = false;
	private Map<String, String> env;

	// State tracking
	private final ConcurrentMap<String, RemoteModelStateEnum> modelStates = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, String> modelClusterIps = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, String> modelNames = new ConcurrentHashMap<>();
	private CuratorCache warmingCache;
	private CuratorCache activeCache;

	public String modelScalerIp;

	private Boolean devPortForwarding = false;

	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	private RemoteClientServerZK() {
		classLogger.info("RemoteClientServerZK being initialized...");
	}

	public static RemoteClientServerZK getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (RemoteClientServerZK.class) {
				if (instance == null) {
					instance = new RemoteClientServerZK();
					instance.init();
				}
			}
		}
		return instance;
	}

	private void init() {
		try {
			env = System.getenv();
			if (env.containsKey("ZK_SERVER")) {
				zkServer = env.get("ZK_SERVER");
			}

			// Initialize Curator client
			RetryOneTime retryPolicy = new RetryOneTime(1000);
			client = CuratorFrameworkFactory.builder().connectString(zkServer).retryPolicy(retryPolicy)
					.connectionTimeoutMs(5000).sessionTimeoutMs(10000).maxCloseWaitMs(2000).build();

			// Add connection state listener
			client.getConnectionStateListenable().addListener((client, state) -> {
				classLogger.info("ZooKeeper connection state changed to: {}", state);
				if (state == ConnectionState.CONNECTED || state == ConnectionState.RECONNECTED) {
					connected = true;
				} else if (state == ConnectionState.LOST || state == ConnectionState.SUSPENDED) {
					connected = false;
				}
			});

			client.start();

			// Wait for connection
			if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Failed to connect to ZooKeeper");
			}

			setupZKPaths();
			setupCaches();

			loadInitialState();

			setupCacheRefresher();

		} catch (Exception e) {
			classLogger.error("Failed to initialize ZooKeeper connection", e);
			throw new RuntimeException("Failed to initialize ZooKeeper connection", e);
		}
	}

	private void loadInitialState() {
		try {
			// Loading model scaler IP first
			if (client.checkExists().forPath(MODEL_SCALER_PATH) != null) {
				byte[] scalerData = client.getData().forPath(MODEL_SCALER_PATH);
				if (scalerData != null && scalerData.length > 0) {
					modelScalerIp = new String(scalerData, "UTF-8");
					classLogger.info("Discovered model scaler IP: {}", modelScalerIp);
				} else {
					classLogger.error("Model scaler path exists but no IP data found");
				}
			}

			// Load active models
			List<String> activeModels = client.getChildren().forPath(ACTIVE_PATH);
			for (String modelId : activeModels) {
				String path = ACTIVE_PATH + "/" + modelId;
				classLogger.info("Loading data for active model at path: {}", path);

				byte[] data = client.getData().forPath(path);
				if (data != null && data.length > 0) {
					String rawData = new String(data, "UTF-8");

					JSONObject jsonData = new JSONObject(rawData);
					String clusterIp = jsonData.getString("ip");
					String modelName = jsonData.getString("model_name");

					modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
					modelClusterIps.put(modelId, clusterIp);
					modelNames.put(modelId, modelName);

					classLogger.info("Loaded active model {} ({}) with IP: {} from ZK.", modelId, modelName, clusterIp);
				}
			}

			// Load warming models
			List<String> warmingModels = client.getChildren().forPath(WARMING_PATH);
			for (String modelId : warmingModels) {
				modelStates.put(modelId, RemoteModelStateEnum.WARMING);
				classLogger.info("Loaded warming model: {}", modelId);
			}
		} catch (Exception e) {
			classLogger.error("Error loading initial state", e);
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

	private void refreshModelStates() throws Exception {
		classLogger.debug("Refreshing model states...");

		// Refresh active models
		List<String> activeModels = client.getChildren().forPath(ACTIVE_PATH);
		for (String modelId : activeModels) {
			modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
		}

		// Refresh warming models
		List<String> warmingModels = client.getChildren().forPath(WARMING_PATH);
		for (String modelId : warmingModels) {
			if (!modelStates.get(modelId).equals(RemoteModelStateEnum.ACTIVE)) {
				modelStates.put(modelId, RemoteModelStateEnum.WARMING);
			}
		}

		// Clean up stale entries
		for (String modelId : new HashSet<>(modelStates.keySet())) {
			if (!activeModels.contains(modelId) && !warmingModels.contains(modelId)) {
				modelStates.put(modelId, RemoteModelStateEnum.COLD);
			}
		}

		classLogger.debug("Refreshed model states, current map: {}", modelStates);
	}

	@Override
	public String getModelScalerIp() {
		return modelScalerIp;
	}

	public void close() {
		try {
			if (warmingCache != null) {
				warmingCache.close();
			}
			if (activeCache != null) {
				activeCache.close();
			}
			if (client != null) {
				client.close();
			}
		} catch (Exception e) {
			classLogger.error("Error closing resources", e);
		}
	}

	private void setupZKPaths() {
		try {
			// Ensure base paths exist
			if (client.checkExists().forPath(WARMING_PATH) == null) {
				client.create().creatingParentsIfNeeded().forPath(WARMING_PATH);
			}
			if (client.checkExists().forPath(ACTIVE_PATH) == null) {
				client.create().creatingParentsIfNeeded().forPath(ACTIVE_PATH);
			}
		} catch (Exception e) {
			classLogger.error("Error setting up ZK paths", e);
		}
	}

	private void setupCaches() {
		try {
			// Set up cache for warming path
			warmingCache = CuratorCache.build(client, WARMING_PATH);
			CuratorCacheListener warmingListener = CuratorCacheListener.builder().forCreates(node -> {
				String modelId = getModelIdFromPath(node.getPath());
				classLogger.info("Model {} entered warming state", modelId);
				modelStates.put(modelId, RemoteModelStateEnum.WARMING);
			}).forDeletes(node -> {
				String modelId = getModelIdFromPath(node.getPath());
				classLogger.info("Model {} left warming state", modelId);
				// Only update if not active
				if (!modelStates.get(modelId).equals(RemoteModelStateEnum.ACTIVE)) {
					modelStates.put(modelId, RemoteModelStateEnum.COLD);
				}
			}).build();
			warmingCache.listenable().addListener(warmingListener);
			warmingCache.start();

			// Set up cache for active path
			activeCache = CuratorCache.build(client, ACTIVE_PATH);
			CuratorCacheListener activeListener = CuratorCacheListener.builder().forCreates(node -> {
				String modelId = getModelIdFromPath(node.getPath());
				classLogger.info("Model {} became active, processing data from path: {}", modelId, node.getPath());
				modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);

				try {
					// First try immediate read
					byte[] data = node.getData();
					if (data == null || data.length == 0) {
						classLogger.debug("Initial data read empty for {}, starting retry sequence", modelId);
						// If empty Retry
						data = getNodeDataWithRetry(node.getPath(), 5, 500); // 5 retries, 500ms delay
					}

					if (data == null || data.length == 0) {
						classLogger.error("No data found for active model {} after retries", modelId);
						return;
					}

					String rawData = new String(data, "UTF-8");

					classLogger.info("Processing raw data for model {}: {}", modelId, rawData);

					JSONObject jsonData = new JSONObject(rawData);
					String clusterIp = jsonData.getString("ip");
					String modelName = jsonData.getString("model_name");

					modelClusterIps.put(modelId, clusterIp);
					modelNames.put(modelId, modelName);

					classLogger.info("Successfully registered model {} ({}) with IP: {}", modelId, modelName,
							clusterIp);
				} catch (Exception e) {
					classLogger.error("Error processing data for model {}: {}", modelId, e.getMessage(), e);
				}
			}).forDeletes(node -> {
				String modelId = getModelIdFromPath(node.getPath());
				String modelName = modelNames.get(modelId);
				classLogger.info("Model {} ({}) is no longer active", modelId, modelName);
				modelClusterIps.remove(modelId);
				modelNames.remove(modelId);
				if (isModelWarming(modelId)) {
					modelStates.put(modelId, RemoteModelStateEnum.WARMING);
				} else {
					modelStates.put(modelId, RemoteModelStateEnum.COLD);
				}
			}).build();
			activeCache.listenable().addListener(activeListener);
			activeCache.start();

		} catch (Exception e) {
			classLogger.error("Error setting up ZK caches", e);
		}
	}

	private byte[] getNodeDataWithRetry(String path, int maxRetries, long delayMs) {
		for (int i = 0; i < maxRetries; i++) {
			try {
				byte[] data = client.getData().forPath(path);
				if (data != null && data.length > 0) {
					return data;
				}
				Thread.sleep(delayMs);
			} catch (Exception e) {
				classLogger.debug("Attempt {} to read data from {} failed: {}", i + 1, path, e.getMessage());
			}
		}
		return null;
	}

	@Override
	public String getModelName(String modelId) {
		return modelNames.get(modelId);
	}

	private String getModelIdFromPath(String path) {
		return path.substring(path.lastIndexOf('/') + 1);
	}

	@Override
	public RemoteModelStateEnum getModelState(String modelId) {
		return modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD);
	}

	private String getModelPath(String modelId) {
		return modelId;
	}

	@Override
	public String getModelClusterIp(String modelId) {
		try {
			String fullPath = ACTIVE_PATH + "/" + modelId;
			if (client.checkExists().forPath(fullPath) == null) {
				classLogger.error("Path does not exist: {}", fullPath);
				return null;
			}

			byte[] data = client.getData().forPath(fullPath);

			if (data == null || data.length == 0) {
				classLogger.error("No data found at path: {}", fullPath);
				return null;
			}

			String rawData = new String(data, "UTF-8");

			JSONObject jsonData = new JSONObject(rawData);
			String ip = jsonData.getString("ip");

			// Update cache maps
			modelClusterIps.put(modelId, ip);
			modelNames.put(modelId, jsonData.getString("model_name"));

			return ip;
		} catch (Exception e) {
			classLogger.error("Error getting cluster IP for model {}: {}", modelId, e.getMessage(), e);
			return null;
		}
	}

	@Override
	public boolean isModelWarming(String modelId) {
		try {
			String modelPath = getModelPath(modelId);
			return client.checkExists().forPath(WARMING_PATH + "/" + modelPath) != null;
		} catch (Exception e) {
			classLogger.error("Error checking warming state for model {}", modelId, e);
			return false;
		}
	}

	@Override
	public boolean isModelActive(String modelId) {
		try {
			String modelPath = getModelPath(modelId);
			return client.checkExists().forPath(ACTIVE_PATH + "/" + modelPath) != null;
		} catch (Exception e) {
			classLogger.error("Error checking active state for model {}", modelId, e);
			return false;
		}
	}

	@Override
	public boolean waitForModelActive(String modelId, long timeoutMs) {
		try {
			long startTime = System.currentTimeMillis();

			// Waiting for model to appear in active path
			while (System.currentTimeMillis() - startTime < timeoutMs) {
				if (isModelActive(modelId)) {
					classLogger.info("Model {} was found in the active path", modelId);
					return true;
				} else {
					classLogger.info("Model {} in a warming wait loop..", modelId);
				}
				Thread.sleep(3000);
			}

			classLogger.warn("Timeout waiting for model {} to appear in active path after {}ms", modelId, timeoutMs);
			return false;

		} catch (Exception e) {
			classLogger.error("Error waiting for model {} to become active", modelId, e);
			return false;
		}
	}

	/**
	 * Waits for a model to reach a specific state, with a timeout
	 * 
	 * @param modelId      The ID of the model to wait for
	 * @param desiredState The state to wait for
	 * @param timeoutMs    Maximum time to wait in milliseconds
	 * @return true if the model reached the desired state within the timeout, false
	 *         otherwise
	 */
	@Override
	public boolean waitForState(String modelId, RemoteModelStateEnum desiredState, long timeoutMs) {
		try {
			long startTime = System.currentTimeMillis();
			while (System.currentTimeMillis() - startTime < timeoutMs) {
				RemoteModelStateEnum currentState = getModelState(modelId);
				if (currentState == desiredState) {
					return true;
				}
				// If we're waiting for ACTIVE but hit FAILED, break early
				if (desiredState == RemoteModelStateEnum.ACTIVE && currentState == RemoteModelStateEnum.FAILED) {
					classLogger.error("Model {} failed while waiting for active state", modelId);
					return false;
				}
				Thread.sleep(1000); // 1 second
			}
			classLogger.warn("Timeout waiting for model {} to reach state {} after {}ms", modelId, desiredState,
					timeoutMs);
			return false;
		} catch (Exception e) {
			classLogger.error("Error waiting for model {} to reach state {}", modelId, desiredState, e);
			return false;
		}
	}

	/**
	 * Gets a list of all active models with their associated information
	 * 
	 * @return List of ModelInfo objects containing model details
	 */
	@Override
	public List<RemoteModelInfo> getActiveModels() {
		List<RemoteModelInfo> activeModels = new ArrayList<>();

		try {
			List<String> activeModelIds = client.getChildren().forPath(ACTIVE_PATH);

			for (String modelId : activeModelIds) {
				String name = modelNames.get(modelId);
				RemoteModelStateEnum state = modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD);

				if (name != null) {
					activeModels.add(new RemoteModelInfo(modelId, name, state));
				} else {
					classLogger.warn("Incomplete data for active model {}: name={}", modelId, name);

					String fullPath = ACTIVE_PATH + "/" + modelId;
					byte[] data = client.getData().forPath(fullPath);
					if (data != null && data.length > 0) {
						String rawData = new String(data, "UTF-8");
						JSONObject jsonData = new JSONObject(rawData);

						name = jsonData.getString("model_name");

						activeModels.add(new RemoteModelInfo(modelId, name, state));

						modelNames.put(modelId, name);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Error getting active models with details", e);
		}

		return activeModels;
	}

	/**
	 * Gets a list of all warming models with their associated information
	 * 
	 * @return List of RemoteModelInfo objects containing model details
	 */
	@Override
	public List<RemoteModelInfo> getWarmingModels() {
		List<RemoteModelInfo> warmingModels = new ArrayList<>();

		try {
			List<String> warmingModelIds = client.getChildren().forPath(WARMING_PATH);

			for (String modelId : warmingModelIds) {
				String name = modelNames.get(modelId);
				RemoteModelStateEnum state = modelStates.getOrDefault(modelId, RemoteModelStateEnum.WARMING);

				warmingModels.add(new RemoteModelInfo(modelId, name != null ? name : "Warming...", state));
			}
		} catch (Exception e) {
			classLogger.error("Error getting warming models with details", e);
		}

		return warmingModels;
	}
}