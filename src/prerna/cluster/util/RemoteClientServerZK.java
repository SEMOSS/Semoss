package prerna.cluster.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.RemoteModelStateEnum;

/**
 * This is a Singleton class used to exclusively manage a connection to ZooKeeper for tracking RemoteClientServer deployments on our cluster.
 * This will track the state of each model deployment, including whether it is warming or active.
 * This class handles holding requests until models are in an active state by performing health checks on the FastAPI service running in the container.
 * This is required to give the FastAPI service a grace time between when the model is added to the active path and the time it requires to start up in the container.
 * This is used by engines that extend the AbstractRemoteModelEngine IE: NEREngine..
 * See https://github.com/SEMOSS/remote-client-server for RemoteClientServer implementation.
 */
public class RemoteClientServerZK {
	
	private static final Logger classLogger = LogManager.getLogger(RemoteClientServerZK.class);
	
	private static RemoteClientServerZK instance;

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

	private Boolean devPortFowarding = false;

	private RemoteClientServerZK() {
		classLogger.info("RemoteClientServerZK being initialized...");
	}

	public static RemoteClientServerZK getInstance() {
		if(instance != null) {
			return instance;
		}
		
		if (instance == null) {
			synchronized (RemoteClientServerZK.class){
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
			client = CuratorFrameworkFactory.builder()
					.connectString(zkServer)
					.retryPolicy(retryPolicy)
					.connectionTimeoutMs(5000)
					.sessionTimeoutMs(10000)
					.maxCloseWaitMs(2000)
					.build();

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
			} else {
				classLogger.error("Model scaler path {} does not exist in ZooKeeper", MODEL_SCALER_PATH);
			}

			// Load active models
			List<String> activeModels = client.getChildren().forPath(ACTIVE_PATH);
			for (String modelId : activeModels) {
				String path = ACTIVE_PATH + "/" + modelId;
				byte[] data = client.getData().forPath(path);
				if (data != null && data.length > 0) {
					try {
						JSONObject jsonData = new JSONObject(new String(data, "UTF-8"));
						String clusterIp = jsonData.getString("ip");
						String modelName = jsonData.getString("model_name");

						modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
						modelClusterIps.put(modelId, clusterIp);
						modelNames.put(modelId, modelName);
					} catch (Exception e) {
						// Fallback for backward compatibility
						String clusterIp = new String(data, "UTF-8");
						modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
						modelClusterIps.put(modelId, clusterIp);
						classLogger.warn("Model {} data not in JSON format, using legacy format", modelId);
					}
				}
			}

			// Load warming models
			List<String> warmingModels = client.getChildren().forPath(WARMING_PATH);
			for (String modelId : warmingModels) {
				modelStates.put(modelId, RemoteModelStateEnum.WARMING);
			}
		} catch (Exception e) {
			classLogger.error("Error loading initial state", e);
		}
	}


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
			CuratorCacheListener warmingListener = CuratorCacheListener.builder()
					.forCreates(node -> {
						String modelId = getModelIdFromPath(node.getPath());
						classLogger.info("Model {} entered warming state", modelId);
						modelStates.put(modelId, RemoteModelStateEnum.WARMING);
					})
					.forDeletes(node -> {
						String modelId = getModelIdFromPath(node.getPath());
						classLogger.info("Model {} left warming state", modelId);
						// Only update if not active
						if (!modelStates.get(modelId).equals(RemoteModelStateEnum.ACTIVE)) {
							modelStates.put(modelId, RemoteModelStateEnum.COLD);
						}
					})
					.build();
			warmingCache.listenable().addListener(warmingListener);
			warmingCache.start();

			// Set up cache for active path
			activeCache = CuratorCache.build(client, ACTIVE_PATH);
			CuratorCacheListener activeListener = CuratorCacheListener.builder()
					.forCreates(node -> {
						String modelId = getModelIdFromPath(node.getPath());
						classLogger.info("Model {} became active", modelId);
						modelStates.put(modelId, RemoteModelStateEnum.ACTIVE);
						// Extract cluster IP and model name from node data
						try {
							JSONObject jsonData = new JSONObject(new String(node.getData(), "UTF-8"));
							String clusterIp = jsonData.getString("ip");
							String modelName = jsonData.getString("model_name");

							modelClusterIps.put(modelId, clusterIp);
							modelNames.put(modelId, modelName);
							classLogger.info("Updated cluster IP for model {} ({}): {}", 
									modelId, modelName, clusterIp);
						} catch (Exception e) {
							// Fallback for backward compatibility
							try {
								String clusterIp = new String(node.getData(), "UTF-8");
								modelClusterIps.put(modelId, clusterIp);
								classLogger.info("Updated cluster IP for model {} using legacy format: {}", 
										modelId, clusterIp);
							} catch (Exception ex) {
								classLogger.error("Error extracting data for model {}", modelId, ex);
							}
						}
					})
					.forDeletes(node -> {
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
					})
					.build();
			activeCache.listenable().addListener(activeListener);
			activeCache.start();

		} catch (Exception e) {
			classLogger.error("Error setting up ZK caches", e);
		}
	}

	public String getModelName(String modelId) {
		return modelNames.get(modelId);
	}

	private String getModelIdFromPath(String path) {
		return path.substring(path.lastIndexOf('/') + 1);
	}

	public RemoteModelStateEnum getModelState(String modelId) {
		return modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD);
	}

	public String getModelClusterIp(String modelId) {
		return modelClusterIps.get(modelId);
	}

	public boolean isModelWarming(String modelId) {
		try {
			return client.checkExists().forPath(WARMING_PATH + "/" + modelId) != null;
		} catch (Exception e) {
			classLogger.error("Error checking warming state for model {}", modelId, e);
			return false;
		}
	}

	public boolean isModelActive(String modelId) {
		try {
			return client.checkExists().forPath(ACTIVE_PATH + "/" + modelId) != null;
		} catch (Exception e) {
			classLogger.error("Error checking active state for model {}", modelId, e);
			return false;
		}
	}

	// I need this because there is a period of time between when the model is on the active path but the FastAPI service is not quite ready
	private boolean checkModelHealth(String modelId) {
		String clusterIp = modelClusterIps.get(modelId);
		String modelName = modelNames.get(modelId);
		if (clusterIp == null) {
			classLogger.error("No cluster IP available for health check of model {} ({})", 
					modelId, modelName);
			return false;
		}
		String healthUrl = "";
		if (devPortFowarding) {
			healthUrl = "http://localhost:8888/api/health";
		} else {
			healthUrl = String.format("http://%s/api/health", clusterIp);
		}

		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(1000)
				.setSocketTimeout(1000)
				.build();

		try (CloseableHttpClient httpClient = HttpClients.custom()
				.setDefaultRequestConfig(requestConfig)
				.build()) {

			HttpGet httpGet = new HttpGet(healthUrl);

			try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode == 200) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						String responseString = EntityUtils.toString(entity);
						JSONObject healthResponse = new JSONObject(responseString);
						if ("ok".equals(healthResponse.optString("status"))) {
							return true;
						}
					}
				}
				classLogger.debug("Health check failed for model {} ({}): status code {}", 
						modelId, modelName, statusCode);
			}
		} catch (Exception e) {
			classLogger.debug("Health check failed for model {} ({}): {}", 
					modelId, modelName, e.getMessage());
		}
		return false;
	}

	public boolean waitForModelActive(String modelId, long timeoutMs) {
		try {
			long startTime = System.currentTimeMillis();
			boolean foundInActivePath = false;
			boolean isHealthy = false;

			// First wait for the model to appear in active path
			while (System.currentTimeMillis() - startTime < timeoutMs) {
				if (isModelActive(modelId)) {
					classLogger.info("Model {} was found in the active path", modelId);
					foundInActivePath = true;
					break;
				} else {
					classLogger.info("Model {} in a warming wait loop..", modelId);
				}
				Thread.sleep(3000);
			}

			if (!foundInActivePath) {
				classLogger.warn("Timeout waiting for model {} to appear in active path after {}ms", 
						modelId, timeoutMs);
				return false;
			}

			// Then wait for the service to become healthy
			long healthCheckStart = System.currentTimeMillis();
			long remainingTimeout = timeoutMs - (healthCheckStart - startTime);

			while (System.currentTimeMillis() - healthCheckStart < remainingTimeout) {
				if (checkModelHealth(modelId)) {
					classLogger.info("Model {} health check passed", modelId);
					isHealthy = true;
					break;
				}
				Thread.sleep(1000);
			}

			if (!isHealthy) {
				classLogger.warn("Timeout waiting for model {} to become healthy after appearing in active path", 
						modelId);
				return false;
			}

			return true;

		} catch (Exception e) {
			classLogger.error("Error waiting for model {} to become active", modelId, e);
			return false;
		}
	}

	/**
	 * Waits for a model to reach a specific state, with a timeout
	 * @param modelId The ID of the model to wait for
	 * @param desiredState The state to wait for
	 * @param timeoutMs Maximum time to wait in milliseconds
	 * @return true if the model reached the desired state within the timeout, false otherwise
	 */
	public boolean waitForState(String modelId, RemoteModelStateEnum desiredState, long timeoutMs) {
		try {
			long startTime = System.currentTimeMillis();
			while (System.currentTimeMillis() - startTime < timeoutMs) {
				RemoteModelStateEnum currentState = getModelState(modelId);
				if (currentState == desiredState) {
					return true;
				}
				// If we're waiting for ACTIVE but hit FAILED, break early
				if (desiredState == RemoteModelStateEnum.ACTIVE && 
						currentState == RemoteModelStateEnum.FAILED) {
					classLogger.error("Model {} failed while waiting for active state", modelId);
					return false;
				}
				Thread.sleep(1000); // Wait 1 second between checks
			}
			classLogger.warn("Timeout waiting for model {} to reach state {} after {}ms", 
					modelId, desiredState, timeoutMs);
			return false;
		} catch (Exception e) {
			classLogger.error("Error waiting for model {} to reach state {}", modelId, desiredState, e);
			return false;
		}
	}

	/**
	 * Gets a list of all active model IDs
	 * @return List of active model IDs
	 */
	public List<String> getActiveModels() {
		try {
			return client.getChildren().forPath(ACTIVE_PATH);
		} catch (Exception e) {
			classLogger.error("Error getting active models", e);
			return new ArrayList<>();
		}
	}

	/**
	 * Gets a list of all warming model IDs
	 * @return List of warming model IDs
	 */
	public List<String> getWarmingModels() {
		try {
			return client.getChildren().forPath(WARMING_PATH);
		} catch (Exception e) {
			classLogger.error("Error getting warming models", e);
			return new ArrayList<>();
		}
	}
}