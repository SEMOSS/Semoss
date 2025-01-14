package prerna.cluster.util;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
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
	                
	                classLogger.info("Loaded active model {} ({}) with IP: {} from ZK.", 
	                    modelId, modelName, clusterIp);
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
				            
				            JSONObject jsonData = new JSONObject(rawData);
				            String clusterIp = jsonData.getString("ip");
				            String modelName = jsonData.getString("model_name");	      
				            
				            modelClusterIps.put(modelId, clusterIp);
				            modelNames.put(modelId, modelName);
				            
				            classLogger.info("Successfully registered model {} ({}) with IP: {}", 
				                modelId, modelName, clusterIp);
				        } catch (Exception e) {
				            classLogger.error("Error processing data for model {}: {}", modelId, e.getMessage(), e);
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
	
	private byte[] getNodeDataWithRetry(String path, int maxRetries, long delayMs) {
	    for (int i = 0; i < maxRetries; i++) {
	        try {
	            byte[] data = client.getData().forPath(path);
	            if (data != null && data.length > 0) {
	                return data;
	            }
	            Thread.sleep(delayMs);
	        } catch (Exception e) {
	            classLogger.debug("Attempt {} to read data from {} failed: {}", 
	                i + 1, path, e.getMessage());
	        }
	    }
	    return null;
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
	
	private String getModelPath(String modelId) {
	    return modelId;
	}

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
	public boolean isModelWarming(String modelId) {
	    try {
	        String modelPath = getModelPath(modelId);
	        return client.checkExists().forPath(WARMING_PATH + "/" + modelPath) != null;
	    } catch (Exception e) {
	        classLogger.error("Error checking warming state for model {}", modelId, e);
	        return false;
	    }
	}

	public boolean isModelActive(String modelId) {
	    try {
	        String modelPath = getModelPath(modelId);
	        return client.checkExists().forPath(ACTIVE_PATH + "/" + modelPath) != null;
	    } catch (Exception e) {
	        classLogger.error("Error checking active state for model {}", modelId, e);
	        return false;
	    }
	}

	// I need this because there is a period of time between when the model is on the active path but the FastAPI service is not quite ready
	private boolean checkModelHealth(String modelId) {
	    String clusterIp = modelClusterIps.get(modelId);
	    String modelName = modelNames.get(modelId);
	    
	    if (clusterIp == null || clusterIp.trim().isEmpty()) {
	        classLogger.error("No valid cluster IP available for health check of model {} ({})", 
	                modelId, modelName);
	        return false;
	    }
	    
	    String healthUrl = devPortFowarding ? 
	        "http://localhost:8888/api/health" :
	        String.format("http://%s/api/health", clusterIp);
	        
	    classLogger.info("Attempting health check at URL: {}", healthUrl);
	    
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
			}
		} catch (Exception e) {
			classLogger.error("Health check failed for model {} ({}): {}", 
					modelId, modelName, e.getMessage());
		}
		return false;
	}

	public boolean waitForModelActive(String modelId, long timeoutMs) {
		try {
			long startTime = System.currentTimeMillis();
			boolean foundInActivePath = false;
			boolean isHealthy = false;

			// Waiting for model to appear in active path
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

			// Wait for container to be healthy
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

	public class RemoteModelInfo {
	    private final String id;
	    private final String name;
	    private final RemoteModelStateEnum state;

	    public RemoteModelInfo(String id, String name, RemoteModelStateEnum state) {
	        this.id = id;
	        this.name = name;
	        this.state = state;
	    }

	    // Getters
	    public String getId() { return id; }
	    public String getName() { return name; }
	    public RemoteModelStateEnum getState() { return state; }
	}

	/**
	 * Gets a list of all active models with their associated information
	 * @return List of ModelInfo objects containing model details
	 */
	public List<RemoteModelInfo> getActiveModels() {
	    List<RemoteModelInfo> activeModels = new ArrayList<>();
	    
	    try {
	        List<String> activeModelIds = client.getChildren().forPath(ACTIVE_PATH);
	        
	        for (String modelId : activeModelIds) {
	            String name = modelNames.get(modelId);
	            RemoteModelStateEnum state = modelStates.getOrDefault(modelId, RemoteModelStateEnum.COLD);
	            
	            if (name != null) {
	                activeModels.add(new RemoteModelInfo(
	                    modelId,
	                    name,
	                    state
	                ));
	            } else {
	                classLogger.warn("Incomplete data for active model {}: name={}", modelId, name);
	                    
	                String fullPath = ACTIVE_PATH + "/" + modelId;
	                byte[] data = client.getData().forPath(fullPath);
	                if (data != null && data.length > 0) {
	                    String rawData = new String(data, "UTF-8");
	                    JSONObject jsonData = new JSONObject(rawData);
	                    
	                    name = jsonData.getString("model_name");
	                    
	                    activeModels.add(new RemoteModelInfo(
	                        modelId,
	                        name,
	                        state
	                    ));
	                    
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
	 * @return List of RemoteModelInfo objects containing model details
	 */
	public List<RemoteModelInfo> getWarmingModels() {
	    List<RemoteModelInfo> warmingModels = new ArrayList<>();
	    
	    try {
	        List<String> warmingModelIds = client.getChildren().forPath(WARMING_PATH);
	        
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
	
	public Map<String, Object> canItRun(String hfModelId) throws Exception {
	    String modelScalerIp = getModelScalerIp();
	    if (modelScalerIp == null) {
	        classLogger.error("Unable to get model scaler IP from ZooKeeper");
	        throw new RuntimeException("Failed to get model scaler IP");
	    }
	    
	    String canItRunUrl;
	    if (devPortFowarding) {
	        canItRunUrl = "http://localhost:8000/api/can-it-run";
	    } else {
	        canItRunUrl = String.format("http://%s/api/can-it-run", modelScalerIp);
	    }

	    RequestConfig requestConfig = RequestConfig.custom()
	            .setConnectTimeout(5000)
	            .setSocketTimeout(5000)
	            .build();

	    try (CloseableHttpClient httpClient = HttpClients.custom()
	            .setDefaultRequestConfig(requestConfig)
	            .build()) {

	        HttpPost httpPost = new HttpPost(canItRunUrl);
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
	                
	                classLogger.info("Successfully checked compatibility for model: {} - Can run: {}", 
	                    hfModelId, result.get("can_run"));
	                return result;
	            } else {
	                JSONObject errorResponse = new JSONObject(responseString);
	                String errorMessage = errorResponse.getJSONObject("detail").getString("message");
	                classLogger.error("Error checking model compatibility: {} (Status: {})", 
	                    errorMessage, statusCode);
	                throw new RuntimeException("Failed to check model compatibility: " + errorMessage);
	            }
	        }
	    } catch (Exception e) {
	        classLogger.error("Error making request to model scaler: {}", e.getMessage(), e);
	        throw new RuntimeException("Failed to check model compatibility", e);
	    }
	}
}