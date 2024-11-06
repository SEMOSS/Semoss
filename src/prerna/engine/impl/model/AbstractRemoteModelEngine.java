package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
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
import org.json.JSONException;
import java.nio.charset.StandardCharsets;

import prerna.cluster.util.clients.AppCloudClientProperties;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.engine.api.RemoteModelStateEnum;


public class AbstractRemoteModelEngine extends AbstractModelEngine {
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	private volatile RemoteModelStateEnum modelState = RemoteModelStateEnum.COLD;
	private String clusterIp;

	private static AbstractRemoteModelEngine sync = null;
	
	public static final String ZK_SERVER_STRING = "ZK_SERVER";
	public static final String HOST_IP = "HOST_IP";
	private static final String WARMING_PATH = "/models/warming";
	private static final String ACTIVE_PATH = "/models/active";
	
	private CuratorFramework client = null;
	private CuratorCache warmingCache;
	private CuratorCache activeCache;
	
	private String deployerEndpoint;
	protected String model;
	
	String host;
	
	public AbstractRemoteModelEngine() {
		initializeEngine();
	}
	
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
	}
	
    protected synchronized RemoteModelStateEnum getModelState() {
        return modelState;
    }
    
    protected synchronized void setModelState(RemoteModelStateEnum newState) {
    	RemoteModelStateEnum oldState = this.modelState;
        this.modelState = newState;
        classLogger.info("Model {} state transition: {} -> {}", this.engineId, oldState, newState);
    }
    
    private void handleModelStateChange(PathChildrenCacheEvent event, String changeType) {
        try {
            String path = event.getData().getPath();
            String modelId = path.substring(path.lastIndexOf('/') + 1);
            
            if (!modelId.equals(this.engineId)) {
                return;
            }

            synchronized (this) {
                switch (changeType) {
                    case "ADDED":
                        handleModelAdded(path);
                        break;
                    case "REMOVED":
                        handleModelRemoved(path);
                        break;
                    case "UPDATED":
                        handleModelUpdated(path);
                        break;
                }
            }
        } catch (Exception e) {
            classLogger.error("Error handling model state change", e);
            setModelState(RemoteModelStateEnum.UNKNOWN);
        }
    }
    
    private void handleModelAdded(String path) throws Exception {
        if (path.startsWith(WARMING_PATH)) {
            setModelState(RemoteModelStateEnum.WARMING);
        } else if (path.startsWith(ACTIVE_PATH)) {
            // Get the data (cluster IP) from the ZNode
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                this.clusterIp = new String(data, "UTF-8");
            }
            setModelState(RemoteModelStateEnum.ACTIVE);
        }
    }

    private void handleModelRemoved(String path) throws Exception {
        if (path.startsWith(WARMING_PATH)) {
            if (isModelActive()) {
                setModelState(RemoteModelStateEnum.ACTIVE);
            } else {
                setModelState(RemoteModelStateEnum.FAILED);
            }
        } else if (path.startsWith(ACTIVE_PATH)) {
            this.clusterIp = null; // Clear cluster IP when model no longer active
            if (isModelWarming()) {
                setModelState(RemoteModelStateEnum.WARMING);
            } else {
                setModelState(RemoteModelStateEnum.COLD);
            }
        }
    }

    private void handleModelUpdated(String path) throws Exception {
        if (path.startsWith(WARMING_PATH)) {
            setModelState(RemoteModelStateEnum.WARMING);
        } else if (path.startsWith(ACTIVE_PATH)) {
            // Get the updated data (cluster IP) from the ZNode
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                this.clusterIp = new String(data, "UTF-8");
            }
            setModelState(RemoteModelStateEnum.ACTIVE);
        }
    }
    
    public RemoteModelStateEnum getCurrentModelState() throws Exception {
    	RemoteModelStateEnum currentState = getModelState();
        
        // Verify against ZK
        if (isModelActive()) {
            return RemoteModelStateEnum.ACTIVE;
        } else if (isModelWarming()) {
            return RemoteModelStateEnum.WARMING;
        } else if (currentState == RemoteModelStateEnum.FAILED) {
            return RemoteModelStateEnum.FAILED;
        } else {
        	boolean success = initiateAndWaitForDeployment(120000);
        	if(success) {
        		classLogger.info("Model {} is now active", this.engineId);
        	} else {
        		classLogger.error("Model {} failed to deploy", this.engineId);
        	}
            return RemoteModelStateEnum.COLD;
        }
    }
    
    public boolean waitForState(RemoteModelStateEnum desiredState, long timeoutMs) throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (getCurrentModelState() == desiredState) {
                return true;
            }
            Thread.sleep(1000);
        }
        return false;
    }

	private void initializeEngine() {
		classLogger.info("Starting up the Remote Model Engine...");
		
		AppCloudClientProperties clientProps = new AppCloudClientProperties();
		
		// what is the zk server ip 
//		String zk_server = clientProps.get(ZK_SERVER_STRING);
		String zk_server = "localhost:2181";
		
		if (zk_server == null || zk_server.isEmpty()) {
			throw new IllegalArgumentException("Zookeeper Server endpoint is not defined");
		}
		
		// what is the host ip of the container/pod/box - this is used as a unique id for the container/singleton
		host = clientProps.get(HOST_IP);
		if (host == null || host.isEmpty()) {
			classLogger.info("Host IP is not set");
		   host="node_"+Utility.getRandomString(5);
		}
		
		try {
			client =  CuratorFrameworkFactory.newClient(zk_server, new RetryNTimes(3, 10));
			client.start();
			
	        // Check if the ZNode exists before trying to create it - project
	        if (client.checkExists().forPath(WARMING_PATH) == null) {
	            client.create().creatingParentsIfNeeded().forPath(WARMING_PATH);
	        }
	        
	        // Check if the ZNode exists before trying to create it - engine
	        if (client.checkExists().forPath(ACTIVE_PATH) == null) {
	            client.create().creatingParentsIfNeeded().forPath(ACTIVE_PATH);
	        }
	        
	        warmingCache = createCacheListener(WARMING_PATH);
	        activeCache = createCacheListener(ACTIVE_PATH);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	private CuratorCache createCacheListener(String pathToWatch) {
	    CuratorCache cache = CuratorCache.build(client, pathToWatch);
	    CuratorCacheListener listener = CuratorCacheListener.builder()
	            .forPathChildrenCache(pathToWatch, client, new PathChildrenCacheListener() {
	                @Override
	                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
	                    switch (event.getType()) {
	                        case CHILD_ADDED:
	                            handleModelStateChange(event, "ADDED");
	                            break;
	                        case CHILD_REMOVED:
	                            handleModelStateChange(event, "REMOVED");
	                            break;
	                        case CHILD_UPDATED:
	                            handleModelStateChange(event, "UPDATED");
	                            break;
	                    }
	                }
	            })
	            .build();
	    cache.listenable().addListener(listener);
	    cache.start();

	    return cache;
	}

	private void updateClusterIp(String path) throws Exception {
	    try {
	        byte[] data = client.getData().forPath(path);
	        if (data != null && data.length > 0) {
	            this.clusterIp = new String(data, "UTF-8");
	            classLogger.info("Updated cluster IP for model {}: {}", this.engineId, this.clusterIp);
	        } else {
	            classLogger.warn("No cluster IP data found for model {} at path {}", this.engineId, path);
	        }
	    } catch (Exception e) {
	        classLogger.error("Error getting cluster IP for model {} at path {}", this.engineId, path, e);
	        throw e;
	    }
	}

	public boolean isModelActive() throws Exception {
	    String path = ACTIVE_PATH + "/" + this.engineId;
	    if (client.checkExists().forPath(path) != null) {
	        updateClusterIp(path);
	        return true;
	    }
	    return false;
	}

	public boolean waitForModelActive(long timeoutMs) throws Exception {
	    long startTime = System.currentTimeMillis();
	    while (System.currentTimeMillis() - startTime < timeoutMs) {
	        if (isModelActive()) {
	            return true;
	        }
	        Thread.sleep(1000);
	    }
	    return false;
	}
	
	private CompletableFuture<Boolean> deployModel() {
		classLogger.info("Deploying model {} with engine ID {}", this.model, this.engineId);
	    
	    return CompletableFuture.supplyAsync(() -> {
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
	}

	// initiate deployment and wait for model to become active
	protected boolean initiateAndWaitForDeployment(long timeoutMs) {
	    try {
	        // First check if the model is already active
	        if (isModelActive()) {
	            classLogger.info("Model {} is already active", this.engineId);
	            return true;
	        }
	        
	        if (isModelWarming()) {
	            classLogger.info("Model {} is already warming, waiting for activation", this.engineId);
	            return waitForModelActive(timeoutMs);
	        }
	        
	        // Initiate deployment
	        CompletableFuture<Boolean> deploymentFuture = deployModel();
	        
	        // Wait for deployment initiation to complete
	        if (!deploymentFuture.get(30, TimeUnit.SECONDS)) {
	            classLogger.error("Failed to initiate deployment for model {}", this.engineId);
	            return false;
	        }
	        return waitForModelActive(timeoutMs);
	    } catch (Exception e) {
	        classLogger.error("Error during model deployment process", e);
	        return false;
	    }
	}
	
	protected JSONObject makeModelRequest(JSONObject requestPayload) throws Exception {
	    // Get current state and handle warming/cold states
	    RemoteModelStateEnum currentState = getCurrentModelState();
	    
	    // If model is warming, wait for it to become active
	    if (currentState == RemoteModelStateEnum.WARMING) {
	        classLogger.info("Model {} is warming, waiting for activation...", this.engineId);
	        boolean becameActive = waitForState(RemoteModelStateEnum.ACTIVE, 300000); // 5 minute timeout
	        if (!becameActive) {
	            classLogger.error("Model {} failed to become active after warming", this.engineId);
	            return null;
	        }
	        currentState = RemoteModelStateEnum.ACTIVE;
	    }
	    // If not active after handling warming state, return null
	    if (currentState != RemoteModelStateEnum.ACTIVE) {
	        classLogger.error("Model {} is not active. Current state: {}", this.engineId, currentState);
	        return null;
	    }

	    if (clusterIp == null) {
	        classLogger.error("No cluster IP available for model {}", this.engineId);
	        return null;
	    }

	    // Construct the URL using cluster IP
//	    String url = String.format("http://%s:8888/api/generate", clusterIp);
	    String url = "http://localhost:8888/api/generate"; // TEMP FOR LOCAL DEVELOPMENT
	    
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
	    } catch (IOException e) {
	        classLogger.error("HTTP request failed", e);
	    } catch (JSONException e) {
	        classLogger.error("Failed to decode JSON response", e);
	    } catch (Exception e) {
	        classLogger.error("Unexpected error", e);
	    }

	    return null;
	}
	
	public boolean isModelWarming() throws Exception {
	    return client.checkExists().forPath(WARMING_PATH + "/" +  this.engineId) != null;
	}
	
	public List<String> getActiveModels() throws Exception {
	    return client.getChildren().forPath(ACTIVE_PATH);
	}

	public List<String> getWarmingModels() throws Exception {
	    return client.getChildren().forPath(WARMING_PATH);
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
