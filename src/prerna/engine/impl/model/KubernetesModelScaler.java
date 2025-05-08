package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.cluster.util.IRemoteClientServer;
import prerna.cluster.util.RemoteClientServerZKRESTProxy;
import prerna.cluster.util.ZKClientFactory;

public class KubernetesModelScaler {
	private static final Logger classLogger = LogManager.getLogger(KubernetesModelScaler.class);
	
	private static KubernetesModelScaler instance;

	private KubernetesModelScaler() {
		classLogger.info("KubernetesModelScaler being initialized...");
	}
	
	public static KubernetesModelScaler getInstance() {
		if(instance != null) {
			return instance;
		}
		
		if (instance == null) {
			synchronized (KubernetesModelScaler.class){
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
			classLogger.info("KMS_INGRESS environment variable not found and devPortforwarding not set, using ZooKeeper for KMS IP resolution. This is correct for production deployments.");
			this.kmsUrl = zkClient.getModelScalerIp();
		}
		
	}
	
	public Map<String, Object> canItRun(String hfModelId) throws Exception {
	
		String serviceUrl = this.kmsUrl + "/api/can-it-run";
		
	    RequestConfig requestConfig = RequestConfig.custom()
	            .setConnectTimeout(5000)
	            .setSocketTimeout(5000)
	            .build();

	    try (CloseableHttpClient httpClient = HttpClients.custom()
	            .setDefaultRequestConfig(requestConfig)
	            .build()) {

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
	

