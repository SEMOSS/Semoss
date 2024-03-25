package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.impl.model.responses.IModelEngineResponseHandler;
import prerna.engine.impl.model.responses.IModelEngineResponseStreamHandler;
import prerna.sablecc2.comm.JobManager;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;

public abstract class RESTModelEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(RESTModelEngine.class);
	
	protected static final String ENDPOINT = "ENDPOINT";
	
	protected ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture = null; 				// Holds the future of the scheduled task
	protected Runnable timeoutAction = this::resetAfterTimeout;
	private long timeoutDelay;										// Delay after which the timeoutMethod is called

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		String timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT, "30");
		this.timeoutDelay = Long.parseLong(timeout);
		this.scheduler = Executors.newScheduledThreadPool(1);
	}
	
	/**
	 * This method is responsible for resetting the timeout window between REST calls.
	 */
	protected synchronized void resetTimer() {
		if (scheduledFuture != null && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
        }
		
		scheduledFuture = scheduler.schedule(timeoutAction, timeoutDelay, TimeUnit.MINUTES);
    }
	
	/**
	 * This method defined what should happen when the timeout is reached. 
	 * Currently this is an abstract method until conversation history / chains are standardized.
	 */
	protected abstract void resetAfterTimeout();
	    
	@Override
	public void close() throws IOException {
		this.scheduler.shutdown();
	}
	
	/**
	 * 
	 * @param url
	 * @param headersMap
	 * @param body
	 * @param contentType
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @param isStream
	 * @param responseType
	 * @param insightId
	 * @return
	 */
	protected IModelEngineResponseHandler postRequestStringBody(String url, Map<String, String> headersMap, String body, ContentType contentType, 
			String keyStore, String keyStorePass, String keyPass, 
			boolean isStream, Class<? extends IModelEngineResponseHandler> responseType, String insightId) {
		CloseableHttpClient httpClient = null;
	    CloseableHttpResponse response = null;
	    try {
	        httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
	        HttpPost httpPost = new HttpPost(url);
	        if (headersMap != null && !headersMap.isEmpty()) {
	            for (String key : headersMap.keySet()) {
	                httpPost.addHeader(key, headersMap.get(key));
	            }
	        }
	        if (body != null && !body.isEmpty()) {
	            httpPost.setEntity(new StringEntity(body, contentType));
	        }
	        response = httpClient.execute(httpPost);

	        int statusCode = response.getStatusLine().getStatusCode();
	        if (statusCode >= 200 && statusCode < 300) {
	            HttpEntity entity = response.getEntity();
	            if (!isStream) {
	                // Handle regular response
	                String responseData = entity != null ? EntityUtils.toString(entity) : null;
	                return handleDeserialization(responseData, responseType);
	            } else {
	                // Handle streaming response
	                if (entity != null) {
	                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()))) {
	                        String line;
	                        StringBuilder responseAssimilator = new StringBuilder();
	                        IModelEngineResponseHandler responseObject = responseType.newInstance();
	                        
	                        while ((line = reader.readLine()) != null) {
	                            if (line.contains("data: [DONE]")) {
	                                break;
	                            }
	                            
	                            if (line.startsWith("data: ")) {
	                                // Extract JSON part
	                                String jsonPart = line.substring("data: ".length());
	                                IModelEngineResponseStreamHandler partialObject = new Gson().fromJson(jsonPart, responseObject.getStreamHandlerClass());
	                                Object partial = partialObject.getPartialResponse();
	                                
	                                if (partial != null) {
	                                	responseObject.appendStream(partialObject);
		                                JobManager.getManager().addPartialOut(insightId, partial+"");
		                                responseAssimilator.append(partial);
	                                }
	                            }
	                        }
	                        responseObject.setResponse(responseAssimilator.toString());
	                        return responseObject;
	                    } catch (Exception e) {
	            	        classLogger.error(Constants.STACKTRACE, e);
	            	        throw new IllegalArgumentException("There was an error processing the response from " + url);
	            	    }
	                }
	            }
	        } else {
	        	// try to send back the error from the server
	            String errorResponse = EntityUtils.toString(response.getEntity());
	            throw new IllegalArgumentException("Connected to " + url + " but received error = " + errorResponse);
	        }
	    } catch (IOException e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        throw new IllegalArgumentException("Could not connect to URL at " + url);
	    } finally {
	        try {
	            if (response != null) {
	                response.close();
	            }
	            if (httpClient != null) {
	                httpClient.close();
	            }
	        } catch (IOException e) {
	            classLogger.error("Error while closing resources", e);
	        }
	    }
	    return null; // In case of unexpected flow
	}
	
	/**
	 * This method is intended to be overridden in an implementing class
	 * if the responseData requires more unique deserialization than gson.fromJson() can provide
	 * 
	 * @param responseData
	 * @param responseType
	 * @return
	 */
	protected IModelEngineResponseHandler handleDeserialization(String responseData, Class<? extends IModelEngineResponseHandler> responseType) {
		IModelEngineResponseHandler responseObject = new Gson().fromJson(responseData, responseType);
        return responseObject;
	}
}
