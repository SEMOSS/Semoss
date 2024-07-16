package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.reactor.job.JobReactor;
import prerna.sablecc2.comm.JobManager;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class StreamRESTFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(StreamRESTFunctionEngine.class);

	private String httpMethod;
	private String url;
	private Map<String, String> headers;
	
	private String contentType = "JSON";
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.httpMethod = smssProp.getProperty("HTTP_METHOD");
		if(this.httpMethod == null 
				|| (this.httpMethod=this.httpMethod.trim().toUpperCase()).isEmpty()
				|| (!this.httpMethod.equals("GET") && !this.httpMethod.equals("POST") 
						&& !this.httpMethod.equals("PUT") && !this.httpMethod.equals("HEAD")
						)) {
			throw new IllegalArgumentException("RESTFunctionEngine only supports GET, HEAD, POST, or PUT requests");
		}
		
		this.url = smssProp.getProperty("URL");
		if(this.url == null 
				|| (this.url=this.url.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide a URL");
		}
		Utility.checkIfValidDomain(url);
		
		String headersStr = smssProp.getProperty("HEADERS");
		if(headersStr!= null && !(headersStr=headersStr.trim()).isEmpty()) {
			this.headers = new Gson().fromJson(headersStr, new TypeToken<Map<String, String>>() {}.getType());
		}
		
		if(smssProp.containsKey("CONTENT_TYPE")) {
			this.contentType = smssProp.getProperty("CONTENT_TYPE");
		}
	}
	
	@Override
	public void close() throws IOException {
		// i dont have anything to do here...
		
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		String jobId = (String) parameterValues.remove(JobReactor.JOB_KEY);
		if(jobId == null) {
			throw new IllegalArgumentException("Must provide the job id for streaming output");
		}
		
		// validate all the required keys are set
		if(this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for(String requiredP : this.requiredParameters) {
				if(!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if(!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}
		
		// store the responses combined
        StringBuilder responseAssimilator = new StringBuilder();
		String responseData = null;

		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpHelperUtility.getCustomClient(null, null, null, null);
			response = getResponse(httpClient, parameterValues);
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
            	 // Handle streaming response
                if (entity != null) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"))) {
                        String line;
                        
                        while ((line = reader.readLine()) != null) {
                        	responseAssimilator.append(line);
                            JobManager.getManager().addPartialOut(jobId, line);
                        }
                        
                        // return the combined outputs
                        return responseAssimilator;
                    } catch (Exception e) {
            	        classLogger.error(Constants.STACKTRACE, e);
            	        throw new IllegalArgumentException("There was an error processing the response from " + url);
            	    }
                }
            } else {
                responseData = entity != null ? EntityUtils.toString(entity, "UTF-8") : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return responseData;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(response != null) {
				try {
					response.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param httpClient
	 * @param parameterValues
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	private CloseableHttpResponse getResponse(CloseableHttpClient httpClient, Map<String, Object> parameterValues) throws ClientProtocolException, IOException {
		CloseableHttpResponse response = null;
		if(httpMethod.equalsIgnoreCase("GET")) {
			StringBuffer queryString = new StringBuffer();
			boolean first = true;
			for(String k : parameterValues.keySet()) {
				if(!first) {
					queryString.append("&");
				}
				queryString.append(k).append("=").append(parameterValues.get(k));
				first = false;
			}
			String runTimeUrl = url + "?" + queryString;
			
			HttpGet httpGet = new HttpGet(runTimeUrl);
			addHeaders(httpGet);
			response = httpClient.execute(httpGet);
		} else if(httpMethod.equalsIgnoreCase("HEAD")) {
			StringBuffer queryString = new StringBuffer();
			boolean first = true;
			for(String k : parameterValues.keySet()) {
				if(!first) {
					queryString.append("&");
				}
				queryString.append(k).append("=").append(parameterValues.get(k));
				first = false;
			}
			String runTimeUrl = url + "?" + queryString;

			HttpHead httpHead = new HttpHead(runTimeUrl);
			addHeaders(httpHead);
			response = httpClient.execute(httpHead);
		} else if(httpMethod.equalsIgnoreCase("PUT")) {
			HttpPut httpPut = new HttpPut(url);
			addHeaders(httpPut);

			if(parameterValues != null && !parameterValues.isEmpty()) {
				if(this.contentType.equalsIgnoreCase("JSON")) {
					httpPut.setEntity(new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
				} else {
					List<NameValuePair> params = new ArrayList<NameValuePair>();
					for(String key : parameterValues.keySet()) {
						params.add(new BasicNameValuePair(key, parameterValues.get(key)+""));
					}
					httpPut.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
				}
			}
			response = httpClient.execute(httpPut);
		} else {
			HttpPost httpPost = new HttpPost(url);
			addHeaders(httpPost);

			if(parameterValues != null && !parameterValues.isEmpty()) {
				if(this.contentType.equalsIgnoreCase("JSON")) {
					httpPost.setEntity(new StringEntity(new Gson().toJson(parameterValues), ContentType.APPLICATION_JSON));
				} else {
					List<NameValuePair> params = new ArrayList<NameValuePair>();
					for(String key : parameterValues.keySet()) {
						params.add(new BasicNameValuePair(key, parameterValues.get(key)+""));
					}
					httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
				}
			}
			response = httpClient.execute(httpPost);
		}
			
		return response;
	}
	
	/**
	 * Add headers to a request
	 * @param requestMethod
	 */
	private void addHeaders(HttpRequestBase requestMethod) {
		if(this.headers != null && !this.headers.isEmpty()) {
			for(String key : this.headers.keySet()) {
				requestMethod.addHeader(key, this.headers.get(key));
			}
		}
	}
	
}
