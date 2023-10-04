package prerna.engine.impl.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IFunctionEngine;
import prerna.security.AbstractHttpHelper;
import prerna.util.Utility;

public class RESTFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(RESTFunctionEngine.class);

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
				|| (!this.httpMethod.equals("GET") && !this.httpMethod.equals("POST"))) {
			throw new IllegalArgumentException("RESTFunctionEngine only supports GET or POST requests");
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
		Object output = null;
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
			output = AbstractHttpHelper.getRequest(runTimeUrl, this.headers, null, null, null);
		} else {
			// for POST, will assume we are constructing a JSON body
			Map<String, String> bodyMap = new HashMap<>();
			for(String k : parameterValues.keySet()) {
				bodyMap.put(k, parameterValues.get(k) + "");
			}
			if(this.contentType.equalsIgnoreCase("JSON")) {
				output = AbstractHttpHelper.postRequestStringBody(this.url, this.headers, new Gson().toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null);
			} else {
				output = AbstractHttpHelper.postRequestUrlEncodedBody(this.url, this.headers, bodyMap, null, null, null);
			}
		}
		return output;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Testing
	 */
	
	public static void main(String[] args) throws Exception {
		Properties tempSmss = new Properties();
		tempSmss.put("URL", "http://127.0.0.1:5000/runML");
		tempSmss.put("HTTP_METHOD", "post");
		tempSmss.put("HEADERS", "{Content-Type: 'application/json'}");
		tempSmss.put("CONTENT_TYPE", "JSON");
		tempSmss.put(IFunctionEngine.NAME_KEY, "myExampleExecution");
		tempSmss.put(IFunctionEngine.DESCRIPTION_KEY, "Perform addition");
		List<FunctionParameter> parameters = new ArrayList<>();
		parameters.add(new FunctionParameter("number1", "double", "the first number to use"));
		parameters.add(new FunctionParameter("number2", "double", "the second number to use"));
		tempSmss.put(IFunctionEngine.PARAMETER_KEY, new Gson().toJson(parameters));
		tempSmss.put(IFunctionEngine.REQUIRED_PARAMETER_KEY, new Gson().toJson(Arrays.asList("number1", "number2")));
		RESTFunctionEngine engine = new RESTFunctionEngine();
		engine.open(tempSmss);
		Map<String, Object> execMap = new HashMap<>();
		execMap.put("number1", 1);
		execMap.put("number2", 2);
		Object output = engine.execute(execMap);
		System.out.println("My output = " + output);
		engine.close();
	}

}
