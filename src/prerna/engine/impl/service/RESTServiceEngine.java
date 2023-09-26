package prerna.engine.impl.service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IServiceEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public class RESTServiceEngine implements IServiceEngine {

	private static final Logger classLogger = LogManager.getLogger(RESTServiceEngine.class);

	private String engineId;
	private String engineName;
	
	private String smssFilePath;
	private Properties smssProp;
	
	private String httpMethod;
	private String url;
	private Map<String, String> headers;
	private List<String> executeInputNames;
	
	private String contentType = "JSON";
	
	@Override
	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	@Override
	public String getEngineId() {
		return this.engineId;
	}

	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return this.engineName;
	}

	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		
		this.httpMethod = smssProp.getProperty("HTTP_METHOD");
		if(this.httpMethod == null 
				|| (this.httpMethod=this.httpMethod.trim().toUpperCase()).isEmpty()
				|| (!this.httpMethod.equals("GET") && !this.httpMethod.equals("POST"))) {
			throw new IllegalArgumentException("RESTServiceEngine only supports GET or POST requests");
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
		
		String executeInputNamesStr = smssProp.getProperty("EXECUTE_INPUT_NAMES");
		if(executeInputNamesStr!= null && !(executeInputNamesStr=executeInputNamesStr.trim()).isEmpty()) {
			this.executeInputNames = new Gson().fromJson(executeInputNamesStr, new TypeToken<List<String>>() {}.getType());
		}
		
		if(smssProp.containsKey("CONTENT_TYPE")) {
			this.contentType = smssProp.getProperty("CONTENT_TYPE");
		}
	}
	
	@Override
	public void delete() throws IOException {
		classLogger.debug("Delete database engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch(IOException e) {
			classLogger.warn("Error occurred trying to close service engine");
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		File engineFolder =new File(
				DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "/" + Constants.SERVICE_FOLDER 
				+ "/" + SmssUtilities.getUniqueName(this.engineName, this.engineId));

		try {
			FileUtils.deleteDirectory(engineFolder);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		classLogger.debug("Deleting smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.engineId);
	}
	
	@Override
	public void close() throws IOException {
		// i dont have anything to do here...
		
	}

	@Override
	public Object execute(Object[] args) {
		Object output = null;
		if(httpMethod.equalsIgnoreCase("GET")) {
			// for GET, will assume you have placeholders to replace based on the executeInputNames
			if(executeInputNames != null && !executeInputNames.isEmpty()) {
				for(int i = 0; i < executeInputNames.size(); i++) {
					url = url.replace(executeInputNames.get(i), args[i] + "");
				}
			}
			output = AbstractHttpHelper.getRequest(this.url, this.headers, null, null, null);
		} else {
			// for POST, will assume we are constructing a JSON body
			Map<String, String> bodyMap = null;
			if(executeInputNames != null && !executeInputNames.isEmpty()) {
				bodyMap = new HashMap<>();
				for(int i = 0; i < executeInputNames.size(); i++) {
					bodyMap.put(executeInputNames.get(i), args[i] + "");
				}
			}
			if(this.contentType.equalsIgnoreCase("JSON")) {
				output = AbstractHttpHelper.postRequestStringBody(this.url, this.headers, new Gson().toJson(bodyMap), ContentType.APPLICATION_JSON, null, null, null);
			} else {
				output = AbstractHttpHelper.postRequestUrlEncodedBody(this.url, this.headers, bodyMap, null, null, null);
			}
		}
		return output;
	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}

	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
	}

	@Override
	public Properties getSmssProp() {
		return this.smssProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		return this.smssProp;
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.SERVICE;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return null;
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
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
		tempSmss.put("EXECUTE_INPUT_NAMES", "['number1','number2']");
		tempSmss.put("CONTENT_TYPE", "JSON");
		RESTServiceEngine engine = new RESTServiceEngine();
		engine.open(tempSmss);
		Object output = engine.execute(new Object[] {1,2});
		System.out.println("My output = " + output);
		engine.close();
	}
	

}
