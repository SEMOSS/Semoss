package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public abstract class AbstractFunctionEngine implements IFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractFunctionEngine.class);

	private String engineId;
	private String engineName;
	
	private String smssFilePath;
	private Properties smssProp;
	
	protected String functionName;
	protected String functionDescription;
	protected List<FunctionParameter> parameters;
	protected List<String> requiredParameters;
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);

		if(!smssProp.containsKey(IFunctionEngine.NAME_KEY)) {
			throw new IllegalArgumentException("Must have key " + IFunctionEngine.NAME_KEY + " in SMSS");
		}
		if(!smssProp.containsKey(IFunctionEngine.DESCRIPTION_KEY)) {
			throw new IllegalArgumentException("Must have key " + IFunctionEngine.DESCRIPTION_KEY + " in SMSS");
		}

		this.functionName = smssProp.getProperty(IFunctionEngine.NAME_KEY);
		this.functionDescription = smssProp.getProperty(IFunctionEngine.DESCRIPTION_KEY);
		
		if(smssProp.containsKey(IFunctionEngine.PARAMETER_KEY)) {
			this.parameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.PARAMETER_KEY), new TypeToken<List<FunctionParameter>>() {}.getType());
		}
		
		if(smssProp.containsKey(IFunctionEngine.REQUIRED_PARAMETER_KEY)) {
			this.requiredParameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY), new TypeToken<List<String>>() {}.getType());
		}
	}

	@Override
	public void delete() throws IOException {
		classLogger.debug("Delete function engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch(IOException e) {
			classLogger.warn("Error occurred trying to close service engine");
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		File engineFolder = new File(
				EngineUtility.getSpecificEngineBaseFolder
					(IEngine.CATALOG_TYPE.FUNCTION, this.engineId, this.engineName)
				);
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
	public JSONObject getFunctionDefintionJson() {
		JSONObject json = new JSONObject();
		json.put("name", this.functionName);
		json.put("description", this.functionDescription);
		
		JSONObject parameterJSON = new JSONObject();
		if(this.parameters != null && !this.parameters.isEmpty()) {
			parameterJSON.put("type", "object");
			JSONObject propertiesJSON = new JSONObject();
			for(FunctionParameter fParam : this.parameters) {
				JSONObject thisPropJSON = new JSONObject();
				thisPropJSON.put("type", fParam.getParameterType());
				thisPropJSON.put("description", fParam.getParameterDescription());
				propertiesJSON.put(fParam.getParameterName(), thisPropJSON);
			}
			parameterJSON.put("properties", propertiesJSON);
		}
		json.put("parameters", parameterJSON);
		
		JSONArray requiredJSON = new JSONArray();
		if(this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			requiredJSON.put(this.requiredParameters);
		}
		json.put("required", requiredJSON);
		
		return json;
	}

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
	public String getFunctionName() {
		return functionName;
	}

	@Override
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	@Override
	public String getFunctionDescription() {
		return functionDescription;
	}

	@Override
	public void setFunctionDescription(String functionDescription) {
		this.functionDescription = functionDescription;
	}

	@Override
	public List<FunctionParameter> getParameters() {
		return parameters;
	}

	@Override
	public void setParameters(List<FunctionParameter> parameters) {
		this.parameters = parameters;
	}

	@Override
	public List<String> getRequiredParameters() {
		return this.requiredParameters;
	}
	
	@Override
	public void setRequiredParameters(List<String> requiredParameters) {
		this.requiredParameters = requiredParameters;
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
		return IEngine.CATALOG_TYPE.FUNCTION;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "REST";
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}
