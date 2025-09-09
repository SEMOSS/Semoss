package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public abstract class AbstractReactorFunctionEngine extends AbstractReactor implements IReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractReactorFunctionEngine.class);

	protected String engineId;
	protected String engineName;
	
	protected String smssFilePath;
	protected Properties smssProp;
	
	protected String functionName;
	protected String functionDescription;
	protected List<FunctionParameter> parameters;
	protected List<String> requiredParameters;
	
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		NounStore ns = new NounStore("reactorExecution");
		for(String key : parameterValues.keySet()) {
			GenRowStruct nounGrs = ns.makeNoun(key);
			
			Object val = parameterValues.get(key);
			if(val instanceof Collection) {
				Collection<Object> valCollection = (Collection) val;
				for(Object valEle : valCollection) {
					nounGrs.add(NounMetadata.predictNounMetadata(valEle));
				} 
			} else {
				nounGrs.add(NounMetadata.predictNounMetadata(val));
			}
		}
		return execute(ns, null);
	}
	
	@Override
	public NounMetadata execute(NounStore ns) {
		return execute(ns, null);
	}
	
	/**
	 * Convenience method to allow order or named noun for basic string inputs
	 */
	public Map<String, String> organizeKeys(NounStore ns, GenRowStruct curRow) {
		Map<String, String> keyValue = new HashMap<>();
		if(ns.size() > 0) {
			for(int keyIndex = 0; keyIndex < this.keysToGet.length; keyIndex++) {
				String key = this.keysToGet[keyIndex];
				if(ns.getNoun(key) != null) {
					GenRowStruct grs = ns.getNoun(key);
					if(!grs.isEmpty()) {
						keyValue.put(this.keysToGet[keyIndex], grs.get(0)+"");	
					}
				}
			}
		}
		
		// fill in order based on whatever is left
		int counter = 0;
		if(curRow != null && !curRow.isEmpty()) {
			for(int keyIndex = 0; keyIndex < this.keysToGet.length; keyIndex++) {
				if(!keyValue.containsKey(this.keysToGet[keyIndex])) {
					keyValue.put(this.keysToGet[keyIndex], curRow.get(counter) + "");
					// increase counter index
					counter++;
				}
				
				if(counter >= curRow.size()) {
					break;
				}
			}
		}
		
		// check which of these are optional
		checkOptional(keyValue);
		return keyValue;
	}
	
	/**
	 * 
	 */
	protected void checkOptional(Map<String, String> keyValue) {
		StringBuilder nullMessage = new StringBuilder();
		for(int keyIndex = 0; this.keyRequired != null && keyIndex < this.keyRequired.length;keyIndex++) {
			int required = this.keyRequired[keyIndex];
			if(required == 1) {
				String thisKey = this.keysToGet[keyIndex];
				if(!keyValue.containsKey(thisKey)) {
					// this is where the default would come in
					nullMessage.append(thisKey).append("  ");
				}
			}
		}
		
		if(nullMessage.length() != 0) {
			nullMessage.append("Cannot be empty").insert(0, "Fields  ");
			throw new IllegalArgumentException(nullMessage.toString());
		}
	}
	
	/**
	 * Utility method to get the string inputs from a named GenRowStruct entry
	 * @param key
	 * @return
	 */
	public List<String> getNounAsStringList(NounStore ns, String key) {
		List<String> columns = new ArrayList<>();
		GenRowStruct colGrs = ns.getNoun(key);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		}

		return columns;
	}
	
	@Override
	public void organizeKeys() {
		// we cant have this because it is not thread safe
		throw new UnsupportedOperationException("Cannot invoke the organizeKeys() method on reactor functions. Please use organizeKeys(NounStore ns, GenRowStruct curRow)");
	}
	
	@Override
	public void checkOptional() {
		// we cant have this because it is not thread safe
		throw new UnsupportedOperationException("Cannot invoke the checkOptional() method on reactor functions. Please use checkOptional(Map<String, String> keyValue)");
	}
	
	@Override
	public List<String> getNounAsStringList(String key) {
		// we cant have this because it is not thread safe
		throw new UnsupportedOperationException("Cannot invoke the getNounAsStringList(String key) method on reactor functions. Please use getNounAsStringList(NounStore ns, String key)");
	}
	
	@Override
	public void setNounStore(NounStore store) {
		// we cant have this because it is not thread safe
		throw new UnsupportedOperationException("Cannot invoke the setNounStore method on reactor functions. Please use execte(NounStore ns)");
	}
	
	@Override
	public NounMetadata execute() {
		// we cant have this because it is not thread safe
		throw new UnsupportedOperationException("Cannot invoke the setNounStore method on reactor functions. Please use execte(NounStore ns)");
	}
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);

		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if(secretStore != null) {
			Map<String, Object> engineSecrets = secretStore.getEngineSecrets(getCatalogType(), this.engineId, this.engineName);
			if(engineSecrets != null && !engineSecrets.isEmpty()) {
				this.smssProp.putAll(engineSecrets);
			}
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
	public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
		throw new NotImplementedException("This method has not been implemented yet...");
	}
	
	@Override
	public Map<String, Object> buildBedrockToolSpec() {
		throw new NotImplementedException("This method has not been implemented yet...");
	}
	
	@Override
	public Map<String, Object> buildFunctionEngineToolMap() {
		// TODO Auto-generated method stub
		return null;
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
	public boolean holdsFileLocks() {
		return false;
	}
	
	@Override
	public void close() throws IOException {

	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "REACTOR";
	}

	@Override
	public boolean isBasic() {
		return false;
	}
	
	@Override
	public void setBasic(boolean isBasic) {
		// always false
	}

}
