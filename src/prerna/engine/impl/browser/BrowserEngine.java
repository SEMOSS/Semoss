package prerna.engine.impl.browser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IBrowserEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class BrowserEngine implements IBrowserEngine {
	
	private static final Logger classLogger = LogManager.getLogger(BrowserEngine.class);
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	protected String smssFilePath = null;
	protected CaseInsensitiveProperties origSmssProp = null;
	protected CaseInsensitiveProperties smssProp = null;
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties generalEngineProp = null;
	protected Properties ontoProp = null;

	protected String browserFile = null;
	


	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		if(smssProp.isEmpty()) {
			return;
		}
		// grab the main properties
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		
		this.browserFile = this.smssProp.getProperty(Constants.BROWSER_FILE);
	}
	
	@Override
	public void close() throws IOException {
		// do nothing
	}
	
	public String getBrowserFile() {
		return this.browserFile;
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
		if(smssProp instanceof CaseInsensitiveProperties) {
			this.origSmssProp = (CaseInsensitiveProperties) smssProp;
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		} else {
			this.origSmssProp = new CaseInsensitiveProperties(smssProp);
			this.smssProp = new CaseInsensitiveProperties(smssProp);
		}
	}

	@Override
	public CaseInsensitiveProperties getSmssProp() {
		return this.smssProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		return this.origSmssProp;
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		// TODO Auto-generated method stub
		return IEngine.CATALOG_TYPE.BROWSER;
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

		File engineFolder = new File(EngineUtility.getSpecificEngineBaseFolder(
				getCatalogType(), this.engineId, this.engineName)
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
	public boolean holdsFileLocks() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
		// Fetch metadata for the engine
				Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(
						this.getEngineId(),
						Arrays.asList("description"),
						true
						);

				// Extract the description from metadata
				String description = (String) metadata.get("description");
				if (description == null) {
					description = "No description available.";
				}

				// Create the main map
				Map<String, Object> toolMap = new HashMap<>();
				toolMap.put("type", "browser");

//				// Create the function map
//				Map<String, Object> functionMap = new HashMap<>();
//				functionMap.put("name", "function_engine");
//				functionMap.put("description", description);
//
//				// Create the parameters map
//				Map<String, Object> parametersMap = new HashMap<>();
//				parametersMap.put("type", "object");
//
//				// Create the properties map
//				Map<String, Object> propertiesMap = new HashMap<>();
//
//				// Add the id property
//				Map<String, Object> idMap = new HashMap<>();
//				idMap.put("type", "string");
//				idMap.put("description", "The unique identifier for this function_engine used to call this specific engine");
//				idMap.put("enum", Arrays.asList(this.getEngineId()));
//				propertiesMap.put("id", idMap);
//
//				// Add the map property
//				Map<String, Object> mapMap = new HashMap<>();
//				mapMap.put("type", "object");
//
//				// Create the map properties map
//				Map<String, Object> mapPropertiesMap = new HashMap<>();
//				for (FunctionParameter param : this.getParameters()) {
//					Map<String, Object> paramMap = new HashMap<>();
//					paramMap.put("type", param.getParameterType().toLowerCase());
//					paramMap.put("description", param.getParameterDescription());
//					mapPropertiesMap.put(param.getParameterName(), paramMap);
//				}
//				mapMap.put("properties", mapPropertiesMap);
//				mapMap.put("required", this.getRequiredParameters());
//				mapMap.put("description", "A map containing the parameters to pass into the function_engine call.");
//
//				propertiesMap.put("map", mapMap);
//
//				// Finalize parameters map
//				parametersMap.put("properties", propertiesMap);
//				parametersMap.put("required", Arrays.asList("id", "map"));
//
//				// Add parameters to function map
//				functionMap.put("parameters", parametersMap);
//
//				// Add function map to main map
//				toolMap.put("function", functionMap);

				return toolMap;
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
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.BROWSER.name();
	}

	@Override
	public JSONObject getBrowserFileInstructions() {
		// TODO Auto-generated method stub
		return null;
	}

}
