package prerna.engine.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public abstract class AbstractEngine implements IEngine, IMCP {

	private static final Logger classLogger = LogManager.getLogger(AbstractEngine.class);

	protected static final String FILE_SEPARATOR = "/";

	protected String smssFilePath = null;
	protected CaseInsensitiveProperties origSmssProp = null;
	protected CaseInsensitiveProperties smssProp = null;

	protected String engineId = null;
	protected String engineName = null;

	protected String engineBaseFolder = null;
	protected String engineAppRootFolder = null;
	protected String engineVersionFolder = null;
	protected String engineAssetsFolder = null;

	// to define custom log4j2.xml at an engine level
	// to isolate tenant logs
	protected LoggerContext engineSpecificLoggerCtx;

	/**
	 * This is if we have an engine with no assets Or for database, connection but
	 * no OWL
	 */
	protected boolean isBasic = false;

	/**
	 * Init the general smss values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	/**
	 * Init the general smss values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		// this is because of some silly stuff on databases
		if (this.smssProp.isEmpty()) {
			return;
		}
		// is basic, no real folder structure
		if (this.isBasic) {
			if (smssProp.containsKey(Constants.ENGINE)) {
				this.engineId = smssProp.getProperty(Constants.ENGINE);
			}
			if (smssProp.containsKey(Constants.ENGINE_ALIAS)) {
				this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);
			}
			return;
		}

		// not basic, so normal flow
		this.engineId = smssProp.getProperty(Constants.ENGINE);
		this.engineName = smssProp.getProperty(Constants.ENGINE_ALIAS);

		String engineIdAndName = SmssUtilities.getUniqueName(engineName, engineId);

		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if (secretStore != null) {
			Map<String, Object> engineSecrets = secretStore.getEngineSecrets(getCatalogType(), this.engineId,
					this.engineName);
			if (engineSecrets == null || engineSecrets.isEmpty()) {
				classLogger.info("No secrets found for " + engineIdAndName);
			} else {
				classLogger.info("Successfully pulled secrets for " + engineIdAndName);
				this.smssProp.putAll(engineSecrets);
			}
		}

		IEngine.CATALOG_TYPE eType = getCatalogType();
		this.engineBaseFolder = EngineUtility.getSpecificEngineBaseFolder(eType, engineIdAndName);
		this.engineAppRootFolder = EngineUtility.getSpecificEngineAppRootFolder(eType, engineIdAndName);
		this.engineVersionFolder = EngineUtility.getSpecificEngineVersionFolder(eType, engineIdAndName);
		this.engineAssetsFolder = EngineUtility.getSpecificEngineAssetsFolder(eType, engineIdAndName);

		// make sure we always have an assets folder and all the directories leading up
		// to it
		{
			File f = new File(this.engineAssetsFolder);
			if (!f.exists() || !f.isDirectory()) {
				f.mkdirs();
				// this means you have a legacy structure
				// i will move everything you have into the assets folder
				// with exception of .mv.db files
				Path assetsPath = Path.of(this.engineAssetsFolder);
				Files.list(Path.of(this.engineBaseFolder)).forEach(item -> {
					// skip if the item is already within app_root or app_root/versions
					// this would really only be for the engine image
					String fileName = item.getFileName().toString();
					if (item.toString().replace("\\", "/").contains("/" + Constants.APP_ROOT_FOLDER + "/")
							|| fileName.equals(Constants.APP_ROOT_FOLDER)) {
						return; // skip
					}

					if (!fileName.endsWith(".mv.db") && !fileName.endsWith(".jnl") && !fileName.endsWith(".sqlite")) {
						try {
							Path targetPath = assetsPath.resolve(item.getFileName());
							classLogger.info("Performing asset restructure for " + item + " > " + targetPath);
							Files.move(item, targetPath, StandardCopyOption.REPLACE_EXISTING);
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					} else {
						classLogger.info("Ignoring asset restructure for " + item);
					}
				});
			}
			if (!AssetUtility.isGit(this.engineVersionFolder)) {
				GitRepoUtils.init(this.engineVersionFolder);
			}
		}
	}

	@Override
	public void delete() {
		IEngine.CATALOG_TYPE eType = getCatalogType();
		classLogger.debug("Delete " + eType + " engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(this.engineBaseFolder);
		if (engineFolder.exists()) {
			classLogger.info("Delete " + eType + " engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info(eType + " engine folder " + engineFolder + " does not exist");
		}

		classLogger.info("Deleting " + eType + " engine smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.engineId);
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
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}

	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		if (smssProp instanceof CaseInsensitiveProperties) {
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
	public CaseInsensitiveProperties getOrigSmssProp() {
		return this.origSmssProp;
	}

	@Override
	public boolean isBasic() {
		return this.isBasic;
	}

	@Override
	public void setBasic(boolean isBasic) {
		this.isBasic = isBasic;
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
	public Logger getEngineLogger(String loggerName) {
		if (this.engineSpecificLoggerCtx != null) {
			return this.engineSpecificLoggerCtx.getLogger(loggerName);
		}

		File log4j2 = new File(this.engineAssetsFolder + "/log4j2.xml");
		if (!log4j2.exists() || !log4j2.isFile()) {
			return null;
		}

		if (this.engineSpecificLoggerCtx == null) {
			ClassLoader isolatedLoader = new URLClassLoader(new URL[0], null);
			synchronized (this) {
				if (this.engineSpecificLoggerCtx == null) {
					this.engineSpecificLoggerCtx = Configurator.initialize(this.engineId, isolatedLoader,
							"file:" + log4j2.getAbsolutePath());
				}
			}
		}
		return this.engineSpecificLoggerCtx.getLogger(loggerName);
	}

	
	//-------------------- MCP Specific Methods ----------------------------
	
	public JSONObject getMCPResources()
	{
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(this.getEngineId());
		// need to apply the same from java etc. 
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "resources");
		jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "resources");
		pyToolArray.putAll(javaToolArray);
		
		JSONObject toolMap = new JSONObject();
		toolMap.put("resources", pyToolArray);

		return toolMap;
	}
	
	public JSONObject getMCPTools()
	{
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(this.getEngineId());
		String pythonJsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		String pixelJsonFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";

		JSONObject toolMap = new JSONObject();
		JSONArray toolsArray = new JSONArray();
		toolsArray.putAll(MCPUtility.getNode(pythonJsonFileLoc, "tools"));
		toolsArray.putAll(MCPUtility.getNode(pixelJsonFileLoc, "tools"));
		toolMap.put("tools", toolsArray);

		// add in meta as well
		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_PROJECT_ID, this.getEngineId());
		_meta.put(MCPUtility.SMSS_PROJECT_NAME, getEngineName());
		toolMap.put("_meta", _meta);

		return toolMap;
	}
	
	public JSONObject initMCP(String protocolVersion)
	{
		String projectName = getEngineName();
		
		// need to return the protocol version of the client request
		// as part of initialization 
		JSONObject resultJson = new JSONObject();
		resultJson.put("protocolVersion", protocolVersion);
		
		JSONObject serverJson = new JSONObject();
		serverJson.put("name", projectName);
		serverJson.put("version", "1.8.0");
		resultJson.put("serverInfo", serverJson);
		
		JSONObject capabilitiesJson = new JSONObject();
		capabilitiesJson.put("experimental", new JSONObject());
		
		JSONObject promptJson = new JSONObject();
		promptJson.put("listChanged", false);
		promptJson.put("subscribe", true);
		capabilitiesJson.put("prompts", promptJson);
		
		JSONObject resourcesJson = new JSONObject();
		resourcesJson.put("listChanged", false);
		resourcesJson.put("subscribe", true);
		capabilitiesJson.put("resources", resourcesJson);
		
		JSONObject toolsJson = new JSONObject();
		toolsJson.put("listChanged", false);
		toolsJson.put("subscribe", true);
		capabilitiesJson.put("tools", toolsJson);

		resultJson.put("capabilities", capabilitiesJson);
		
		return resultJson;
	}
	
	public JSONObject getMCPPrompts()
	{
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(this.getEngineId());
		// need to apply the same from java etc. 
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		JSONArray pyToolArray = MCPUtility.getNode(jsonFileLoc, "prompts");
		jsonFileLoc = projectAssetFolder + "/mcp/java_mcp.json";
		JSONArray javaToolArray = MCPUtility.getNode(jsonFileLoc, "prompts");
		pyToolArray.putAll(javaToolArray);
		
		JSONObject toolMap = new JSONObject();
		toolMap.put("prompts", pyToolArray);
		return toolMap;
	}
		
	
	
}
