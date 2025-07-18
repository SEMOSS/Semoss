package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;

public class RunMCPToolReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RunMCPToolReactor.class);

	public RunMCPToolReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FUNCTION.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access.");
		}

		String functionName = this.keyValue.get(this.keysToGet[1]);
		if(functionName == null || (functionName=functionName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Function name must be passed in to execute the mcp tool");
		}

		String output = "{}";
		// get the param map
		// load the script and then run it
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		projectAssetFolder = projectAssetFolder.replace("\\", "/");

		String pyFolderLoc = projectAssetFolder + "/py";

		Map<String, Object> paramMap = getMap();

		String sysImport = "import sys";
		String getpath = "sys.path";
		String setpath = "sys.path.insert(0,'" + pyFolderLoc + "')";
		String loadLib = "import smss_driver as smss";

		// this is where we need to compose the method
		// for every argument I need to know the type
		// and then compose accordingly
		String jsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";

		JSONObject functionProperties = getFunction(functionName, jsonFileLoc);

		// iterate function properties and find if it is string etc. 
		Iterator <String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while(props.hasNext())
		{
			if(paramString.length() != 0) {
				paramString.append(", ");
			}
			String propName = props.next();
			JSONObject thisProp = ((JSONObject)functionProperties.get(propName));
			String propType = thisProp.getString("type");
			Object propValue = null;

			// get the value
			if(paramMap != null && paramMap.containsKey(propName)) {
				propValue = paramMap.get(propName);
			} else if (thisProp.has("default")) {
				// get the default value
				propValue = thisProp.getString("default");
			} else {
				propValue = "None";
			}
			paramString.append(propName).append("=");

			// compose the string
			// if it is none send it as is
			if(propType.toUpperCase().contains("STR") && !propValue.toString().equals("None")) {
				paramString.append("'").append(propValue).append("'");
			} else {
				paramString.append(propValue);
			}
		}
		
		String runMethod = "smss." + functionName + "(" + paramString + ")";
		classLogger.info("Running method..  " + runMethod + "  On project " + projectId);
		String curPath = insight.getPyTranslator().runScript(sysImport, getpath)+"";
		curPath = curPath.replace("\\", "/");
		if(!curPath.contains(pyFolderLoc)) {
			insight.getPyTranslator().runScript(setpath, loadLib);
		}
		// run method
		output = insight.getPyTranslator().runScript(runMethod)+"";
		classLogger.info(output);

		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	/**
	 * 
	 * @param functionName
	 * @param jsonFileLoc
	 * @return
	 */
	private JSONObject getFunction(String functionName, String jsonFileLoc)
	{
		File jsonFile = new File(jsonFileLoc);
		if(jsonFile.exists())
		{
			try (InputStream is = new FileInputStream(jsonFile)){
				String jsonTxt = IOUtils.toString(is, "UTF-8");
				JSONObject json = new JSONObject(jsonTxt);
				// the tools is what has it
				JSONArray toolObj = null;
				if(json.has("tools"))
				{
					toolObj = (JSONArray)json.getJSONArray("tools");
					for (int toolIndex = 0;toolIndex < toolObj.length();toolIndex++)
					{
						JSONObject thisTool = toolObj.getJSONObject(toolIndex);
						String toolName = thisTool.getString("name");
						if(toolName.contains(functionName))
						{
							// get everything else
							JSONObject properties = ((JSONObject)thisTool.get("inputSchema")).getJSONObject("properties");
							return properties;
						}
					}
				}
			} catch (FileNotFoundException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (JSONException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new JSONObject();
	}

	@Override
	public String getReactorDescription() {
		return "Execute a tool defined in the app";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
			return "The name of the function (tool) to execute";
		} else if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "A key-value pair map containing the parameter inputs for the function (tool)";
		}
		return super.getDescriptionForKey(key);
	}
	
}
