package prerna.reactor.agent.mcp;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.PixelRunner;
import prerna.util.AssetUtility;

public final class MCPToolExecution {

	private static final Logger classLogger = LogManager.getLogger(MCPToolExecution.class);
	
	/**
	 * Run a python mcp tool
	 * 
	 * @param project
	 * @param insight
	 * @param functionName
	 * @param functionProperties
	 * @param paramMap
	 * @return
	 */
	public static String runPythonTool(IProject project, Insight insight, 
			String functionName, JSONObject functionProperties, Map<String, Object> paramMap) {
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectId());

		// load the path to have access to the file
		String pyFolderLoc = projectAssetFolder + "/py";
		String sysImport = "import sys";
		String getpath = "sys.path";
		String setpath = "sys.path.insert(0,'" + pyFolderLoc + "')";
		String loadLib = "import smss_driver as smss";

		// iterate function properties and find if it is string etc. 
		Iterator <String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while(props.hasNext()) {
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
		classLogger.info("Running python tool '" + runMethod + "' from project " + project.getProjectId());
		String curPath = insight.getPyTranslator().runScript(sysImport, getpath)+"";
		curPath = curPath.replace("\\", "/");
		if(!curPath.contains(pyFolderLoc)) {
			insight.getPyTranslator().runScript(setpath, loadLib);
		}
		// run method
		return insight.getPyTranslator().runScript(runMethod)+"";
	}
	
	/**
	 * Run a pixel mcp tool
	 * 
	 * @param project
	 * @param insight
	 * @param functionName
	 * @param functionProperties
	 * @param paramMap
	 * @return
	 */
	public static String runPixelTool(IProject project, Insight insight, 
			String functionName, JSONObject functionProperties, Map<String, Object> paramMap) {
		// iterate function properties and find if it is string etc. 
		Iterator <String> props = functionProperties.keys();
		StringBuilder paramString = new StringBuilder();
		while(props.hasNext()) {
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
			} 
			// if we have a value, add it 
			if(propValue != null) {
				paramString.append(propName).append("=");
	
				// compose the string
				// if it is none send it as is
				if(propType.toUpperCase().contains("STR") && !propValue.toString().equals("None")) {
					paramString.append("'").append(propValue).append("'");
				} else {
					paramString.append(propValue);
				}
			}
		}
		
		String runMethod = functionName+"("+paramString+");";
		classLogger.info("Running pixel tool '" + runMethod + "' from project " + project.getProjectId());
		// run pixel
		PixelRunner pixelReturn = insight.runPixel(runMethod);
		return pixelReturn.getResults().get(0).getValue()+"";
	}
	
	private MCPToolExecution() {
		
	}
}
