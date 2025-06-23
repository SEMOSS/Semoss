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

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class RunMCPToolReactor extends AbstractReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	private static final Logger classLogger = LogManager.getLogger(RunMCPToolReactor.class);

	public RunMCPToolReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		String projectId = null;
		String functionName = null;
		if(this.store.getNoun(ReactorKeysEnum.PROJECT.getKey()) != null)
			projectId = this.store.getNoun(ReactorKeysEnum.PROJECT.getKey()).get(0).toString();

		if(this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey()) != null)
			functionName = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey()).get(0).toString();
		
		String output = "{}";
		// get the key
		// get the value from the param map
		// format it

		if(projectId != null || functionName != null)
		{
			// get the param map
			// load the script and then run it
			String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
			projectAssetFolder = projectAssetFolder.replace("\\", "/");

			String pyFolderLoc = projectAssetFolder + "/py";
			
			Map <String, Object> paramMap = getMap();
			
			String sysImport = "import sys";
			String getpath = "print(sys.path)";
			String setpath = "sys.path.insert(0,'" + pyFolderLoc + "')";
			String loadLib = "import main";
			
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
				if(paramString.length() != 0)
					paramString.append(", ");
				String propName = props.next();
				JSONObject thisProp = ((JSONObject)functionProperties.get(propName));
				String propType = thisProp.getString("type");
				Object propValue = null;
				
				// get the value
				if(paramMap.containsKey(propName))
					propValue = paramMap.get(propName);
				else if (thisProp.has("default"))
					// get the default value
					propValue = thisProp.getString("default");
				else
					propValue = "None";
				paramString.append(propName).append("=");
				
				// compose the string
				if(propType.toUpperCase().contains("STR") && !propValue.toString().equals("None")) // if it is none send it as is
					paramString.append("'").append(propValue).append("'");
				else
					paramString.append(propValue);
				
			}
			String runMethod = "main." + functionName + "(" + paramString + ")";
			classLogger.info("Running method..  " + runMethod + "  On project " + projectId);
			String curPath = insight.getPyTranslator().runPyAndReturnOutput(sysImport, getpath);
			curPath = curPath.replace("\\", "/");
			if(!curPath.contains(pyFolderLoc))
				insight.getPyTranslator().runPyAndReturnOutput(setpath, loadLib);
			// run set up
			// do set context
			
			// run method
			output = insight.getPyTranslator().runSingle(runMethod, insight);
			
			classLogger.info(output);
			
			
		}	
		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

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
	
	private JSONObject getFunction(String functionName, String jsonFileLoc)
	{
		File jsonFile = new File(jsonFileLoc);
		if(jsonFile.exists())
		{
			InputStream is = null;
			try {
				is = new FileInputStream(jsonFile);
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
				is.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return new JSONObject();

	}
	
}
