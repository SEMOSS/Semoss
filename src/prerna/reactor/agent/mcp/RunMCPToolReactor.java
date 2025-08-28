/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossMCPException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunMCPToolReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(RunMCPToolReactor.class);

  public RunMCPToolReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PROJECT.getKey(),
          ReactorKeysEnum.FUNCTION.getKey(),
          ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
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
      throw new IllegalArgumentException(
          "Project " + projectId + " does not exist or user does not have access.");
    }
    IProject project = Utility.getProject(projectId);

    String functionName = this.keyValue.get(this.keysToGet[1]);
    if (functionName == null || (functionName = functionName.trim()).isEmpty()) {
      throw new IllegalArgumentException("Function name must be passed in to execute the mcp tool");
    }
    functionName = MCPUtility.removeProjectIdFromToolsMethodName(projectId, functionName);

    // these are the params
    Map<String, Object> paramMap = getMap();

    String output = "{}";

    // first need to find the right tool

    String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
    projectAssetFolder = projectAssetFolder.replace("\\", "/");

    String pythonJsonFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
    String pixelJsonFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";

    JSONObject functionProperties = getFunction(functionName, pythonJsonFileLoc);
    if (functionProperties != null) {
      // this is a python mcp tool
      output =
          MCPUtility.runPythonTool(
              project, this.insight, functionName, functionProperties, paramMap);
      return new NounMetadata(
          output, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
    }

    functionProperties = getFunction(functionName, pixelJsonFileLoc);
    if (functionProperties != null) {
      // this is a pixel mcp tool
      output =
          MCPUtility.runPixelTool(
              project, this.insight, functionName, functionProperties, paramMap);
      return new NounMetadata(
          output, PixelDataType.CONST_STRING, PixelOperationType.MCP_TOOL_EXECUTION);
    }

    throw new SemossMCPException("Unknown tool: invalid_tool_name", MCPErrorCode.INVALID_PARAMS);
  }

  /**
   * @return
   */
  private Map<String, Object> getMap() {
    GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        return (Map<String, Object>) mapInputs.get(0).getValue();
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }
    return null;
  }

  /**
   * @param functionName
   * @param jsonFileLoc
   * @return
   */
  private JSONObject getFunction(String functionName, String jsonFileLoc) {
    File jsonFile = new File(jsonFileLoc);
    if (jsonFile.exists()) {
      try {
        String jsonTxt = FileUtils.readFileToString(jsonFile, "UTF-8");
        JSONObject json = new JSONObject(jsonTxt);
        // the tools is what has it
        JSONArray toolObj = null;
        if (json.has("tools")) {
          toolObj = (JSONArray) json.getJSONArray("tools");
          for (int toolIndex = 0; toolIndex < toolObj.length(); toolIndex++) {
            JSONObject thisTool = toolObj.getJSONObject(toolIndex);
            String toolName = thisTool.getString("name");
            if (toolName.contains(functionName)) {
              // get everything else
              JSONObject properties =
                  ((JSONObject) thisTool.get("inputSchema")).getJSONObject("properties");
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
    return null;
  }

  @Override
  public String getReactorDescription() {
    return "Execute a tool defined in the app";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
      return "The unique id for the project/app";
    } else if (key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
      return "The name of the function (tool) to execute";
    } else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
      return "A key-value pair map containing the parameter inputs for the function (tool)";
    }
    return super.getDescriptionForKey(key);
  }
}
