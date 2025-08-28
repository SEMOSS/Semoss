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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InitMCPReactor extends AbstractReactor {

  // responsible for making the mcp
  // looks for project id and then makes the MCP based on it

  // expected payload
  //	//{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2024-11-05",
  // "capabilities":{"experimental":{},"prompts":{"listChanged":false},
  // "resources":{"subscribe":false,"listChanged":false},
  // "tools":{"listChanged":false}},
  // "serverInfo":{"name":"Stock Price Server","version":"1.8.0"}}}
  private static final Logger classLogger = LogManager.getLogger(InitMCPReactor.class);

  private final String PROTOCOL_VERSION = "protocolVersion";

  public InitMCPReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), PROTOCOL_VERSION};
    this.keyRequired = new int[] {1, 1};
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
    if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
      throw new IllegalArgumentException(
          "Project " + projectId + " does not exist or user does not have access to edit.");
    }

    // get the project
    // check to see if there is a py directory
    // if there is pick the main.py and ask the system to make the json
    IProject project = Utility.getProject(keyValue.get(keysToGet[0]));
    String projectName = project.getProjectName();

    // need to return the protocol version of the client request
    // as part of initialization
    String protocolVersion = this.keyValue.get(PROTOCOL_VERSION);

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
    return new NounMetadata(resultJson, PixelDataType.JSON_OBJECT);
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(PROTOCOL_VERSION)) {
      return "The protocol version that was specified in the Initialization phase from the client";
    }
    return super.getDescriptionForKey(key);
  }
}
