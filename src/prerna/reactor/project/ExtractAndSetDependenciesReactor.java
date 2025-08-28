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
package prerna.reactor.project;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ExtractAndSetDependenciesReactor extends AbstractReactor {

  public ExtractAndSetDependenciesReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();

    String projectId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
      throw new IllegalArgumentException(
          "The user does not have access to edit this project or project id is invalid");
    }

    IProject project = Utility.getProject(projectId);

    String fileRelativePath = this.keyValue.get(keysToGet[1]);
    if (fileRelativePath != null && !(fileRelativePath = fileRelativePath.trim()).isEmpty()) {
      fileRelativePath = fileRelativePath.replace("\\", "/");
      if (!fileRelativePath.startsWith("/")) {
        fileRelativePath = "/" + fileRelativePath;
      }
    }

    // getting the asset folder path where UUIDs are present
    String assetsFileLocation =
        EngineUtility.getSpecificEngineAssetsFolder(
            IEngine.CATALOG_TYPE.PROJECT, project.getProjectId(), project.getProjectName());
    String projectFolderPath = assetsFileLocation;
    if (fileRelativePath != null && !(fileRelativePath = fileRelativePath.trim()).isEmpty()) {
      fileRelativePath = fileRelativePath.replace("\\", "/");
      if (!fileRelativePath.startsWith("/")) {
        fileRelativePath = "/" + fileRelativePath;
      }
      projectFolderPath += fileRelativePath;
    }

    File projectF = new File(projectFolderPath);
    Map<String, Object> engineIdMap =
        ProjectHelper.extractEngineIdsFromProjectFolder(projectId, projectF);
    // update the project dependencies table only with valid engineIds
    if (engineIdMap.containsKey("success")) {
      Map<String, Object> successMap = (Map<String, Object>) engineIdMap.get("success");
      SecurityProjectUtils.updateProjectDependencies(user, projectId, successMap.keySet());
    }

    // sending the success and failed list of engineIds to FE
    Map<String, Object> retMap = new HashMap<>();
    retMap.put("engineIds", engineIdMap);
    return new NounMetadata(
        retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
  }

  @Override
  public String getReactorDescription() {
    return "Extract engine ids from the project's folder and adds the engine ids as project dependencies";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "This is an optional relative file path within the project assets folder to search";
    }
    return super.getDescriptionForKey(key);
  }
}
