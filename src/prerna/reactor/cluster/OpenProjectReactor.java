/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.cluster;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class OpenProjectReactor extends AbstractReactor {

  public OpenProjectReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);

    if (projectId == null || projectId.isEmpty()) {
      throw new IllegalArgumentException("Must input an project id");
    }

    if (projectId.equals("NEWSEMOSSAPP")) {
      Map<String, Object> returnMap = new HashMap<String, Object>();
      returnMap.put("project_name", "NEWSEMOSSAPP");
      returnMap.put("project_id", projectId);
      returnMap.put("project_type", IDatabaseEngine.DATABASE_TYPE.APP.toString());
      returnMap.put("project_cost", "");
      return new NounMetadata(
          returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
    }

    // make sure valid id for user
    if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
      // you dont have access
      throw new IllegalArgumentException(
          "Project does not exist or user does not have access to the project");
    }

    IProject project = Utility.getProject(projectId);
    if (project == null) {
      throw new IllegalArgumentException("Could not find or load project = " + projectId);
    }

    Map<String, Object> returnMap = new HashMap<String, Object>();
    returnMap.put("project_name", project.getProjectName());
    returnMap.put("project_id", project.getProjectId());
    returnMap.put("project_type", "");
    returnMap.put("project_cost", "");
    return new NounMetadata(
        returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
  }
}
