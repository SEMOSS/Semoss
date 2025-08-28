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
package prerna.reactor.project;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetProjectAvailableReactorsReactor extends AbstractReactor {

  public GetProjectAvailableReactorsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);

    if (projectId == null || projectId.isEmpty()) {
      throw new IllegalArgumentException("Must input an project id");
    }

    // make sure valid id for user
    projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
    if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
      // you dont have access
      throw new IllegalArgumentException(
          "Project does not exist or user does not have access to the project");
    }

    IProject project = Utility.getProject(projectId);
    return new NounMetadata(project.getAvailableReactors(), PixelDataType.CONST_STRING);
  }
}
