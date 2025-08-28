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
package prerna.reactor.security;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class GetProjectDependenciesReactor extends AbstractSetMetadataReactor {

  public GetProjectDependenciesReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    String userId = this.insight.getUserId();
    String projectId = UploadInputUtility.getProjectNameOrId(this.store);
    if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
      throw new IllegalArgumentException(
          "The user does not have access to view this project or project id is invalid");
    }

    return new NounMetadata(
        SecurityProjectUtils.getProjectDependencyDetails(projectId, userId), PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Set the engine dependencies for a project";
  }
}
