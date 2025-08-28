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
package prerna.util.git.gitlab;

import java.util.List;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GitlabListBranchesForProjectReactor extends AbstractReactor {

  public GitlabListBranchesForProjectReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.HOST.getKey(),
          ReactorKeysEnum.GITLAB_PROJECT_ID.getKey(),
          ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey(),
          ReactorKeysEnum.USE_APPLICATION_CERT.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String host = this.keyValue.get(ReactorKeysEnum.HOST.getKey());
    String gitProjectId = this.keyValue.get(ReactorKeysEnum.GITLAB_PROJECT_ID.getKey());
    String gitPrivateToken = this.keyValue.get(ReactorKeysEnum.GITLAB_PRIVATE_TOKEN.getKey());
    Boolean useApplicationCert =
        Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.USE_APPLICATION_CERT.getKey()) + "");
    List<Map<String, Object>> responseData =
        GitlabUtility.getGitlabBranches(
            host, gitProjectId, null, gitPrivateToken, useApplicationCert);
    return new NounMetadata(responseData, PixelDataType.VECTOR);
  }

  @Override
  public String getReactorDescription() {
    return "This reactor returns a list of JSON maps for the branches that exist for a GitLab project";
  }
}
