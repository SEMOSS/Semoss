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

import java.util.ArrayList;
import java.util.List;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ProjectPyReactor extends AbstractReactor {

  public ProjectPyReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.PROJECT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
    if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
      projectId = this.insight.getContextProjectId();
      if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
        projectId = this.insight.getProjectId();
      }
    }
    if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
      throw new IllegalArgumentException("Must input an project id");
    }

    String code = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CODE.getKey()));

    // make sure valid id for user
    projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
    if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
      // you don't have access
      throw new IllegalArgumentException(
          "Project does not exist or user does not have access to the project");
    }

    IProject project = Utility.getProject(projectId);
    PyTranslator projectPyTranslator = project.getProjectPyTranslator();
    Object output = projectPyTranslator.runScript(code);

    List<NounMetadata> outputs = new ArrayList<>(1);
    outputs.add(new NounMetadata(output + "", PixelDataType.CONST_STRING));
    return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
  }
}
