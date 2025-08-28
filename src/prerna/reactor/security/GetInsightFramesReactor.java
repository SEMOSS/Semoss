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

import java.util.List;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightFramesReactor extends AbstractReactor {

  public GetInsightFramesReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PROJECT.getKey(),
          ReactorKeysEnum.ID.getKey(),
          ReactorKeysEnum.FRAME.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);
    String rdbmsId = this.keyValue.get(this.keysToGet[1]);
    String frameNamePattern = this.keyValue.get(this.keysToGet[2]);

    projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
    if (!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
      NounMetadata noun =
          new NounMetadata(
              "User does not have access to this insight",
              PixelDataType.CONST_STRING,
              PixelOperationType.ERROR);
      SemossPixelException err = new SemossPixelException(noun);
      err.setContinueThreadOfExecution(false);
      throw err;
    }

    List<Object[]> retList =
        SecurityInsightUtils.getInsightFrames(projectId, rdbmsId, frameNamePattern);
    NounMetadata retNoun = new NounMetadata(retList, PixelDataType.CUSTOM_DATA_STRUCTURE);
    return retNoun;
  }
}
