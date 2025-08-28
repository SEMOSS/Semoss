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
package prerna.reactor.insights.save;

import java.util.Map;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteInsightCacheReactor extends AbstractInsightReactor {

  public DeleteInsightCacheReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PROJECT.getKey(),
          ReactorKeysEnum.ID.getKey(),
          ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);
    String rdbmsId = this.keyValue.get(this.keysToGet[1]);
    Map<String, Object> parameterValues = getInsightParamValueMap();

    projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
    if (!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, rdbmsId)) {
      throw new IllegalArgumentException(
          "Project does not exist or user does not have permission to edit this insight");
    }

    String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
    try {
      InsightCacheUtility.deleteCache(projectId, projectName, rdbmsId, parameterValues, true);
      return new NounMetadata(true, PixelDataType.BOOLEAN);
    } catch (Exception e) {
      return new NounMetadata(false, PixelDataType.BOOLEAN);
    }
  }
}
