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
package prerna.auth.utils.reactors.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminProjectInfoReactor extends AbstractReactor {

  public AdminProjectInfoReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
    if (adminUtils == null) {
      throw new IllegalArgumentException("User must be an admin to perform this function");
    }
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);

    if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
      throw new IllegalArgumentException("Must input an project id");
    }

    List<Map<String, Object>> baseInfo =
        adminUtils.getAllProjectSettings(Arrays.asList(projectId), null, null, null, null);
    if (baseInfo == null || baseInfo.isEmpty()) {
      throw new IllegalArgumentException("Could not find any project data");
    }

    // we filtered to a single project
    Map<String, Object> projectInfo = baseInfo.get(0);
    projectInfo.putAll(
        SecurityProjectUtils.getAggregateProjectMetadata(projectId, getMetaKeys(), true));
    return new NounMetadata(
        projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
  }

  private List<String> getMetaKeys() {
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
    if (grs != null && !grs.isEmpty()) {
      return grs.getAllStrValues();
    }

    return null;
  }
}
