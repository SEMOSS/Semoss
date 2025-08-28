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
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class DatabaseUsersReactor extends AbstractReactor {

  public DatabaseUsersReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);

    if (databaseId == null || (databaseId = databaseId.trim()).isEmpty()) {
      throw new IllegalArgumentException("Must input an database id");
    }

    // make sure valid id for user
    databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
      // you dont have access
      throw new IllegalArgumentException(
          "Database does not exist or user does not have access to database");
    }

    List<Map<String, Object>> baseInfo =
        SecurityEngineUtils.getDisplayDatabaseOwnersAndEditors(databaseId);
    return new NounMetadata(
        baseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_USERS);
  }
}
