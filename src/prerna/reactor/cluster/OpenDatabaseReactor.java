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
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class OpenDatabaseReactor extends AbstractReactor {

  public OpenDatabaseReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);

    if (databaseId == null || databaseId.isEmpty()) {
      throw new IllegalArgumentException("Must input an database id");
    }

    if (databaseId.equals("NEWSEMOSSAPP")) {
      Map<String, Object> returnMap = new HashMap<String, Object>();
      returnMap.put("database_name", "NEWSEMOSSAPP");
      returnMap.put("database_id", databaseId);
      returnMap.put("database_type", IDatabaseEngine.DATABASE_TYPE.APP.toString());
      returnMap.put("database_subtype", "");
      returnMap.put("database_cost", "");
      return new NounMetadata(
          returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
    }

    // make sure valid id for user
    databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
    if (!(SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)
        || SecurityEngineUtils.engineIsDiscoverable(databaseId))) {
      // you dont have access
      throw new IllegalArgumentException(
          "Database does not exist or user does not have access to the database");
    }

    IDatabaseEngine engine = Utility.getDatabase(databaseId);
    if (engine == null) {
      throw new IllegalArgumentException("Could not find or load database = " + databaseId);
    }

    Map<String, Object> returnMap = new HashMap<String, Object>();
    returnMap.put("database_name", engine.getEngineName());
    returnMap.put("database_id", engine.getEngineId());
    Object[] typeAndCost = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(engine.getSmssProp());
    returnMap.put("database_type", typeAndCost[0].toString());
    returnMap.put("database_subtype", typeAndCost[1]);
    returnMap.put("database_cost", typeAndCost[2]);

    return new NounMetadata(
        returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
  }
}
