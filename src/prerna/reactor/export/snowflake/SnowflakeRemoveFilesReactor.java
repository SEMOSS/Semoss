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
package prerna.reactor.export.snowflake;

import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class SnowflakeRemoveFilesReactor extends AbstractReactor {

  public SnowflakeRemoveFilesReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.DATABASE.getKey(), "userStage", "tableStage", "namedStage"};
    this.keyRequired = new int[] {1, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
      throw new IllegalArgumentException(
          "Database " + databaseId + " does not exist or user does not have access to database");
    }

    String userStage = this.keyValue.get(this.keysToGet[1]);
    String tableStage = this.keyValue.get(this.keysToGet[2]);
    String namedStage = this.keyValue.get(this.keysToGet[3]);

    String sql = "remove ";
    if (userStage != null && !(userStage = userStage.trim()).isEmpty()) {
      sql += " @~" + userStage;
    } else if (tableStage != null && !(tableStage = tableStage.trim()).isEmpty()) {
      sql += " @%" + tableStage;
    } else if (namedStage != null && !(namedStage = namedStage.trim()).isEmpty()) {
      sql += " @" + namedStage;
    } else {
      throw new IllegalArgumentException(
          "Must pass in userStage, tableStage, or namedStage. All values were null or empty");
    }

    IDatabaseEngine snowflake = Utility.getDatabase(databaseId);
    if (!(snowflake instanceof IRDBMSEngine)) {
      throw new IllegalArgumentException("Database is not a snowlfake db");
    }
    IRDBMSEngine snowflakeRdbms = (IRDBMSEngine) snowflake;
    if (snowflakeRdbms.getDbType() != RdbmsTypeEnum.SNOWFLAKE) {
      throw new IllegalArgumentException("Database is not a snowlfake db");
    }
    HardSelectQueryStruct qs = new HardSelectQueryStruct();
    qs.setQuery(sql);
    List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(snowflake, qs);
    return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE);
  }

  @Override
  public String getReactorDescription() {
    return "Utility method to remove a file from '@~/<userStage>' or '@%<tableStage>' or '@<namedStage>' on snowflake depending on the parameters provided. "
        + "'@~' is for user stage, @% is for table stage, and @ is for a named stage. Snowflake docs found here: https://docs.snowflake.com/en/sql-reference/sql/remove";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
      return "The database id for the snowflake db";
    } else if (key.equals("userStage")) {
      return "The path prefix for the user stage. Do not include the '@~' qualifier";
    } else if (key.equals("tableStage")) {
      return "The table name and path prefix. Do not enter the '@%' qualifier";
    } else if (key.equals("namedStage")) {
      return "The named stage and path prefix. This will not create a stage if it does not exist. Do not enter the '@' qualifier ";
    }

    return super.getDescriptionForKey(key);
  }
}
