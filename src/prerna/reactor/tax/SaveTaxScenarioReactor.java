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
package prerna.reactor.tax;

import java.util.Map;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SaveTaxScenarioReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {

    return null;
  }

  public static boolean addNewVersion(
      IDatabaseEngine engine,
      String clientID,
      double scenarioID,
      double latestVersion,
      double newVersion,
      Map<String, String> newValues) {
    clientID = AbstractSqlQueryUtil.escapeForSQLStatement(clientID);
    String sql =
        "Insert Into INPUTCSV (CLIENT_ID, SCENARIO, VERSION, FORMNAME, FIELDNAME, ALIAS_1, VALUE_1, TYPE_1, HASHCODE, WEIGHT, RETURNTYPE, COLUMN_1) "
            + "Select CLIENT_ID, SCENARIO, "
            + newVersion
            + ", FORMNAME, FIELDNAME, ALIAS_1, VALUE_1, TYPE_1, HASHCODE, WEIGHT, RETURNTYPE, COLUMN_1 From INPUTCSV "
            + "WHERE CLIENT_ID='"
            + clientID
            + "' AND SCENARIO="
            + scenarioID
            + " AND VERSION="
            + latestVersion;

    // execute inserts
    ((RDBMSNativeEngine) engine).execUpdateAndRetrieveStatement(sql, true);

    for (String alias : newValues.keySet()) {
      String aliasValue = AbstractSqlQueryUtil.escapeForSQLStatement(newValues.get(alias));
      alias = AbstractSqlQueryUtil.escapeForSQLStatement(alias);
      sql =
          "UPDATE INPUTCSV SET VALUE_1='"
              + aliasValue
              + "' "
              + "WHERE CLIENT_ID='"
              + clientID
              + "' AND SCENARIO="
              + scenarioID
              + " AND VERSION="
              + newVersion
              + " AND ALIAS_1='"
              + alias
              + "'";

      // execute individual update
      ((RDBMSNativeEngine) engine).execUpdateAndRetrieveStatement(sql, true);
    }

    return true;
  }
}
