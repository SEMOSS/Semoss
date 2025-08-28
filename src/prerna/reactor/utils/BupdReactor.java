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
package prerna.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class BupdReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(BupdReactor.class);

  public BupdReactor() {
    this.keysToGet = new String[] {"fancy", "embed"};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);
    Connection conn = null;
    try {
      conn = engine.makeConnection();
    } catch (SQLException e) {
      classLogger.error(Constants.STACKTRACE, e);
      String engineName = engine.getEngineName() != null ? engine.getEngineName() : "engine";
      throw new IllegalArgumentException("Could not connect to " + engineName);
    }

    Statement stmt = null;
    try {
      // check to see if such a fancy name exists
      stmt = conn.createStatement();
      String query =
          "SELECT embed, fancy from bitly where fancy='" + this.keyValue.get("fancy") + "'";
      ResultSet rs = stmt.executeQuery(query);
      // if there is a has next not sure what

      if (rs.next()) {
        query =
            "Update bitly set embed = '"
                + this.keyValue.get("embed")
                + "' where fancy = '"
                + this.keyValue.get("fancy")
                + "'";
        stmt.executeUpdate(query);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
      if (engine.isConnectionPooling() && conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
    return new NounMetadata("Updated " + this.keyValue.get("fancy"), PixelDataType.CONST_STRING);
  }
}
