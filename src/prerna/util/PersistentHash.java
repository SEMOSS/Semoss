/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

public class PersistentHash {

  private static final Logger classLogger = LogManager.getLogger(PersistentHash.class);

  // simple hash table that saves and gets values from the database
  private static final String TABLE_NAME = "KVSTORE";

  private RDBMSNativeEngine engine = null;

  public Hashtable<String, String> thisHash = new Hashtable<String, String>();
  boolean dirty = false;

  public PersistentHash() {}

  public void setEngine(RDBMSNativeEngine engine) {
    this.engine = engine;
  }

  public void load() {
    if (engine == null) {
      return;
    }

    // this is only for local master!!!
    Connection conn = null;
    try {
      conn = engine.getConnection();
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT K, V from " + TABLE_NAME);
        while (rs.next()) {
          this.thisHash.put(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException e) {
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      try {
        if (engine.isConnectionPooling() && conn != null) {
          conn.close();
        }
      } catch (SQLException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }
  }

  public void put(String key, String value) {
    this.thisHash.put(key, value);
    dirty = true;
  }

  public boolean containsKey(String key) {
    return this.thisHash.containsKey(key);
  }

  public String get(String key) {
    return this.thisHash.get(key);
  }

  public void persistBack() {
    if (engine != null && this.dirty) {
      Connection conn = null;
      Statement stmt = null;
      PreparedStatement ps = null;
      try {
        conn = engine.getConnection();
        stmt = conn.createStatement();
        stmt.execute("DELETE FROM " + TABLE_NAME);
        Enumeration<String> keys = thisHash.keys();
        ps = conn.prepareStatement("INSERT KVSTORE(K, V) VALUES(?, ?)");
        while (keys.hasMoreElements()) {
          String key = keys.nextElement();
          String value = thisHash.get(key);
          int parameterIndex = 1;
          ps.setString(parameterIndex++, key);
          ps.setString(parameterIndex++, value);
          ps.addBatch();
        }
        ps.executeBatch();
        if (!ps.getConnection().getAutoCommit()) {
          ps.getConnection().commit();
        }
        this.dirty = false;
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
      } finally {
        ConnectionUtils.closeStatement(stmt);
        ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
      }
    }
  }

  /**
   * See if we can init the persistent hash
   *
   * @param engine
   * @return
   */
  public static boolean canInit(RDBMSNativeEngine engine, Connection conn) {
    try {
      if (engine == null) {
        return false;
      }

      AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
      if (queryUtil == null) {
        return false;
      }
      // make sure the KVSTORE table exists
      return queryUtil.tableExists(conn, TABLE_NAME, engine.getDatabase(), engine.getSchema());
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      try {
        if (engine.isConnectionPooling() && conn != null) {
          conn.close();
        }
      } catch (SQLException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }
    return false;
  }
}
