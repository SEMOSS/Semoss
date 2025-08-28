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
package prerna.tcp.client.workers;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.Utility;

public class EngineWorker implements Runnable {

  private static final Logger classLogger = LogManager.getLogger(EngineWorker.class);

  // responsible for doing all of the work from an engine's perspective
  // the server sends information to semoss core to execute something
  // this thread will work through in terms of executing it
  // and then send the response back
  SocketClient sc = null;
  PayloadStruct ps = null;
  public static final int MAX_ROWS = 50;

  public EngineWorker(SocketClient sc, PayloadStruct ps) {
    this.sc = sc;
    this.ps = ps;
  }

  @Override
  public void run() {

    try {
      // TODO Auto-generated method stub
      String engineId = ps.objId;

      // TODO: *****************need to do a security check *************
      User user = sc.getUser();
      boolean canAccess =
          SecurityEngineUtils.userIsOwner(user, engineId)
              || SecurityEngineUtils.userCanEditEngine(user, engineId)
              || SecurityEngineUtils.userCanViewEngine(user, engineId);

      if (canAccess) {
        IDatabaseEngine engine = Utility.getDatabase(engineId);
        Method method = findEngineMethod(engine, ps.methodName, ps.payloadClasses);
        Object retObject = method.invoke(engine, ps.payload);

        // the map that comes may not be fully serializable
        if (retObject instanceof Map && !(retObject instanceof CaseInsensitiveProperties)) {
          Map<String, Object> outputMap = normalizeMap((Map<String, Object>) retObject);
          ps.payload = new Object[] {outputMap};
        } else {
          // need to check for serialization
          ps.payload = new Object[] {retObject};
        }
      } else {
        ps.payload = new Object[] {"User does not have permission"};
        ps.payloadClasses = new Class[] {java.lang.String.class};
      }
      // got the response
      ps.response = true;

    } catch (Exception ex) {
      classLogger.error(Constants.STACKTRACE, ex);
      ps.ex = ex.getLocalizedMessage();
      ps.response = true;
    }
    sc.executeCommand(ps);
  }

  public Method findEngineMethod(IDatabaseEngine engine, String methodName, Class[] arguments) {
    Method retMethod = null;

    // look for it in the child class if not parent class
    // we can even cache this later
    try {
      if (arguments != null) {
        try {
          retMethod = engine.getClass().getDeclaredMethod(methodName, arguments);
        } catch (Exception ex) {

        }
        if (retMethod == null)
          retMethod = engine.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);

      } else {
        try {
          retMethod = engine.getClass().getDeclaredMethod(methodName);
        } catch (Exception ex) {

        }
        if (retMethod == null)
          retMethod = engine.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
      }
      // LOGGER.info("Found the method " + retMethod);
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      classLogger.error(Constants.STACKTRACE, e);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      classLogger.error(Constants.STACKTRACE, e);
    }
    return retMethod;
  }

  private Map<String, Object> normalizeMap(Map<String, Object> input) {
    // parse through the objects
    // if the object is not serializable no go
    // if the object is a result set
    // turn it into CachedRowsetImpl
    Map<String, Object> output = new HashMap<String, Object>();

    Iterator<String> keys = input.keySet().iterator();

    while (keys.hasNext()) {
      String key = keys.next();
      Object obj = input.get(key);

      if (obj instanceof ResultSet) {
        try {
          // move this CacheRowSetImpl
          CachedRowSet impl = RowSetProvider.newFactory().createCachedRowSet();
          impl.setMaxRows(MAX_ROWS);
          impl.populate((ResultSet) obj);
          output.put(key, impl);
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          classLogger.error(Constants.STACKTRACE, e);
        }
      } else if (obj instanceof Serializable) {
        output.put(key, obj);
      }
    }
    return output;
  }
}
