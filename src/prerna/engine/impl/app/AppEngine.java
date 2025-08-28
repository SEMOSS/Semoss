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
package prerna.engine.impl.app;

import java.util.Properties;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.util.Constants;

@Deprecated // this is replaced by projects
public class AppEngine extends AbstractDatabaseEngine {

  private static final Logger LOGGER = LogManager.getLogger(AppEngine.class);

  /**
   * Overriding the default behavior Do not need to do anything except load the insights database
   *
   * @throws Exception
   */
  @Override
  public void open(Properties smssProp) {
    setSmssProp(smssProp);
    // get id & name
    this.engineId = this.smssProp.getProperty(Constants.ENGINE);
    this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
  }

  @Override
  public DATABASE_TYPE getDatabaseType() {
    return DATABASE_TYPE.APP;
  }

  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////

  /*
   * Need to clean the interface to allow for what we are doing
   * APP is a wrapper around a set of insights (parameterized insights)
   * Where we allow the swapping of data on the insights
   *
   * Since there is no data, the below are not needed
   */

  @Override
  public Object execQuery(String query) {
    return null;
  }

  @Override
  public void insertData(String query) {
    LOGGER.info("There is no data to store for an AppEngine!");
  }

  @Override
  public Vector<Object> getEntityOfType(String type) {
    return null;
  }

  @Override
  public void removeData(String query) {
    LOGGER.info("There is no data to store for an AppEngine!");
  }

  @Override
  public void commit() {
    LOGGER.info("There is no data to store for an AppEngine!");
  }

  @Override
  public boolean holdsFileLocks() {
    return false;
  }
}
