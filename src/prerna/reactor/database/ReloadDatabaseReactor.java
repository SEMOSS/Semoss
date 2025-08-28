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
package prerna.reactor.database;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ReloadDatabaseReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(ReloadDatabaseReactor.class);

  public ReloadDatabaseReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);

    // make sure user has at least edit access
    if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
      if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
        throw new IllegalArgumentException("User does not have permission to reload the database");
      }
    }

    IDatabaseEngine database = Utility.getDatabase(databaseId);
    String smssFilePath = database.getSmssFilePath();
    try {
      database.close();
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    database.setSmssProp(null);
    try {
      database.open(smssFilePath);
    } catch (Exception e1) {
      throw new IllegalArgumentException(
          "An error occurred reloading the database. Please reach out to an administrator for assistance");
    }

    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
