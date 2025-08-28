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
package prerna.reactor.utils;

import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class CheckRecommendOptimizationReactor extends AbstractReactor {

  private static final Logger classLogger =
      LogManager.getLogger(CheckRecommendOptimizationReactor.class);

  public CheckRecommendOptimizationReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);
    databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);

    IDatabaseEngine database = Utility.getDatabase(databaseId);
    List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(databaseId);
    for (Object[] tableCol : allTableCols) {
      if (tableCol.length == 4) {
        String table = tableCol[0] + "";
        String col = tableCol[1] + "";
        String dataType = tableCol[2] + "";
        // only care about strings
        if (dataType != null && dataType.equals("STRING")) {
          String queryCol = col;
          String uniqueValQuery =
              "SELECT DISTINCT ?concept ?unique WHERE "
                  + "{ BIND(<http://semoss.org/ontologies/Concept/"
                  + queryCol
                  + "/"
                  + table
                  + "> AS ?concept)"
                  + "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
          IRawSelectWrapper it = null;
          try {
            it = database.getOWLEngineFactory().getReadOWL().query(uniqueValQuery);
            if (it.hasNext()) {
              // it had a unique value so we assume they're all there
              return new NounMetadata(true, PixelDataType.BOOLEAN);
            } else {
              // the first string value didn't have a unique count so
              // we assume none do
              return new NounMetadata(false, PixelDataType.BOOLEAN);
            }
          } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
          } finally {
            if (it != null) {
              try {
                it.close();
              } catch (IOException e) {
                classLogger.error(Constants.STACKTRACE, e);
              }
            }
          }
          return new NounMetadata(false, PixelDataType.BOOLEAN);
        }
      }
    }
    // none were strings so no need to get uniques
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
