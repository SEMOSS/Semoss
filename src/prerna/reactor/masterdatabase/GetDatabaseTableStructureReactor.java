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
package prerna.reactor.masterdatabase;

import java.util.List;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class GetDatabaseTableStructureReactor extends AbstractReactor {

  /*
   * PAYLOAD MUST MATCH THAT OF
   * {@link prerna.sablecc2.reactor.frame.GetFrameTableStructureReactor}
   */

  private static final String CLASS_NAME = GetDatabaseTableStructureReactor.class.getName();

  public GetDatabaseTableStructureReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);
    if (engineId == null) {
      throw new IllegalArgumentException(
          "Need to define the database to get the structure from from");
    }
    engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);

    // account for security
    // TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "Database does not exist or user does not have access to database");
    }

    Logger logger = getLogger(CLASS_NAME);
    logger.info("Pulling database structure for database " + engineId);
    // if cache exists, return from there
    List<Object[]> data = EngineSyncUtility.getDatabaseStructureCache(engineId);
    if (data == null) {
      data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
      // store the cache for the database structure
      EngineSyncUtility.setDatabaseStructureCache(engineId, data);
    }
    return new NounMetadata(
        data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
  }
}
