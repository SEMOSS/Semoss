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
package prerna.reactor.database.metaeditor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class SaveOwlPositionsReactor extends AbstractReactor {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  protected static final Logger classLogger = LogManager.getLogger(SaveOwlPositionsReactor.class);

  public SaveOwlPositionsReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.POSITION_MAP.getKey()};
  }

  @Override
  public NounMetadata execute() {
    String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
    if (databaseId == null) {
      throw new IllegalArgumentException("Must pass in the database id");
    }
    // run security tests + alias replacement
    databaseId = testDatabaseId(databaseId, true);
    Map<String, Object> positions = getPositionMap();
    if (positions == null || positions.isEmpty()) {
      throw new IllegalArgumentException("Must pass in the valid position map");
    }

    // TODO: below does not even work/is wrong
    // TODO: need to make a method to push/pull the positions file

    // write the json file in the database folder
    // just put it in the same location as the OWL
    IDatabaseEngine database = Utility.getDatabase(databaseId);
    ClusterUtil.pullOwl(databaseId);
    File positionFile = database.getOwlPositionFile();

    FileWriter writer = null;
    try {
      writer = new FileWriter(positionFile);
      GSON.toJson(positions, writer);
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
    ClusterUtil.pushOwl(databaseId);
    // update the positions cache
    EngineSyncUtility.setMetamodelPositions(databaseId, positions);
    MasterDatabaseUtility.saveMetamodelPositions(databaseId, positions);

    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }

  private Map<String, Object> getPositionMap() {
    GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
    if (grs != null && !grs.isEmpty()) {
      List<NounMetadata> maps = grs.getNounsOfType(PixelDataType.MAP);
      if (maps != null && !maps.isEmpty()) {
        return (Map<String, Object>) maps.get(0).getValue();
      }
    }

    // check is passed as direct input
    List<NounMetadata> maps = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (maps != null && !maps.isEmpty()) {
      return (Map<String, Object>) maps.get(0).getValue();
    }

    return null;
  }
}
