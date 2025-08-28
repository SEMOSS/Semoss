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
package prerna.reactor.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class CopyEnginePermissionsReactor extends AbstractReactor {

  // TODO: make equivalent for project permissions
  // TODO: make equivalent for project permissions
  // TODO: make equivalent for project permissions
  // TODO: make equivalent for project permissions
  // TODO: make equivalent for project permissions

  private static final Logger logger = LogManager.getLogger(CopyEnginePermissionsReactor.class);

  private static final String SOURCE_ENGINE = "sourceEngine";
  private static final String TARGET_ENGINE = "targetEngine";

  public CopyEnginePermissionsReactor() {
    this.keysToGet = new String[] {SOURCE_ENGINE, TARGET_ENGINE};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String sourceEngineId = this.keyValue.get(this.keysToGet[0]);
    String targetEngineId = this.keyValue.get(this.keysToGet[1]);

    // must be an editor for both to run this
    if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), sourceEngineId)) {
      throw new IllegalArgumentException("You do not have edit access to the source engine");
    }
    if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), targetEngineId)) {
      throw new IllegalArgumentException("You do not have edit access to the target engine");
    }

    // now perform the operation
    try {
      SecurityEngineUtils.copyEnginePermissions(sourceEngineId, targetEngineId);
    } catch (Exception e) {
      logger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(
          "An error occurred copying the engine permissions.  Detailed error: " + e.getMessage());
    }

    String sourceDatabase = SecurityEngineUtils.getEngineAliasForId(sourceEngineId);
    String targetDatabase = SecurityEngineUtils.getEngineAliasForId(targetEngineId);

    return new NounMetadata(
        "Copied permissions from database "
            + sourceDatabase
            + "__"
            + sourceEngineId
            + " to "
            + targetDatabase
            + "__"
            + targetEngineId,
        PixelDataType.CONST_STRING);
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(SOURCE_ENGINE)) {
      return "The engine id used to copy permissions from";
    } else if (key.equals(TARGET_ENGINE)) {
      return "The engine id to copy permissions to";
    }
    return ReactorKeysEnum.getDescriptionFromKey(key);
  }
}
