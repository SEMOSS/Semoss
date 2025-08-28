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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GetEngineAssetsReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(GetEngineAssetsReactor.class);

  public GetEngineAssetsReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();
    // check if user is logged in
    if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
      throwAnonymousUserError();
    }

    String engineId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
      throw new IllegalArgumentException(
          "Engine " + engineId + " does not exist or user does not have access to edit assets.");
    }
    // force to pull it from cloud if not in the container
    Utility.getEngine(engineId);

    String filePath = this.keyValue.get(this.keysToGet[1]);
    if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
      throw new IllegalArgumentException("Must pass a filePath for the file to retrieve");
    }
    filePath = filePath.replace("\\", "/");
    if (!filePath.startsWith("/")) {
      filePath = "/" + filePath;
    }
    filePath = Utility.normalizePath(filePath);

    String assetFolder = EngineUtility.getSpecificEngineBaseFolder(engineId);

    String output = null;
    // just read the current file
    String assetFilePath = assetFolder + filePath;
    File assetFile = new File(assetFilePath);
    if (!assetFile.exists()) {
      throw new IllegalArgumentException("The filePath " + filePath + " does not exist");
    }
    if (!assetFile.isFile()) {
      throw new IllegalArgumentException("The filePath " + filePath + " exists but is not a file");
    }
    try {
      output = FileUtils.readFileToString(new File(assetFilePath), Charset.forName("UTF-8"));
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException("Unable to read file " + filePath);
    }

    return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
  }

  @Override
  public String getReactorDescription() {
    return "Retrieve the contents of a file in the engine";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
      return "The unique id for the engine";
    } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "Names of the file to get the contents";
    }
    return super.getDescriptionForKey(key);
  }
}
