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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class BrowseEngineAssetsReactor extends AbstractReactor {

  public BrowseEngineAssetsReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();
    // check if user is logged in
    if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
      throwAnonymousUserError();
    }

    String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
    if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
      throw new IllegalArgumentException(
          "Engine " + engineId + " does not exist or user does not have access to edit assets.");
    }

    String relativeFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
    if (relativeFilePath != null) {
      relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
      if (!relativeFilePath.isEmpty()) {
        relativeFilePath = relativeFilePath.replace('\\', '/');
        if (!relativeFilePath.startsWith("/")) {
          relativeFilePath = "/" + relativeFilePath;
        }
      }
    }

    String pathSubstring = EngineUtility.getSpecificEngineBaseFolder(engineId);
    int pathSubstringIndex = pathSubstring.length();
    String filePath = pathSubstring;
    if (relativeFilePath != null && !relativeFilePath.isEmpty()) {
      filePath += relativeFilePath;
    }

    File directory = new File(filePath);
    if (!directory.exists()) {
      throw new IllegalArgumentException(
          "The directory " + relativeFilePath + " does not exist within the engine folder");
    }
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException(
          "The path "
              + relativeFilePath
              + " exists within the engine folder but is not a directory");
    }

    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(user.getZoneId());

    List<Map<String, Object>> retObj = new ArrayList<>();
    File[] allFiles = directory.listFiles();
    for (File f : allFiles) {
      if (f.getName().startsWith(".") && f.isDirectory()) {
        // we dont want to show this
        continue;
      }
      Map<String, Object> fileMap = new HashMap<>();
      fileMap.put("name", f.getName());
      if (f.isDirectory()) {
        fileMap.put("type", "directory");
      } else {
        fileMap.put("type", FilenameUtils.getExtension(f.getName()));
      }
      fileMap.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
      fileMap.put("path", f.getAbsolutePath().substring(pathSubstringIndex));
      retObj.add(fileMap);
    }

    NounMetadata retNoun =
        new NounMetadata(retObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    return retNoun;
  }

  @Override
  public String getReactorDescription() {
    return "List the files and directories from a relative filePath input from within the engine folder";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
      return "The unique id for the engine";
    } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "The relative file path to list contents from.";
    }
    return super.getDescriptionForKey(key);
  }
}
