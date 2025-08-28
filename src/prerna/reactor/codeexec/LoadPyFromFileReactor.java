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
package prerna.reactor.codeexec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;

public class LoadPyFromFileReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(LoadPyFromFileReactor.class);

  public LoadPyFromFileReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.FILE_PATH.getKey(),
          ReactorKeysEnum.ALIAS.getKey(),
          ReactorKeysEnum.SPACE.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    // this also validates
    String filePath = UploadInputUtility.getFilePath(this.store, this.insight);

    String appFolder = null;

    if (keyValue.containsKey(keysToGet[2])) {
      String space = keyValue.get(keysToGet[2]);
      if (space != AssetUtility.INSIGHT_SPACE_KEY && space != AssetUtility.USER_SPACE_KEY) {
        appFolder = AssetUtility.getProjectAssetsFolder(space) + "/" + Constants.PY_BASE_FOLDER;
        appFolder = appFolder.replace("\\", "/");
      }
    }

    String alias = keyValue.get(keysToGet[1]);

    try {
      if (appFolder != null) {
        String script =
            alias
                + " = smssutil.load_module_from_file(module_name='"
                + alias
                + "', file_path='"
                + filePath
                + "', search='"
                + appFolder
                + "')";
        this.insight.getPyTranslator().runScript(script);
      } else {
        String script =
            alias
                + " = smssutil.load_module_from_file(module_name='"
                + alias
                + "', file_path='"
                + filePath
                + "', search=None)";
        this.insight.getPyTranslator().runScript(script);
      }
      return new NounMetadata("Variable set " + alias, PixelDataType.CONST_STRING);
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException("Unable to load python file as module");
    }
  }
}
