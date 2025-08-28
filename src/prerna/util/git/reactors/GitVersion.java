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
package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class GitVersion extends AbstractReactor {

  public GitVersion() {
    this.keysToGet = new String[] {"app"};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    String baseFolder = Utility.getBaseFolder();
    String appFolder = baseFolder + "db/" + keyValue.get(keysToGet[0]);

    Logger logger = getLogger(this.getClass().getName());
    logger.info("Converting " + appFolder + " to a versionable app");
    logger.info("Checking to see if it is already versioned");

    if (GitUtils.isGit(appFolder)) {
      logger.info("App is already versionable");
    } else {
      logger.info("Creating initial version");
      GitRepoUtils.makeLocalDatabaseGitVersionFolder(appFolder);
      // we create a version folder
      String versionFolder = appFolder + "/version";
      GitPushUtils.addAllFiles(versionFolder, false);
      GitPushUtils.commitAddedFiles(versionFolder);
    }
    logger.info("Complete");

    return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
  }
}
