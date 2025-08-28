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
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class NewEngineAssetsDirectoryReactor extends AbstractReactor {

  /*
   * TODO: expose Git at engine level as well
   */

  private static final Logger classLogger =
      LogManager.getLogger(NewEngineAssetsDirectoryReactor.class);

  public NewEngineAssetsDirectoryReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    this.keyRequired = new int[] {1, 1};
    //				,
    //				ReactorKeysEnum.COMMENT_KEY.getKey() };
    //		this.keyRequired = new int[] {1,1,0};
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
    IEngine engine = Utility.getEngine(engineId);

    //		String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
    // project.getProjectId());
    String assetFolder = EngineUtility.getSpecificEngineBaseFolder(engineId);

    String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
    if (filePath == null || filePath.isEmpty()) {
      throw new IllegalArgumentException("Must provide a valid filePath");
    }
    //		String comment = this.keyValue.get(this.keysToGet[2]);
    //		if(comment == null) {
    //			comment = "add: creating new directory";
    //		}

    File directory = new File(assetFolder + "/" + filePath);
    try {
      directory.mkdirs();
      File placeholder = new File(directory.getAbsolutePath(), "placeholder.txt");
      FileUtils.writeStringToFile(placeholder, "placeholder", Charset.forName("UTF-8"));
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      NounMetadata error =
          NounMetadata.getErrorNounMessage("Unable to create directory: " + filePath);
      SemossPixelException exception = new SemossPixelException(error);
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }

    //		List<String> gitRelativeFilePaths = new ArrayList<>();
    //		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + filePath);
    //
    //		// Get the user's email
    //		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
    //		String email = accessToken.getEmail();
    //		String author = accessToken.getUsername();
    //
    //		GitRepoUtils.addSpecificFiles(gitFolder, gitRelativeFilePaths);
    //		// commit it
    //		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
    // handle synchronization to the cloud
    ClusterUtil.pushEngineFolder(engine, assetFolder);

    NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
    return retNoun;
  }

  @Override
  public String getReactorDescription() {
    return "Create a new empty directory in the engine folder";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
      return "The unique id for the engine";
    } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "Names of the file to create.";
    } else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
      return "Comment to add while creating and saving the new file within the git repository for the engine";
    }
    return super.getDescriptionForKey(key);
  }
}
