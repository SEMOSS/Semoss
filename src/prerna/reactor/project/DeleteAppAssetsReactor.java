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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteAppAssetsReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(DeleteAppAssetsReactor.class);

  public DeleteAppAssetsReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PROJECT.getKey(),
          ReactorKeysEnum.FILE_PATH.getKey(),
          ReactorKeysEnum.COMMENT_KEY.getKey()
        };
    this.keyRequired = new int[] {1, 1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();
    // check if user is logged in
    if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
      throwAnonymousUserError();
    }

    String projectId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
      throw new IllegalArgumentException(
          "Project " + projectId + " does not exist or user does not have access to edit assets.");
    }
    IProject project = Utility.getProject(projectId);

    // Retrieve all file names and contents
    // get the list of file paths to delete
    List<String> filePaths = getNounAsStringList(this.keysToGet[1]);
    if (filePaths == null || filePaths.isEmpty()) {
      throw new IllegalArgumentException("Must pass in at least one file name to delete");
    }

    String versionGitFolder =
        AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
    String assetFolder =
        AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());

    String comment = this.keyValue.get(this.keysToGet[2]);
    if (comment == null) {
      comment = "remove: DeleteAppAssets executed";
    }

    // Prepare to collect Git relative paths and actual File objects
    List<String> gitRelativeFilePaths = new ArrayList<>();
    List<File> deletedFiles = new ArrayList<>();

    // iterate each provided path and delete it
    for (String rawPath : filePaths) {
      String inputFilePath = Utility.normalizePath(rawPath.trim());
      if (inputFilePath == null || inputFilePath.isEmpty()) {
        continue;
      }

      String realFilePath = assetFolder + "/" + inputFilePath;
      realFilePath = realFilePath.replace("\\", "/");
      File realFile = new File(realFilePath);
      if (!realFile.exists()) {
        classLogger.warn("Cannot find the folder/file at path {}. Skipping.", inputFilePath);
        continue;
      }

      if (realFile.isDirectory()) {
        try {
          FileUtils.deleteDirectory(realFile);
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
          throw new IllegalArgumentException(
              "Error occurred trying to delete folder at path " + inputFilePath);
        }
      } else {
        try {
          FileUtils.forceDelete(realFile);
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
          throw new IllegalArgumentException(
              "Error occurred trying to delete file at path " + inputFilePath);
        }
      }

      // Collect for Git and cluster sync
      gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + inputFilePath);
      deletedFiles.add(realFile);
    }

    if (deletedFiles.isEmpty()) {
      throw new IllegalArgumentException("Could not find any of the files passed in to delete");
    }

    // Get the user's email
    AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
    String email = accessToken.getEmail();
    String author = accessToken.getUsername();

    GitDestroyer.removeSpecificFiles(versionGitFolder, true, gitRelativeFilePaths);
    // commit it
    GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
    // handle synchronization to the cloud
    ClusterUtil.pushProjectFolder(project, assetFolder);

    NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
    return retNoun;
  }

  @Override
  public String getReactorDescription() {
    return "Delete a single or multiple files in the projects assets folder";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
      return "The unique id for the project/app";
    } else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "Names of the file(s) to delete. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
    } else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
      return "Comment to add while removing the files within the git repository for the project";
    }
    return super.getDescriptionForKey(key);
  }
}
