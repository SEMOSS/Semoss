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
package prerna.reactor.workspace;

import java.io.File;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class MoveUserAssetReactor extends AbstractReactor {

  private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

  public MoveUserAssetReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.RELATIVE_PATH.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String currentFilePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
    if (currentFilePath == null || currentFilePath.isEmpty()) {
      throw new IllegalArgumentException("Must input file path for the user file");
    }
    currentFilePath = Utility.normalizePath(currentFilePath);

    String newFilePath = this.keyValue.get(this.keysToGet[1]);
    if (newFilePath == null || newFilePath.isEmpty()) {
      throw new IllegalArgumentException("Must provide new file path or name for file");
    }
    newFilePath = Utility.normalizePath(newFilePath);

    File currentFile = new File(currentFilePath);
    if (!currentFile.exists()) {
      throw new IllegalArgumentException("File does not exist at this location");
    }

    String assetProjectId = null;
    User user = this.insight.getUser();
    if (user != null) {
      AuthProvider token = user.getPrimaryLogin();
      if (token != null) {
        assetProjectId = user.getAssetProjectId(token);
        Utility.getProject(assetProjectId);
      }
    }

    if (assetProjectId == null) {
      throw new IllegalArgumentException("Unable to find Asset App ID for user");
    }

    String userFolderPath =
        AssetUtility.getRootFolderPath(this.insight, AssetUtility.USER_SPACE_KEY, true);
    File userFolder = new File(userFolderPath);
    if (!userFolder.exists()) {
      throw new IllegalArgumentException("Unable to find user asset app directory");
    }

    String newRelativePath = userFolderPath + newFilePath;
    Boolean moved = currentFile.renameTo(new File(newRelativePath));
    return new NounMetadata(moved, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
  }
}
