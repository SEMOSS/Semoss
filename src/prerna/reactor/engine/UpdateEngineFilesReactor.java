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
package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateEngineFilesReactor extends AbstractEngineFileReactor {

  private static final Logger classLogger = LogManager.getLogger(UpdateEngineFilesReactor.class);

  public UpdateEngineFilesReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PAYLOAD.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    validateUserAndEngineAccess(user);

    String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
    if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
      throw new IllegalArgumentException(
          "Engine " + engineId + " does not exist or user does not have access to edit assets.");
    }

    String enginePath = getLocalEngineBaseDirectory(engineId);

    Map<String, Object> responseData;

    try {
      responseData = updateEngineFiles(enginePath);
    } catch (IOException e) {
      classLogger.error("Error processing files", e);
      throw new RuntimeException("File processing failed: " + e.getMessage(), e);
    }

    return new NounMetadata(
        responseData, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> updateEngineFiles(String enginePath) throws IOException {
    Map<String, Object> payload = getPayload();
    File engineBaseDir = new File(enginePath);
    Set<String> currentPaths = new HashSet<>();

    writeFilesRecursively(engineBaseDir.toPath(), payload, currentPaths);
    deleteRemovedFiles(engineBaseDir, currentPaths);

    return traverseDirectory(enginePath); // Return updated structure
  }
}
