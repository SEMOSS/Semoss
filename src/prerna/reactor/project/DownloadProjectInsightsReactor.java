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
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class DownloadProjectInsightsReactor extends AbstractReactor {

  private static final Logger classLogger =
      LogManager.getLogger(DownloadProjectInsightsReactor.class);

  /*
   * This class is used to construct a new project
   * This project only contains insights
   */

  public DownloadProjectInsightsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();

    String projectId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
      throw new IllegalArgumentException(
          "Project " + projectId + " does not exist or user does not have access to edit assets.");
    }

    File insightsFile = null;
    try {
      insightsFile =
          SecurityProjectUtils.createInsightsDatabase(projectId, this.insight.getInsightFolder());
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(
          "Error occurred attemping to generate the insights database for this project");
    }

    String downloadKey = UUID.randomUUID().toString();
    InsightFile insightFile = new InsightFile();
    insightFile.setFileKey(downloadKey);
    insightFile.setDeleteOnInsightClose(false);
    insightFile.setFilePath(insightsFile.getAbsolutePath());

    // store the insight file
    // in the insight so the FE can download it
    // only from the given insight
    this.insight.addExportFile(downloadKey, insightFile);

    NounMetadata retNoun =
        new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
    retNoun.addAdditionalReturn(
        NounMetadata.getSuccessNounMessage("Successfully generated the csv file"));
    return retNoun;
  }
}
