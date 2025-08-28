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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.InsightFile;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class DownloadAppNotebookReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(DownloadAppNotebookReactor.class);

  /*
   * This class is used to construct a new project
   * This project only contains insights
   */

  public DownloadAppNotebookReactor() {
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

    IProject project = Utility.getProject(projectId);
    List<File> notebookFiles = project.writeNotebooks();
    if (notebookFiles == null) {
      throw new IllegalArgumentException("The project is not a blocks based app");
    }
    File download = null;
    if (notebookFiles.size() == 1) {
      download = notebookFiles.get(0);
    } else {
      File f = notebookFiles.get(0);
      download = new File(this.insight.getInsightFolder() + "/notebooks.zip");

      // create a zip
      ZipOutputStream zos = null;
      try {
        zos = ZipUtils.zipFolder(f.getParent(), download.getAbsolutePath());
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
        throw new IllegalArgumentException("Unable to zip and download the notebooks");
      } finally {
        try {
          if (zos != null) {
            zos.flush();
            zos.close();
          }
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
          throw new IllegalArgumentException("Unable to zip and download the notebooks");
        }
      }
    }

    String downloadKey = UUID.randomUUID().toString();
    InsightFile insightFile = new InsightFile();
    insightFile.setFileKey(downloadKey);
    insightFile.setDeleteOnInsightClose(false);
    insightFile.setFilePath(download.getAbsolutePath());

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
