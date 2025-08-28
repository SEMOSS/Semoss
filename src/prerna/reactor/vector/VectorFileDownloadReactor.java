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
package prerna.reactor.vector;

import com.google.common.io.Files;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class VectorFileDownloadReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(VectorFileDownloadReactor.class);

  private final String FILE_NAMES = "fileNames";

  public VectorFileDownloadReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), FILE_NAMES};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "Vector db " + engineId + " does not exist or user does not have access to this engine");
    }

    List<String> fileNames = getFiles();
    if (fileNames == null || fileNames.isEmpty()) {
      throw new IllegalArgumentException(
          "Must provide the key '" + FILE_NAMES + "' for the files to download");
    }
    try {
      return getDownload(engineId, fileNames);
    } catch (SemossPixelException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(
          "Error occurred attempting to download the files. Detailed message = " + e.getMessage());
    }
  }

  /**
   * @param fileNames
   * @return
   * @throws IOException
   */
  private NounMetadata getDownload(String engineId, List<String> fileNames) throws IOException {
    String downloadKey = UUID.randomUUID().toString();

    IVectorDatabaseEngine vectorDb = Utility.getVectorDatabase(engineId);
    String engineName = vectorDb.getEngineName();
    String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);

    String vectorDbDocumentFilePath = vectorDb.getDocumentsFilesPath(null);
    String outputDir = this.insight.getInsightFolder();
    String outFilePath = null;

    List<String> warnings = new ArrayList<>();

    FileOutputStream fos = null;
    ZipOutputStream zos = null;
    try {
      if (fileNames.size() == 1) {
        String filepath = vectorDbDocumentFilePath + DIR_SEPARATOR + fileNames.get(0);
        File fileToCheck = new File(filepath);
        if (!fileToCheck.exists()) {
          throw new SemossPixelException(
              "File " + fileNames.get(0) + " does not exist in the vector db to download");
        }
        outFilePath = outputDir + DIR_SEPARATOR + fileNames.get(0);
        Files.copy(fileToCheck, new File(outFilePath));
      } else {
        outFilePath = outputDir + DIR_SEPARATOR + engineNameAndId + "_files.zip";
        fos = new FileOutputStream(outFilePath);
        zos = new ZipOutputStream(fos);

        int fileExistsCount = 0;
        for (String fileName : fileNames) {
          File filetozip = new File(vectorDbDocumentFilePath + DIR_SEPARATOR + fileName);
          if (filetozip.exists()) {
            ZipUtils.addToZipFile(filetozip, zos);
            fileExistsCount++;
          } else {
            warnings.add(fileName);
          }
        }
        if (fileExistsCount == 0) {
          throw new SemossPixelException(
              "None of the files selected to download exist in the vector db to download");
        }
      }
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    } finally {
      try {
        if (zos != null) {
          zos.flush();
          zos.close();
        }
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
      try {
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }

    InsightFile insightFile = new InsightFile();
    insightFile.setFileKey(downloadKey);
    insightFile.setDeleteOnInsightClose(true);
    insightFile.setFilePath(outFilePath);
    this.insight.addExportFile(downloadKey, insightFile);

    NounMetadata retNoun =
        new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
    if (!warnings.isEmpty()) {
      retNoun.addAdditionalReturn(
          NounMetadata.getWarningNounMessage(
              "Could not find some of the files to download: " + warnings));
    }
    return retNoun;
  }

  /**
   * @return list of files to download
   */
  public List<String> getFiles() {
    List<String> filePaths = new ArrayList<>();

    // see if added as key
    GenRowStruct grs = this.store.getNoun(FILE_NAMES);
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) {
        filePaths.add(grs.get(i).toString());
      }
      return filePaths;
    }

    throw new IllegalArgumentException("Must pass in the file names to download");
  }
}
