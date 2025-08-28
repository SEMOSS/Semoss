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
package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.reactor.database.upload.gremlin.AbstractCreateExternalGraphReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;

public class CreateExternalGraphDatabaseReactor extends AbstractCreateExternalGraphReactor {

  private File file;
  private String filePath;
  private TinkerEngine.TINKER_DRIVER tinkerDriver;

  public CreateExternalGraphDatabaseReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.DATABASE.getKey(),
          ReactorKeysEnum.FILE_PATH.getKey(),
          ReactorKeysEnum.SPACE.getKey(),
          ReactorKeysEnum.GRAPH_TYPE_ID.getKey(),
          ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
          ReactorKeysEnum.GRAPH_METAMODEL.getKey(),
          ReactorKeysEnum.USE_LABEL.getKey()
        };
  }

  @Override
  protected void validateUserInput() throws IOException {
    this.filePath = UploadInputUtility.getFilePath(this.store, this.insight);
    if (!(this.file = new File(this.filePath)).exists()) {
      SemossPixelException exception =
          new SemossPixelException(
              new NounMetadata(
                  "Could not find file to save.",
                  PixelDataType.CONST_STRING,
                  PixelOperationType.ERROR));
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }

    this.tinkerDriver = TinkerEngine.TINKER_DRIVER.NEO4J;
    if (new File(this.filePath).isFile() && this.filePath.contains(".")) {
      String fileExtension = FilenameUtils.getExtension(this.filePath);
      this.tinkerDriver = TinkerEngine.TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
    }

    // neo4j is an entire directory and not a file
    // so ignore this case for now
    // FE does not even allow a passing of the file
    if (this.tinkerDriver != TinkerEngine.TINKER_DRIVER.NEO4J) {
      // move the file over to the correct location
      // and then update the host value
      String newLocation =
          this.databaseFolder.getAbsolutePath()
              + DIR_SEPARATOR
              + FilenameUtils.getName(this.file.getAbsolutePath());
      File updatedFileLoc = new File(newLocation);
      try {
        FileUtils.copyFile(this.file, updatedFileLoc);
        this.file = updatedFileLoc;
      } catch (IOException e) {
        throw new IOException("Unable to relocate uploaded file to correct database folder");
      }
    }
  }

  @Override
  protected File generateTempSmss(File owlFile) throws IOException {
    // the file path will become parameterized inside
    return UploadUtilities.generateTemporaryExternalTinkerSmss(
        this.newDatabaseId,
        this.newDatabaseName,
        owlFile,
        this.filePath,
        this.typeMap,
        this.nameMap,
        this.tinkerDriver,
        useLabel());
  }

  @Override
  protected IDatabaseEngine generateEngine() throws Exception {
    TinkerEngine tinkerEng = new TinkerEngine();
    tinkerEng.setEngineId(this.newDatabaseId);
    tinkerEng.setEngineName(this.newDatabaseName);
    tinkerEng.open(this.smssFile.getAbsolutePath());
    return tinkerEng;
  }
}
