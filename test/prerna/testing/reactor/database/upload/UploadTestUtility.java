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
package prerna.testing.reactor.database.upload;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import prerna.reactor.database.upload.PredictDataTypesReactor;
import prerna.reactor.database.upload.rdbms.csv.RdbmsUploadTableDataReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestInsightUtils;
import prerna.testing.ApiSemossTestUtils;

/** Test Utility to upload an engine */
public class UploadTestUtility {

  /**
   * Upload file to insight folder
   *
   * @param filePath
   * @throws IOException
   */
  public static void uploadFile(String filePath) {
    // copy file to insight folder
    // TODO use a reactor to do this and test output
    File movieFile = new File(filePath);
    File insightFolder = new File(ApiSemossTestInsightUtils.getInsight().getInsightFolder());
    try {
      FileUtils.copyFileToDirectory(movieFile, insightFolder);
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  public static Map<String, Object> predictDataTypes(String filePath, String delimiter) {
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            PredictDataTypesReactor.class,
            ReactorKeysEnum.FILE_PATH.getKey(),
            filePath,
            ReactorKeysEnum.DELIMITER.getKey(),
            delimiter);
    NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
    Map<String, Object> retMap = (Map<String, Object>) noun.getValue();
    assertFalse(retMap.isEmpty());
    return retMap;
  }

  public static Map<String, Object> rdbmsUploadTable(
      String databaseName,
      String filePath,
      String delimiter,
      Map<String, Object> dataTypeMap,
      boolean exists) {
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            RdbmsUploadTableDataReactor.class,
            ReactorKeysEnum.DATABASE.getKey(),
            databaseName,
            ReactorKeysEnum.FILE_PATH.getKey(),
            filePath,
            ReactorKeysEnum.DELIMITER.getKey(),
            delimiter,
            ReactorKeysEnum.DATA_TYPE_MAP.getKey(),
            dataTypeMap,
            ReactorKeysEnum.EXISTING.getKey(),
            exists);
    NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
    Map<String, Object> retMap = (Map<String, Object>) noun.getValue();
    assertFalse(retMap.isEmpty());
    return retMap;
  }
}
