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
package prerna.testing.reactor.database.upload.rdbms.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.reactor.database.upload.UploadTestUtility;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsUploadTableDataReactorTests extends AbstractBaseSemossApiTests {

  // @Test
  public void testUploadMovies() {
    // upload file
    String delimiter = ApiTestsSemossConstants.DELIMITER;
    Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
    UploadTestUtility.uploadFile(filePath.toString());

    // run pixel
    Map<String, Object> predictedTypes =
        UploadTestUtility.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter);
    Map<String, Object> dataTypes = (Map<String, Object>) predictedTypes.get("dataTypes");
    boolean exists = false;
    String databaseName = "MOV_DB";
    Map<String, Object> dbInfo =
        UploadTestUtility.rdbmsUploadTable(
            databaseName,
            ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME,
            delimiter,
            dataTypes,
            exists);

    // test output
    assertEquals(CATALOG_TYPE.DATABASE.toString(), dbInfo.get("database_type"));
    assertEquals(databaseName, dbInfo.get("database_name"));
    assertEquals(databaseName.toLowerCase(), dbInfo.get("low_database_name"));
    assertEquals("$", dbInfo.get("database_cost"));
    String dbId = (String) dbInfo.get("database_id");
    assertTrue(dbId != null && !dbId.isEmpty());
    assertFalse((boolean) dbInfo.get("database_discoverable"));
    assertFalse((boolean) dbInfo.get("database_global"));
    assertEquals(ApiTestsSemossConstants.USER_NAME, dbInfo.get("database_created_by"));
    assertEquals(RdbmsTypeEnum.H2_DB.getLabel(), dbInfo.get("database_subtype"));
    assertEquals("Native", dbInfo.get("database_created_by_type"));
  }
}
