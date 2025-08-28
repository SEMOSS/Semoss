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
package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import prerna.auth.utils.reactors.admin.AdminGetEngineSMSSReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;

public class AdminGetEngineSMSSReactorApiTests extends AbstractBaseSemossApiTests {

  private Path smssFilePath;
  private String engine;

  @Test
  public void executeWithEngineKey() throws IOException {
    // create engine
    String engine = ApiSemossTestEngineUtils.createBasicEngine();
    smssFilePath = Files.createTempFile("test-smss", ".txt");
    Files.write(smssFilePath, createSmssFileContent(engine).getBytes());
    String concealedSmssContent = createSmssFileContent(engine);

    // run reactor
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            AdminGetEngineSMSSReactor.class, ReactorKeysEnum.ENGINE.getKey(), engine);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

    assertNotNull(nm);
    assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
    assertEquals(concealedSmssContent, nm.getValue().toString());
  }

  private String createSmssFileContent(String engine) {
    // Create realistic SMSS file content that will be processed by SmssUtilities
    return "#Base Properties\n"
        + "ENGINE\t"
        + engine
        + "\n"
        + "ENGINE_ALIAS\ttest\n"
        + "ENGINE_TYPE\tprerna.engine.impl.rdbms.RDBMSNativeEngine\n"
        + "OWL\tdb/@ENGINE@/test_OWL.OWL\n"
        + "RDBMS_TYPE\tH2_DB\n"
        + "DRIVER\torg.h2.Driver\n"
        + "USERNAME\tsa\n"
        + "PASSWORD\t********\n"
        + "CONNECTION_URL\tjdbc:h2:nio:@BaseFolder@/db/@ENGINE@/database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768\n";
  }
}
