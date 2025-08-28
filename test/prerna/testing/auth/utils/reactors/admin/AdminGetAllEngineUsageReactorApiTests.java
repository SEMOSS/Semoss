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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.auth.utils.reactors.admin.AdminGetAllEngineUsageReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;

public class AdminGetAllEngineUsageReactorApiTests extends AbstractBaseSemossApiTests {

  @Test
  public void test() {
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            AdminGetAllEngineUsageReactor.class,
            ReactorKeysEnum.ENGINE.getKey(),
            ApiSemossTestEngineUtils.createBasicEngine(),
            ReactorKeysEnum.LIMIT.getKey(),
            "10",
            ReactorKeysEnum.OFFSET.getKey(),
            "5");

    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

    assertNotNull(nm);
    assertEquals(PixelDataType.FORMATTED_DATA_SET, nm.getNounType());
    assertEquals(new ArrayList<Map<String, Object>>().toString(), nm.getValue().toString());
  }
}
