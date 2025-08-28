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

import org.junit.jupiter.api.Test;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestProjectUtils;

public class AdminGetProjectAvailableReactorsReactorApiTests extends AbstractBaseSemossApiTests {

  @Test
  public void execute() {

    String project = TestProjectUtils.createBasicProject("testProject");
    String reactor =
        "AdminGetProjectAvailableReactors"; // make sure to include reactor name without "reactor"
    // at the end!!
    String pixel =
        ApiSemossTestUtils.buildPixelCall(reactor, ReactorKeysEnum.PROJECT.getKey(), project);

    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

    String projectReactors = nm.getValue().toString();
    assertNotNull(nm);
    assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
    assertEquals(projectReactors, nm.getValue().toString());
    assertEquals(projectReactors, "[]");
  }
}
