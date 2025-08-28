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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.auth.utils.reactors.admin.AdminGetEngineMarkdownReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AdminGetEngineMarkdownReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void executeWithMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> map = new HashMap<>();
		map.put("markdown", "### test markdown");
		TestEngineUtilities.setEngineMetadata(engine, map);

		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetEngineMarkdownReactor.class,
				ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String retValue = (String) nm.getValue();
		assertEquals("### test markdown", retValue);
	}

	@Test
	public void executeWithoutMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetEngineMarkdownReactor.class,
				ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNull(nm.getValue());
	}
}
