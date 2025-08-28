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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AdminEngineInfoReactorApiTests extends AbstractBaseSemossApiTests {

	// @Test
	public void executeWithoutMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(),
				engine);

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("database_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_discoverable").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals("test", retValue.get("low_database_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("low_engine_name").toString());
		assertEquals("NATIVE", retValue.get("engine_created_by_type").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("app_name").toString());
		assertEquals("DATABASE", retValue.get("engine_type").toString());
		assertEquals("$", retValue.get("engine_cost").toString());
		assertEquals("H2_DB", retValue.get("engine_subtype").toString());
		assertEquals("user1", retValue.get("engine_created_by").toString());
		assertEquals(engine, retValue.get("app_id").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());
	}

	// @Test
	public void executeWithMetakeysExcludesMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("markdown", "### test markdown");
		TestEngineUtilities.setEngineMetadata(engine, map);

		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("markdown");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(),
				engine, ReactorKeysEnum.META_KEYS.getKey(), metaValues);

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("database_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_discoverable").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals("test", retValue.get("low_database_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("low_engine_name").toString());
		assertEquals("NATIVE", retValue.get("engine_created_by_type").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("app_name").toString());
		assertEquals("DATABASE", retValue.get("engine_type").toString());
		assertEquals("$", retValue.get("engine_cost").toString());
		assertEquals("H2_DB", retValue.get("engine_subtype").toString());
		assertEquals("user1", retValue.get("engine_created_by").toString());
		assertEquals(engine, retValue.get("app_id").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());

		assertFalse(retValue.containsKey("markdown"));
		assertTrue(retValue.containsKey("description"));
	}

	// @Test
	public void executeWithMultipleMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("domain", "test domain");
		TestEngineUtilities.setEngineMetadata(engine, map);

		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("domain");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(),
				engine, ReactorKeysEnum.META_KEYS.getKey(), metaValues);

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("database_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_discoverable").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals("test", retValue.get("low_database_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("low_engine_name").toString());
		assertEquals("NATIVE", retValue.get("engine_created_by_type").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("engine_name").toString());
		assertEquals("test", retValue.get("app_name").toString());
		assertEquals("DATABASE", retValue.get("engine_type").toString());
		assertEquals("$", retValue.get("engine_cost").toString());
		assertEquals("H2_DB", retValue.get("engine_subtype").toString());
		assertEquals("user1", retValue.get("engine_created_by").toString());
		assertEquals(engine, retValue.get("app_id").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());

		assertTrue(retValue.containsKey("description"));
		assertTrue(retValue.containsKey("domain"));
	}
}
