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
package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.auth.utils.reactors.admin.AdminGetProjectPortalDetailsReactor;
import prerna.auth.utils.reactors.admin.AdminProjectInfoReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;
import prerna.testing.utility.TestProjectUtils;

public class AdminProjectInfoReactorApiTests extends AbstractBaseSemossApiTests{
	
	@Test
	public void executeWithoutMetakeys() {
		String project = TestProjectUtils.createBasicProject("testProject");

		String pixel = ApiSemossTestUtils.buildPixelCall(AdminProjectInfoReactor.class,
				ReactorKeysEnum.PROJECT.getKey(), project);

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		
		assertFalse(Boolean.valueOf(retValue.get("project_discoverable").toString()));
		assertFalse(Boolean.valueOf(retValue.get("project_has_portal").toString()));
		assertTrue(Boolean.valueOf(retValue.get("project_global").toString()));
		
		assertEquals(project, retValue.get("project_id").toString());
		assertEquals("NATIVE", retValue.get("project_created_by_type").toString());
		assertEquals("INSIGHTS", retValue.get("project_type").toString());
		assertEquals("testProject", retValue.get("project_name").toString());
		assertEquals("testproject", retValue.get("low_project_name").toString());
	}
	
	@Test
	public void executeWithMetakeysExcludesMarkdown() {
		String project = TestProjectUtils.createBasicProject("testProject");
		
		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("markdown", "### test markdown");
		TestProjectUtils.setProjectMetadata(project, map);
		
		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("markdown");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminProjectInfoReactor.class, ReactorKeysEnum.PROJECT.getKey(), 
				project, ReactorKeysEnum.META_KEYS.getKey(), metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		
		assertFalse(Boolean.valueOf(retValue.get("project_discoverable").toString()));
		assertFalse(Boolean.valueOf(retValue.get("project_has_portal").toString()));
		assertTrue(Boolean.valueOf(retValue.get("project_global").toString()));
		
		assertEquals(project, retValue.get("project_id").toString());
		assertEquals("NATIVE", retValue.get("project_created_by_type").toString());
		assertEquals("INSIGHTS", retValue.get("project_type").toString());
		assertEquals("testProject", retValue.get("project_name").toString());
		assertEquals("testproject", retValue.get("low_project_name").toString());
		
		assertFalse(retValue.containsKey("markdown"));
        assertTrue(retValue.containsKey("description"));
	}
	
	@Test
	public void executeWithMultipleMetakeys() {
		String project = TestProjectUtils.createBasicProject("testProject");
		
		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("domain", "test domain");
		TestProjectUtils.setProjectMetadata(project, map);
		
		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("domain");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminProjectInfoReactor.class, ReactorKeysEnum.PROJECT.getKey(), 
				project, ReactorKeysEnum.META_KEYS.getKey(), metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		
		assertFalse(Boolean.valueOf(retValue.get("project_discoverable").toString()));
		assertFalse(Boolean.valueOf(retValue.get("project_has_portal").toString()));
		assertTrue(Boolean.valueOf(retValue.get("project_global").toString()));
		
		assertEquals(project, retValue.get("project_id").toString());
		assertEquals("NATIVE", retValue.get("project_created_by_type").toString());
		assertEquals("INSIGHTS", retValue.get("project_type").toString());
		assertEquals("testProject", retValue.get("project_name").toString());
		assertEquals("testproject", retValue.get("low_project_name").toString());
		
        assertTrue(retValue.containsKey("description"));
        assertTrue(retValue.containsKey("domain"));
	}
}
