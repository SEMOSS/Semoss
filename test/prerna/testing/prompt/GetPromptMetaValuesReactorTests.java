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
package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityUserUtils;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

/**
 * Tests GetPromptMetaValuesReactor functionality.
 * Verifies that logged-in users can retrieve prompt metadata values.
 */
public class GetPromptMetaValuesReactorTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testCanGetPromptMetaValues() {
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);

		// Insert test metadata into PROMPTMETA table
		Map<String, Collection<String>> testMetadata = new HashMap<>();
		testMetadata.put("department", Arrays.asList("Engineering", "Sales", "Marketing"));
		testMetadata.put("region", Arrays.asList("North America", "Europe", "Asia"));

		List<Map<String, Object>> mockMetakeyOptions = new ArrayList<>();
		Map<String, Object> departmentOption = new HashMap<>();
		departmentOption.put("metakey", "department");
		Map<String, Object> regionOption = new HashMap<>();
		regionOption.put("metakey", "region");
		mockMetakeyOptions.add(departmentOption);
		mockMetakeyOptions.add(regionOption);

		try (MockedStatic<SecurityUserUtils> mockedSecurityUtils = mockStatic(SecurityUserUtils.class)) {
			mockedSecurityUtils.when(() -> SecurityUserUtils.getMetakeyOptions(null))
					.thenReturn(mockMetakeyOptions);

			PromptTestUtils.insertPromptMetadata(testMetadata);

			// Request metakey values
			List<String> metaKeys = Arrays.asList("department", "region");

			// Admin should be able to get metadata values
			List<Map<String, Object>> metaValuesMap = PromptTestUtils.getPromptMetaValues(metaKeys);
			assertNotNull(metaValuesMap);

			// Verify results contain the expected values
			assertEquals(6, metaValuesMap.size(),
					"Should return all 6 unique metadata values (3 departments + 3 regions)");

			List<Object> metaValues = metaValuesMap.parallelStream().map(entry -> entry.get("metavalue")).toList();

			// Verify specific values are present
			assertTrue(metaValues.contains("Engineering"), "Should contain Engineering department");
			assertTrue(metaValues.contains("Sales"), "Should contain Sales department");
			assertTrue(metaValues.contains("Marketing"), "Should contain Marketing department");
			assertTrue(metaValues.contains("North America"), "Should contain North America region");
			assertTrue(metaValues.contains("Europe"), "Should contain Europe region");
			assertTrue(metaValues.contains("Asia"), "Should contain Asia region");
		}
	}

	@Test
	public void testGetMetaValuesWithEmptyList() {
		// Create admin user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);

		// Request with empty metakey list
		List<String> emptyMetaKeys = Arrays.asList();

		List<Map<String, Object>> metaValues = PromptTestUtils.getPromptMetaValues(emptyMetaKeys);
		assertNotNull(metaValues);
		assertTrue(metaValues.isEmpty());
	}

	@Test
	public void testGetMetaValuesWithNonExistentKey() {
		// Create admin user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);

		// Request values for non-existent metakey
		List<String> nonExistentKeys = Arrays.asList("nonexistent_key");

		List<Map<String, Object>> metaValues = PromptTestUtils.getPromptMetaValues(nonExistentKeys);
		assertNotNull(metaValues);
		assertTrue(metaValues.isEmpty());
	}

	@Test
	public void testGetMetaValuesReturnsAllUniqueValues() {
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);

		// Mock SecurityUserUtils.getMetakeyOptions to return the department metakey
		List<Map<String, Object>> mockMetakeyOptions = new ArrayList<>();
		Map<String, Object> departmentOption = new HashMap<>();
		departmentOption.put("metakey", "department");
		mockMetakeyOptions.add(departmentOption);

		try (MockedStatic<SecurityUserUtils> mockedSecurityUtils = mockStatic(SecurityUserUtils.class)) {
			mockedSecurityUtils.when(() -> SecurityUserUtils.getMetakeyOptions(null))
					.thenReturn(mockMetakeyOptions);

			// Insert test data with intentional duplicates across multiple prompts
			// This ensures the reactor properly deduplicates values from PROMPTMETA
			Map<String, Collection<String>> testMetadata = new HashMap<>();
			// Insert duplicate "Engineering" values across multiple prompts
			testMetadata.put("department", Arrays.asList("Engineering", "Sales", "Engineering", "Marketing", "Sales"));
			PromptTestUtils.insertPromptMetadata(testMetadata);

			List<String> metaKeys = Arrays.asList("department");

			List<Map<String, Object>> metaValuesMap = PromptTestUtils.getPromptMetaValues(metaKeys);
			assertNotNull(metaValuesMap);

			// Values should be unique (no duplicates) - should only return 3 unique values
			long uniqueCount = metaValuesMap.stream().distinct().count();
			assertEquals(uniqueCount, metaValuesMap.size(), "Returned values should not contain duplicates");

			// Verify we get exactly 3 unique department values
			assertEquals(3, metaValuesMap.size(),
					"Should return 3 unique department values (Engineering, Sales, Marketing)");

			List<Object> metaValues = metaValuesMap.parallelStream().map(entry -> entry.get("metavalue")).toList();

			// Verify specific values are present
			assertTrue(metaValues.contains("Engineering"), "Should contain Engineering");
			assertTrue(metaValues.contains("Sales"), "Should contain Sales");
			assertTrue(metaValues.contains("Marketing"), "Should contain Marketing");
		}
	}
}
