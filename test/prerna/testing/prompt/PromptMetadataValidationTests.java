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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

/**
 * Tests metadata validation logic for prompts
 * Covers validateSelectedMetadata() and metaKeysIsSubset() functionality
 */
public class PromptMetadataValidationTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testAdminAddsPromptWithValidMetakeys() {
		// Create admin user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);
		
		// First create metakeys in the system
		Map<String, Collection<String>> metaMap = new HashMap<>();
		metaMap.put("department", Arrays.asList("engineering", "sales"));
		metaMap.put("region", Arrays.asList("east", "west"));
		
		// Register metakeys
		PromptTestUtils.createTestUser("admin", true, metaMap);
		
		String title = "Admin Prompt with Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata");
		
		// Admin should be able to add prompt with valid metakeys
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, metaMap);
		
		// Verify prompt was created
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size());
	}

	@Test
	public void testAdminCannotAddPromptWithInvalidMetakeys() {
		// Create admin user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);
		
		String title = "Admin Prompt with Invalid Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata");
		
		// Try to add prompt with metakeys that don't exist in the system
		Map<String, Collection<String>> invalidMetaMap = new HashMap<>();
		invalidMetaMap.put("nonexistent_key", Arrays.asList("value1"));
		
		// Should fail with error about metakeys not found
		String errorMsg = PromptTestUtils.addPromptWithGlobalExpectError(title, context, intent, 
		                                                                 tags, false, invalidMetaMap);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("Meta keys not found") || errorMsg.contains("metakey"));
	}

	@Test
	public void testRegularUserAddsPromptWithMetadataSubset() {
		// Create user with specific metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering", "sales", "marketing"));
		userMetadata.put("region", Arrays.asList("east", "west", "central"));
		
		User user = PromptTestUtils.createTestUser("regularuser", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("regularuser", "regularuser@test.com", userMetadata);
		
		String title = "User Prompt with Valid Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata");
		
		// User adds prompt with subset of their metadata
		Map<String, Collection<String>> promptMetadata = new HashMap<>();
		promptMetadata.put("department", Arrays.asList("engineering"));  // Subset
		promptMetadata.put("region", Arrays.asList("east"));  // Subset
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, promptMetadata);
		
		// Verify prompt was created
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size());
	}

	@Test
	public void testRegularUserCannotAddPromptWithMetadataOutsidePermissions() {
		// Create user with limited metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		userMetadata.put("region", Arrays.asList("east"));
		
		User user = PromptTestUtils.createTestUser("limiteduser", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("limiteduser", "limiteduser@test.com", userMetadata);
		
		String title = "User Prompt with Invalid Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata");
		
		// User tries to add prompt with metadata outside their permissions
		Map<String, Collection<String>> invalidMetadata = new HashMap<>();
		invalidMetadata.put("department", Arrays.asList("sales"));  // User doesn't have "sales"
		
		// Should fail
		String errorMsg = PromptTestUtils.addPromptWithGlobalExpectError(title, context, intent, 
		                                                                 tags, false, invalidMetadata);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("Meta filters not found") || errorMsg.contains("permission"));
	}

	@Test
	public void testRegularUserCannotUseMetakeyNotInPermissions() {
		// Create user with specific metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		String title = "User Prompt with Unauthorized Metakey";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata");
		
		// User tries to use a metakey they don't have access to
		Map<String, Collection<String>> invalidMetadata = new HashMap<>();
		invalidMetadata.put("region", Arrays.asList("east"));  // User doesn't have "region" metakey
		
		// Should fail
		String errorMsg = PromptTestUtils.addPromptWithGlobalExpectError(title, context, intent, 
		                                                                 tags, false, invalidMetadata);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("Meta filters not found") || errorMsg.contains("not found"));
	}

	@Test
	public void testUpdatePromptWithValidMetadata() {
		// Create user with metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering", "sales"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		// Add prompt with initial metadata
		String title = "User Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		Map<String, Collection<String>> initialMetadata = new HashMap<>();
		initialMetadata.put("department", Arrays.asList("engineering"));
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, initialMetadata);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Update with different but valid metadata
		Map<String, Collection<String>> updatedMetadata = new HashMap<>();
		updatedMetadata.put("department", Arrays.asList("sales"));
		
		PromptTestUtils.updatePromptWithGlobal(promptId, title, context, intent, tags, false, updatedMetadata);
		
		// Verify update was successful
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(updatedPrompt);
	}

	@Test
	public void testUpdatePromptWithInvalidMetadata() {
		// Create user with limited metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		// Add prompt
		String title = "User Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		Map<String, Collection<String>> initialMetadata = new HashMap<>();
		initialMetadata.put("department", Arrays.asList("engineering"));
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, initialMetadata);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Try to update with invalid metadata
		Map<String, Collection<String>> invalidMetadata = new HashMap<>();
		invalidMetadata.put("department", Arrays.asList("sales"));  // User doesn't have permission
		
		String errorMsg = PromptTestUtils.updatePromptExpectError(promptId, title, context, intent, 
		                                                          tags, false, invalidMetadata);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("Meta filters not found") || errorMsg.contains("permission"));
	}
}
