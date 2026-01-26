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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

/**
 * Tests authorization logic for prompt updates and deletes
 * Covers validatePromptUpdateAuthorization() functionality
 */
public class PromptAuthorizationTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;  // Ensure isolation
		super.beforeEachTest();
	}

	@Test
	public void testRegularUserUpdatesOwnPrompt() {
		// Create a regular user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("regularuser1", "regular1@test.com", false);
		
		String title = "User1 Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Regular user should be able to update their own prompt
		String newTitle = "Updated Title";
		PromptTestUtils.updatePrompt(promptId, newTitle, context, intent, tags);
		
		// Verify update was successful
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(newTitle, updatedPrompt.get("TITLE"));
	}

	@Test
	public void testRegularUserCannotUpdateOthersPrompt() {
		// Create first user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "User1 Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Switch to second user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// Second user should NOT be able to update first user's prompt
		String errorMsg = PromptTestUtils.updatePromptExpectError(promptId, "Hacked Title", 
		                                                          context, intent, tags, null, null);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("permission") || errorMsg.contains("does not have"));
	}

	@Test
	public void testAdminUpdatesGlobalPrompt() {
		// Create admin user and add a global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin1", "admin1@test.com", true);
		
		String title = "Global Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("global");
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, true, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Switch to different admin
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin2", "admin2@test.com", true);
		
		// Second admin should be able to update global prompt
		String newTitle = "Admin Updated Global";
		PromptTestUtils.updatePromptWithGlobal(promptId, newTitle, context, intent, tags, true, null);
		
		// Verify update was successful
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(newTitle, updatedPrompt.get("TITLE"));
	}

	@Test
	public void testAdminCannotUpdateNonGlobalOthersPrompt() {
		// Create regular user and add a non-global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("regularuser", "regular@test.com", false);
		
		String title = "Regular User Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Switch to admin user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);
		
		// Admin should NOT be able to update non-global prompt created by others
		String errorMsg = PromptTestUtils.updatePromptExpectError(promptId, "Admin Hacked", 
		                                                          context, intent, tags, false, null);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("global") || errorMsg.contains("permission"));
	}

	@Test
	public void testAdminUpdatesOwnNonGlobalPrompt() {
		// Create admin user and add a non-global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);
		
		String title = "Admin Private Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("private");
		
		PromptTestUtils.addPromptWithGlobal(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Admin should be able to update their own non-global prompt
		String newTitle = "Admin Updated Own";
		PromptTestUtils.updatePromptWithGlobal(promptId, newTitle, context, intent, tags, false, null);
		
		// Verify update was successful
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(newTitle, updatedPrompt.get("TITLE"));
	}

	@Test
	public void testRegularUserDeletesOwnPrompt() {
		// Create a regular user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "User Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// User should be able to delete their own prompt
		PromptTestUtils.deletePrompt(promptId);
		
		// Verify prompt was deleted
		NounMetadata afterDelete = PromptTestUtils.listPrompts();
		List<Map<String, Object>> afterPrompts = (List<Map<String, Object>>) afterDelete.getValue();
		assertEquals(0, afterPrompts.size());
	}

	@Test
	public void testRegularUserCannotDeleteOthersPrompt() {
		// Create first user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "User1 Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("ID");
		
		// Switch to second user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// Second user should NOT be able to delete first user's prompt
		String errorMsg = PromptTestUtils.deletePromptExpectError(promptId);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("permission") || errorMsg.contains("does not have"));
	}
}
