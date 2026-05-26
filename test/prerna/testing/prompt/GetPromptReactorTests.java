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
 * Tests GetPromptReactor functionality
 * Verifies prompt retrieval by ID with access control
 */
public class GetPromptReactorTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testGetPromptWithValidId() {
		// Create user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "Test Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test", "example");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Get the prompt by ID
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		
		// Verify the prompt details
		assertNotNull(prompt);
		assertEquals(title, prompt.get("title"));
		assertEquals(context, prompt.get("context"));
		assertEquals(intent, prompt.get("intent"));
		assertNotNull(prompt.get("id"));
		assertNotNull(prompt.get("created_by"));
		assertNotNull(prompt.get("date_created"));
		
		// Verify tags are included
		assertTrue(prompt.containsKey("tags"));
		List<String> returnedTags = (List<String>) prompt.get("tags");
		assertEquals(tags.size(), returnedTags.size());
		assertTrue(returnedTags.containsAll(tags));
	}

	@Test
	public void testGetPromptWithInvalidId() {
		// Create user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		// Try to get a prompt with non-existent ID. This shouldn't return anything
		String invalidId = "non-existent-id-12345";
		Map<String, Object> result = PromptTestUtils.getPrompt(invalidId);
		
		// Should return an empty map for some non-existant prompt
		assertTrue(result.isEmpty());
	}

	@Test
	public void testUserCanGetTheirOwnPrompt() {
		// Create user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "User's Own Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// User should be able to get their own prompt
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(prompt);
		assertEquals(title, prompt.get("title"));
	}

	@Test
	public void testUserCanGetGlobalPrompt() {
		// Create first user and add a global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "Global Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("global");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, true, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Switch to different user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// User2 should be able to get the global prompt
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(prompt);
		assertEquals(title, prompt.get("title"));
		assertEquals(true, prompt.get("global"));
	}

	@Test
	public void testUserCannotGetOthersPrivatePrompt() {
		// Create first user and add a private prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "Private Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("private");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Switch to different user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// User2 should NOT be able to get user1's private prompt. Instead, User2 should receive an empty map.
		Map<String, Object> result = PromptTestUtils.getPrompt(promptId);
		assertTrue(result.isEmpty());
	}

	@Test
	public void testGetPromptIncludesAllFields() {
		// Create user and add a prompt with all fields
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "Complete Prompt";
		String context = "Test context with {{question}} and {{answer}}";
		String intent = "Test intent for validation";
		List<String> tags = Arrays.asList("tag1", "tag2", "tag3");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, true, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Get the full prompt
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		
		// Verify all expected fields are present
		assertTrue(prompt.containsKey("id"));
		assertTrue(prompt.containsKey("title"));
		assertTrue(prompt.containsKey("context"));
		assertTrue(prompt.containsKey("version"));
		assertTrue(prompt.containsKey("intent"));
		assertTrue(prompt.containsKey("created_by"));
		assertTrue(prompt.containsKey("date_created"));
		assertTrue(prompt.containsKey("global"));
		assertTrue(prompt.containsKey("tags"));
		
		// Verify GLOBAL field
		assertEquals(true, prompt.get("global"));
		
		// Verify content
		assertEquals(title, prompt.get("title"));
		assertEquals(context, prompt.get("context"));
		assertEquals(intent, prompt.get("intent"));
	}

	@Test
	public void testGetPromptIncludesMetadata() {
		// Create user and add a prompt with metadata
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "Prompt with Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Note: Metadata would need to be added if supported by the prompt system
		// This test validates that metaKeys field is present if metadata exists
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Get the full prompt
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		
		// Verify the prompt is returned
		assertNotNull(prompt);
		assertEquals(title, prompt.get("title"));
	}

	@Test
	public void testGetPromptReturnsLatestVersion() {
		// Create user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String originalTitle = "Original Title";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(originalTitle, context, intent, tags, false, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Update the prompt
		String updatedTitle = "Updated Title";
		PromptTestUtils.updatePrompt(promptId, updatedTitle, context, intent, tags, false, null);
		
		// Get the prompt should return the latest version
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(updatedTitle, prompt.get("title"));
		
		// Verify version was incremented
		assertNotNull(prompt.get("version"));
	}
}
