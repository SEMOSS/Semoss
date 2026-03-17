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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * Tests GLOBAL field functionality and createGlobalOrCreatedByFilter() logic
 * Verifies that users can see global prompts and their own prompts
 */
public class GlobalPromptFilteringTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testRegularUserSeesGlobalPrompts() {
		// Create first user and add a global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "Global Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("global");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, true, null);
		
		// Switch to different user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// User2 should see the global prompt created by user1
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size());
		assertEquals(title, prompts.get(0).get("title"));
		assertEquals(true, prompts.get(0).get("global"));
	}

	@Test
	public void testRegularUserCannotSeeNonGlobalPromptsFromOthers() {
		// Create first user and add a non-global prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "Private Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("private");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Switch to different user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// User2 should NOT see the private prompt created by user1
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(0, prompts.size());
	}

	@Test
	public void testUserSeesOwnPromptsRegardlessOfGlobalStatus() {
		// Create user and add both global and non-global prompts
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String globalTitle = "My Global Prompt";
		String privateTitle = "My Private Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add global prompt
		PromptTestUtils.addPrompt(globalTitle, context, intent, tags, true, null);
		
		// Add private prompt
		PromptTestUtils.addPrompt(privateTitle, context, intent, tags, false, null);
		
		// User should see both their global and private prompts
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(2, prompts.size());
	}

	@Test
	public void testUserSeesGlobalPromptsAndOwnPrompts() {
		// Create user1 and add global and private prompts
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String globalTitle = "User1 Global";
		String privateTitle = "User1 Private";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(globalTitle, context, intent, tags, true, null);
		PromptTestUtils.addPrompt(privateTitle, context, intent, tags, false, null);
		
		// Switch to user2 and add their own prompts
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		String user2Global = "User2 Global";
		String user2Private = "User2 Private";
		
		PromptTestUtils.addPrompt(user2Global, context, intent, tags, true, null);
		PromptTestUtils.addPrompt(user2Private, context, intent, tags, false, null);
		
		// User2 should see:
		// - User1's global prompt
		// - User2's global prompt
		// - User2's private prompt
		// NOT User1's private prompt
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(3, prompts.size());
		
		// Verify the correct prompts are visible
		List<String> visibleTitles = prompts.stream()
				.map(p -> (String) p.get("title"))
				.sorted()
				.toList();
		
		assertTrue(visibleTitles.contains(globalTitle));
		assertTrue(visibleTitles.contains(user2Global));
		assertTrue(visibleTitles.contains(user2Private));
		assertFalse(visibleTitles.contains(privateTitle));
	}

	@Test
	public void testGlobalFieldIncludedInResponse() {
		// Create user and add prompts with different global values
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String globalTitle = "Global Prompt";
		String privateTitle = "Private Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(globalTitle, context, intent, tags, true, null);
		PromptTestUtils.addPrompt(privateTitle, context, intent, tags, false, null);
		
		// Verify GLOBAL field is included in response
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		for (Map<String, Object> prompt : prompts) {
			assertTrue(prompt.containsKey("global"));
			Boolean global = (Boolean) prompt.get("global");
			String title = (String) prompt.get("title");
			
			if (title.equals(globalTitle)) {
				assertTrue(global);
			} else if (title.equals(privateTitle)) {
				assertFalse(global);
			}
		}
	}

	@Test
	public void testGetPromptReturnsGlobalField() {
		// Create user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "Test Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt(title, context, intent, tags, true, null);
		
		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");
		
		// Get individual prompt
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		
		// Verify GLOBAL field is included
		assertTrue(prompt.containsKey("global"));
		assertEquals(true, prompt.get("global"));
	}

	@Test
	public void testAdminSeesAllGlobalPrompts() {
		// Create multiple users with global prompts
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		PromptTestUtils.addPrompt("User1 Global", context, intent, tags, true, null);
		PromptTestUtils.addPrompt("User1 Private", context, intent, tags, false, null);
		
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		PromptTestUtils.addPrompt("User2 Global", context, intent, tags, true, null);
		PromptTestUtils.addPrompt("User2 Private", context, intent, tags, false, null);
		
		// Switch to admin
		ApiSemossTestUserUtils.addAndSetNewNativeUser("admin", "admin@test.com", true);
		
		// Admin should see all global prompts
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		// Admin sees the two global prompts
		long globalCount = prompts.stream()
				.filter(p -> (Boolean) p.get("global"))
				.count();
		assertEquals(2, globalCount);
	}
}
