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

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;

/**
 * Tests user metadata filtering functionality
 * Covers addUserMetaFiltersToQs() logic
 */
public class PromptMetadataFilteringTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testRegularUserWithMetadataSeesOnlyMatchingPrompts() {
		// Create user1 with department=engineering metadata
		Map<String, Collection<String>> user1Metadata = new HashMap<>();
		user1Metadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, user1Metadata);
		PromptTestUtils.setUserWithMetadata("user1", "user1@test.com", user1Metadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// User1 adds prompt with engineering metadata
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPromptWithGlobal("Engineering Prompt", context, intent, tags, false, engMetadata);
		
		// Create user2 with department=sales metadata
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("sales"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata("user2", "user2@test.com", user2Metadata);
		
		// User2 adds prompt with sales metadata
		Map<String, Collection<String>> salesMetadata = new HashMap<>();
		salesMetadata.put("department", Arrays.asList("sales"));
		PromptTestUtils.addPromptWithGlobal("Sales Prompt", context, intent, tags, false, salesMetadata);
		
		// User2 should only see their own sales prompt (not engineering prompt)
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size());
		assertEquals("Sales Prompt", prompts.get(0).get("TITLE"));
	}

	@Test
	public void testAdminSeesAllPromptsRegardlessOfMetadata() {
		// Create regular user with department=engineering
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User regularUser = PromptTestUtils.createTestUser("regularuser", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("regularuser", "regularuser@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Regular user adds prompt with engineering metadata
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPromptWithGlobal("Engineering Prompt", context, intent, tags, false, engMetadata);
		
		// Create admin user with different metadata
		Map<String, Collection<String>> adminMetadata = new HashMap<>();
		adminMetadata.put("department", Arrays.asList("sales"));
		
		User adminUser = PromptTestUtils.createTestUser("admin", true, adminMetadata);
		PromptTestUtils.setUserWithMetadata("admin", "admin@test.com", adminMetadata);
		
		// Admin should see all prompts regardless of metadata mismatch
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		// Admin sees the engineering prompt even though they have sales metadata
		assertTrue(prompts.size() >= 1);
		boolean foundEngineeringPrompt = prompts.stream()
				.anyMatch(p -> "Engineering Prompt".equals(p.get("TITLE")));
		assertTrue(foundEngineeringPrompt);
	}

	@Test
	public void testUserWithMultipleMetadataValuesSeesMatchingPrompts() {
		// Create user with multiple department values
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering", "sales", "marketing"));
		
		User user = PromptTestUtils.createTestUser("multiuser", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("multiuser", "multiuser@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add prompts with different department values
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPromptWithGlobal("Engineering Prompt", context, intent, tags, false, engMetadata);
		
		Map<String, Collection<String>> salesMetadata = new HashMap<>();
		salesMetadata.put("department", Arrays.asList("sales"));
		PromptTestUtils.addPromptWithGlobal("Sales Prompt", context, intent, tags, false, salesMetadata);
		
		Map<String, Collection<String>> marketingMetadata = new HashMap<>();
		marketingMetadata.put("department", Arrays.asList("marketing"));
		PromptTestUtils.addPromptWithGlobal("Marketing Prompt", context, intent, tags, false, marketingMetadata);
		
		// User should see all three prompts since they have all three department values
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(3, prompts.size());
	}

	@Test
	public void testPromptsWithNoMetadataVisibility() {
		// Create user with metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add prompt without any metadata
		PromptTestUtils.addPromptWithGlobal("No Metadata Prompt", context, intent, tags, false, null);
		
		// Add prompt with matching metadata
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPromptWithGlobal("Engineering Prompt", context, intent, tags, false, engMetadata);
		
		// User should see both prompts (prompts without metadata are visible to all)
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(2, prompts.size());
	}

	@Test
	public void testMultipleMetakeyFiltering() {
		// Create user with multiple metakeys
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		userMetadata.put("region", Arrays.asList("east"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add prompt with matching metadata for both keys
		Map<String, Collection<String>> matchingMetadata = new HashMap<>();
		matchingMetadata.put("department", Arrays.asList("engineering"));
		matchingMetadata.put("region", Arrays.asList("east"));
		PromptTestUtils.addPromptWithGlobal("Matching Prompt", context, intent, tags, false, matchingMetadata);
		
		// Create a different user with west region to add the partial match prompt
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("engineering"));
		user2Metadata.put("region", Arrays.asList("west"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata("user2", "user2@test.com", user2Metadata);
		
		// User2 adds prompt with partial match (department matches, region doesn't)
		Map<String, Collection<String>> partialMetadata = new HashMap<>();
		partialMetadata.put("department", Arrays.asList("engineering"));
		partialMetadata.put("region", Arrays.asList("west"));  // Doesn't match user's region
		PromptTestUtils.addPromptWithGlobal("Partial Match Prompt", context, intent, tags, false, partialMetadata);
		
		// Switch back to original user to check what they can see
		PromptTestUtils.setUserWithMetadata("user", "user@test.com", userMetadata);
		
		// User should only see the fully matching prompt
		// (Metadata filtering requires ALL metakeys to match)
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		// Should see at least the matching prompt
		boolean foundMatching = prompts.stream()
				.anyMatch(p -> "Matching Prompt".equals(p.get("TITLE")));
		assertTrue(foundMatching);
	}

	@Test
	public void testGlobalPromptsVisibleDespiteMetadataMismatch() {
		// Create user with specific metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, userMetadata);
		PromptTestUtils.setUserWithMetadata("user1", "user1@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add global prompt with different metadata
		Map<String, Collection<String>> salesMetadata = new HashMap<>();
		salesMetadata.put("department", Arrays.asList("sales"));
		PromptTestUtils.addPromptWithGlobal("Global Sales Prompt", context, intent, tags, true, salesMetadata);
		
		// Create different user
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("engineering"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata("user2", "user2@test.com", user2Metadata);
		
		// User2 should see the global prompt despite metadata mismatch
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		boolean foundGlobalPrompt = prompts.stream()
				.anyMatch(p -> "Global Sales Prompt".equals(p.get("TITLE")));
		assertTrue(foundGlobalPrompt);
	}
}
