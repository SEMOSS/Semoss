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

/**
 * Tests prompt visibility filtering functionality.
 * Tests the simplified logic where users see:
 * - All global prompts (regardless of metadata)
 * - Prompts they created (regardless of global status or metadata)
 */
public class PromptMetadataFilteringTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	public void testUserSeesGlobalPromptsFromOtherUsers() {
		// Create user1
		Map<String, Collection<String>> user1Metadata = new HashMap<>();
		user1Metadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, user1Metadata);
		PromptTestUtils.setUserWithMetadata(user1.getPrimaryLoginToken().getId(), "user1@test.com", user1Metadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// User1 creates a global prompt
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPrompt("User1 Global Prompt", context, intent, tags, true, engMetadata);
		
		// Create user2 with different metadata
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("sales"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata(user2.getPrimaryLoginToken().getId(), "user2@test.com", user2Metadata);
		
		// User2 should see user1's global prompt even with different metadata
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		boolean foundGlobalPrompt = prompts.stream()
				.anyMatch(p -> "User1 Global Prompt".equals(p.get("title")));
		assertTrue(foundGlobalPrompt, "User2 should see user1's global prompt");
	}

	@Test
	public void testUserDoesNotSeeNonGlobalPromptsFromOtherUsers() {
		// Create user1
		Map<String, Collection<String>> user1Metadata = new HashMap<>();
		user1Metadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, user1Metadata);
		PromptTestUtils.setUserWithMetadata(user1.getPrimaryLoginToken().getId(), "user1@test.com", user1Metadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// User1 creates a non-global prompt
		Map<String, Collection<String>> engMetadata = new HashMap<>();
		engMetadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPrompt("User1 Private Prompt", context, intent, tags, false, engMetadata);
		
		// Create user2 with same metadata
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("engineering"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata(user2.getPrimaryLoginToken().getId(), "user2@test.com", user2Metadata);
		
		// User2 should NOT see user1's non-global prompt (even with matching metadata)
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		boolean foundPrivatePrompt = prompts.stream()
				.anyMatch(p -> "User1 Private Prompt".equals(p.get("title")));
		assertTrue(!foundPrivatePrompt, "User2 should not see user1's non-global prompt");
	}

	@Test
	public void testUserSeesTheirOwnNonGlobalPrompts() {
		// Create user
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user = PromptTestUtils.createTestUser("user", false, userMetadata);
		PromptTestUtils.setUserWithMetadata(user.getPrimaryLoginToken().getId(), "user@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// User creates non-global prompts
		Map<String, Collection<String>> metadata = new HashMap<>();
		metadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPrompt("My Private Prompt 1", context, intent, tags, false, metadata);
		PromptTestUtils.addPrompt("My Private Prompt 2", context, intent, tags, false, metadata);
		
		// User should see their own prompts
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		assertEquals(2, prompts.size(), "User should see both of their own prompts");
	}

	@Test
	public void testUserSeesGlobalPromptsRegardlessOfMetadata() {
		// Create user with specific metadata
		Map<String, Collection<String>> userMetadata = new HashMap<>();
		userMetadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, userMetadata);
		PromptTestUtils.setUserWithMetadata(user1.getPrimaryLoginToken().getId(), "user1@test.com", userMetadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// Add global prompts with various metadata (or no metadata)
		Map<String, Collection<String>> salesMetadata = new HashMap<>();
		salesMetadata.put("department", Arrays.asList("sales"));
		PromptTestUtils.addPrompt("Global Sales Prompt", context, intent, tags, true, salesMetadata);
		
		Map<String, Collection<String>> noMetadata = new HashMap<>();
		PromptTestUtils.addPrompt("Global No-Metadata Prompt", context, intent, tags, true, noMetadata);
		
		// Create different user to list prompts
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("marketing"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata(user2.getPrimaryLoginToken().getId(), "user2@test.com", user2Metadata);
		
		// User2 should see both global prompts regardless of metadata mismatch
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		boolean foundSalesPrompt = prompts.stream()
				.anyMatch(p -> "Global Sales Prompt".equals(p.get("title")));
		boolean foundNoMetadataPrompt = prompts.stream()
				.anyMatch(p -> "Global No-Metadata Prompt".equals(p.get("title")));
		
		assertTrue(foundSalesPrompt, "User2 should see global prompt with different metadata");
		assertTrue(foundNoMetadataPrompt, "User2 should see global prompt with no metadata");
	}

	@Test
	public void testUserSeesBothGlobalAndOwnPrompts() {
		// Create user1
		Map<String, Collection<String>> user1Metadata = new HashMap<>();
		user1Metadata.put("department", Arrays.asList("engineering"));
		
		User user1 = PromptTestUtils.createTestUser("user1", false, user1Metadata);
		PromptTestUtils.setUserWithMetadata(user1.getPrimaryLoginToken().getId(), "user1@test.com", user1Metadata);
		
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		// User1 creates a global prompt
		Map<String, Collection<String>> metadata = new HashMap<>();
		metadata.put("department", Arrays.asList("engineering"));
		PromptTestUtils.addPrompt("User1 Global", context, intent, tags, true, metadata);
		
		// Create user2
		Map<String, Collection<String>> user2Metadata = new HashMap<>();
		user2Metadata.put("department", Arrays.asList("sales"));
		
		User user2 = PromptTestUtils.createTestUser("user2", false, user2Metadata);
		PromptTestUtils.setUserWithMetadata(user2.getPrimaryLoginToken().getId(), "user2@test.com", user2Metadata);
		
		// User2 creates their own non-global prompt
		Map<String, Collection<String>> salesMetadata = new HashMap<>();
		salesMetadata.put("department", Arrays.asList("sales"));
		PromptTestUtils.addPrompt("User2 Private", context, intent, tags, false, salesMetadata);
		
		// User2 should see both: the global prompt from user1 AND their own private prompt
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		
		assertEquals(2, prompts.size(), "User2 should see 2 prompts total");
		
		boolean foundGlobal = prompts.stream()
				.anyMatch(p -> "User1 Global".equals(p.get("title")));
		boolean foundPrivate = prompts.stream()
				.anyMatch(p -> "User2 Private".equals(p.get("title")));
		
		assertTrue(foundGlobal, "User2 should see user1's global prompt");
		assertTrue(foundPrivate, "User2 should see their own private prompt");
	}
}
