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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

/**
 * Tests metadata CRUD operations for prompts
 * Since validation has been removed, these tests focus on simple CRUD
 * operations
 * with metadata without any validation requirements
 */
public class PromptMetadataTests extends AbstractBaseSemossApiTests {

	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = true;
		super.beforeEachTest();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreatePromptWithMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt with Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("metadata", "test");

		// Create metadata map
		Map<String, Collection<String>> metaMap = new HashMap<>();
		metaMap.put("department", Arrays.asList("engineering", "sales"));
		metaMap.put("region", Arrays.asList("east", "west"));
		metaMap.put("priority", Arrays.asList("high"));

		// Add prompt with metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, metaMap);

		// Verify prompt was created
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size(), "Should have created one prompt");

		// Get the prompt and verify metadata
		Map<String, Object> prompt = prompts.get(0);
		assertEquals(title, prompt.get("title"));

		@SuppressWarnings("unchecked")
		Map<String, List<String>> metaKeys = (Map<String, List<String>>) prompt.get("metaKeys");
		assertNotNull(metaKeys, "Metadata should be present");
		assertEquals(3, metaKeys.size(), "Should have 3 metadata keys");
		assertTrue(metaKeys.containsKey("department"));
		assertTrue(metaKeys.containsKey("region"));
		assertTrue(metaKeys.containsKey("priority"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCreatePromptWithoutMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt without Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");

		// Add prompt without metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, null);

		// Verify prompt was created
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size(), "Should have created one prompt");

		Map<String, Object> prompt = prompts.get(0);
		assertEquals(title, prompt.get("title"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReadPromptMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt for Reading";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("read-test");

		// Create metadata
		Map<String, Collection<String>> metaMap = new HashMap<>();
		metaMap.put("team", Arrays.asList("alpha", "beta"));
		metaMap.put("status", Arrays.asList("active"));

		// Add prompt with metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, metaMap);

		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");

		// Read the prompt directly
		Map<String, Object> prompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(prompt, "Should be able to read the prompt");
		assertEquals(title, prompt.get("title"));

		// Verify metadata
		@SuppressWarnings("unchecked")
		Map<String, List<String>> metaKeys = (Map<String, List<String>>) prompt.get("metaKeys");
		assertNotNull(metaKeys, "Metadata should be present");
		assertEquals(2, metaKeys.size(), "Should have 2 metadata keys");
		assertEquals(Arrays.asList("alpha", "beta"), metaKeys.get("team"));
		assertEquals(Arrays.asList("active"), metaKeys.get("status"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testUpdatePromptMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt for Updating";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("update-test");

		// Create initial metadata
		Map<String, Collection<String>> initialMetadata = new HashMap<>();
		initialMetadata.put("department", Arrays.asList("engineering"));
		initialMetadata.put("region", Arrays.asList("east"));

		// Add prompt with initial metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, initialMetadata);

		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");

		// Update with different metadata
		Map<String, Collection<String>> updatedMetadata = new HashMap<>();
		updatedMetadata.put("department", Arrays.asList("sales", "marketing"));
		updatedMetadata.put("status", Arrays.asList("active"));
		// Note: region is removed, status is new

		PromptTestUtils.updatePrompt(promptId, title, context, intent, tags, false, updatedMetadata);

		// Verify the update
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(updatedPrompt);

		@SuppressWarnings("unchecked")
		Map<String, List<String>> metaKeys = (Map<String, List<String>>) updatedPrompt.get("metaKeys");
		assertNotNull(metaKeys, "Metadata should be present");
		assertEquals(2, metaKeys.size(), "Should have 2 metadata keys after update");
		assertTrue(metaKeys.containsKey("department"));
		assertTrue(metaKeys.containsKey("status"));
		assertFalse(metaKeys.containsKey("region"), "Old metadata key should be removed");
		assertEquals(Arrays.asList("sales", "marketing"), metaKeys.get("department"));
		assertEquals(Arrays.asList("active"), metaKeys.get("status"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testUpdatePromptRemoveAllMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt to Clear Metadata";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("clear-test");

		// Create initial metadata
		Map<String, Collection<String>> initialMetadata = new HashMap<>();
		initialMetadata.put("department", Arrays.asList("engineering"));
		initialMetadata.put("region", Arrays.asList("east"));

		// Add prompt with metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, initialMetadata);

		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		String promptId = (String) prompts.get(0).get("id");

		// Update with empty metadata (clearing all metadata)
		Map<String, Collection<String>> emptyMetadata = new HashMap<>();
		PromptTestUtils.updatePrompt(promptId, title, context, intent, tags, false, emptyMetadata);

		// Verify metadata was cleared
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertNotNull(updatedPrompt);

		@SuppressWarnings("unchecked")
		Map<String, List<String>> metaKeys = (Map<String, List<String>>) updatedPrompt.get("metaKeys");
		assertTrue(metaKeys == null || metaKeys.isEmpty(), "Metadata should be empty after clearing");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDeletePromptWithMetadata() {
		// Create a user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);

		String title = "Prompt to Delete";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("delete-test");

		// Create metadata
		Map<String, Collection<String>> metaMap = new HashMap<>();
		metaMap.put("department", Arrays.asList("engineering"));
		metaMap.put("status", Arrays.asList("temporary"));

		// Add prompt with metadata
		PromptTestUtils.addPrompt(title, context, intent, tags, false, metaMap);

		// Get the prompt ID
		NounMetadata listResult = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(1, prompts.size(), "Should have one prompt before deletion");
		String promptId = (String) prompts.get(0).get("id");

		// Delete the prompt
		String deletedId = PromptTestUtils.deletePrompt(promptId);
		assertEquals(promptId, deletedId, "Deleted prompt ID should match");

		// Verify prompt no longer exists
		listResult = PromptTestUtils.listPrompts();
		prompts = (List<Map<String, Object>>) listResult.getValue();
		assertEquals(0, prompts.size(), "Should have no prompts after deletion");
	}

}
