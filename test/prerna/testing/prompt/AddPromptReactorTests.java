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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;

public class AddPromptReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void addOnePromptTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		
		// Verify we got a valid UUID back
		assertNotEquals(null, promptId);
		assertNotEquals("", promptId);
		
		// Verify the prompt was actually created with correct data
		Map<String, Object> createdPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(title, createdPrompt.get("title"));
		assertEquals(context, createdPrompt.get("context"));
		assertEquals(intent, createdPrompt.get("intent"));
	}
	
	@Test
	public void addTwoPrompts() {
			
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		String promptId1 = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId1);
		
		// Verify first prompt was created correctly
		Map<String, Object> prompt1 = PromptTestUtils.getPrompt(promptId1);
		assertEquals(title, prompt1.get("title"));
		
		// Changing vars for prompt 2 
		String title2 = "Test-Title-2";
		String context2 = "Translate the {{question}} int {{language}}";
		List<String> tags2 = Arrays.asList("World", "Travel");
		String intent2 = "Test Prompt Intent 2";
		String promptId2 = PromptTestUtils.addPrompt(title2, context2, intent2, tags2, false, null);
		assertNotEquals(null, promptId2);
		assertNotEquals(promptId1, promptId2);
		
		// Verify second prompt was created correctly
		Map<String, Object> prompt2 = PromptTestUtils.getPrompt(promptId2);
		assertEquals(title2, prompt2.get("title"));
		assertEquals(context2, prompt2.get("context"));
		
	}
	
	@Test
	public void addTwoPromptsGetOneTag() {
			
		String title1 = "Test-Title";
		String context1 = "Translate {{question}}";
		String intent1 = "Test Prompt";
		List<String> tags1 = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		String promptId1 = PromptTestUtils.addPrompt(title1, context1, intent1, tags1, false, null);
		assertNotEquals(null, promptId1);
		
		// Changing vars for prompt 2 
		String title2 = "Test-Title-2";
		String context2 = "Translate the {{question}} int {{language}}";
		String intent2 = "second intent";
		List<String> tags2 = Arrays.asList("World", "Travel");
		String promptId2 = PromptTestUtils.addPrompt(title2, context2, intent2, tags2, false, null);
		assertNotEquals(null, promptId2);
		
		// Filter by "World" tag - both prompts should be returned
		List<String> metaTagsFilters = Arrays.asList("World");
		NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getNounType());
		
		// Verify both prompts are in the result
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listPrompts.getValue();
		assertEquals(2, prompts.size(), "Should have exactly 2 prompts with 'World' tag");
		
		// Verify both prompt IDs are present
		List<String> returnedIds = prompts.stream()
			.map(p -> (String) p.get("id"))
			.collect(java.util.stream.Collectors.toList());
		assertTrue(returnedIds.contains(promptId1), "Result should contain first prompt");
		assertTrue(returnedIds.contains(promptId2), "Result should contain second prompt");
		
		// Verify metadata for first prompt
		Map<String, Object> prompt1 = prompts.stream()
			.filter(p -> promptId1.equals(p.get("id")))
			.findFirst()
			.orElse(null);
		assertEquals(title1, prompt1.get("title"));
		assertEquals(context1, prompt1.get("context"));
		assertEquals(intent1, prompt1.get("intent"));
		List<String> prompt1Tags = (List<String>) prompt1.get("tags");
		assertEquals(3, prompt1Tags.size());
		assertTrue(prompt1Tags.containsAll(tags1), "First prompt should have all its tags");
		
		// Verify metadata for second prompt
		Map<String, Object> prompt2 = prompts.stream()
			.filter(p -> promptId2.equals(p.get("id")))
			.findFirst()
			.orElse(null);
		assertEquals(title2, prompt2.get("title"));
		assertEquals(context2, prompt2.get("context"));
		assertEquals(intent2, prompt2.get("intent"));
		List<String> prompt2Tags = (List<String>) prompt2.get("tags");
		assertEquals(2, prompt2Tags.size());
		assertTrue(prompt2Tags.containsAll(tags2), "Second prompt should have all its tags");
		
	}
	
	@Test
	public void addPromptValidationTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		assertFalse(promptId.isBlank());
	}

}
